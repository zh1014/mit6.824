package kvraft

import (
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"mit6.824/raft"
	"mit6.824/util"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	ClientID int
	SerialNo int
	Typ      string
	Key      string
	Val      string
	Ret      chan<- string
}

type KVServer struct {
	me           int
	maxraftstate int // snapshot if log grows this big
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()

	mu              sync.Mutex // protect the following fields
	state           map[string]string
	lastApplied     int
	snapshotInclude int
	clientSerial    map[int]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	logrus.Infof("%s exec Get(%+v)", kv, args)
	waitChan := make(chan string)
	op := Op{
		ClientID: args.ClientID,
		SerialNo: args.SerialNo,
		Typ:      OpTypeGet,
		Key:      args.Key,
		Ret:      waitChan,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-time.After(RaftOpTimeout):
		reply.Err = ErrRaftTimeout
	case reply.Value = <-waitChan:
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	logrus.Infof("%s exec PutAppend(%+v)", kv, args)
	waitChan := make(chan string)
	op := Op{
		ClientID: args.ClientID,
		SerialNo: args.SerialNo,
		Typ:      args.Op,
		Key:      args.Key,
		Val:      args.Value,
		Ret:      waitChan,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-time.After(RaftOpTimeout):
		reply.Err = ErrRaftTimeout
	case <-waitChan:
		reply.Err = OK
	}
}

func (kv *KVServer) ApplyToStateMachine() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// common operation
			kv.lastApplied = msg.CommandIndex
			kv.applyOp(msg.Command.(Op))
			if kv.needSnapshot() {
				kv.snapshot()
			}
		} else {
			// install snapshot
			kv.lastApplied = msg.CommandIndex
			kv.snapshotInclude = msg.CommandIndex
			kv.installSnapshot(msg.Command.([]byte))
		}
	}
}

func (kv *KVServer) applyOp(op Op) {
	if op.SerialNo < kv.clientSerial[op.ClientID] {
		logrus.Errorf("client %d should use increasing serial number ", op.ClientID)
		return
	}
	switch op.Typ {
	case OpTypeGet:
	case OpTypePut:
		if op.SerialNo == kv.clientSerial[op.ClientID] {
			// client retrying, only write once
			logrus.Debugf("%s skip op", kv)
			break
		}
		kv.state[op.Key] = op.Val
	case OpTypeAppend:
		if op.SerialNo == kv.clientSerial[op.ClientID] {
			// client retrying, only write once
			logrus.Debugf("%s skip op", kv)
			break
		}
		kv.state[op.Key] += op.Val
	default:
		panic("unknown op type")
	}
	kv.clientSerial[op.ClientID] = op.SerialNo
	logrus.Debugf("%s Op=%+v, NowVal=%s", kv, op, kv.state[op.Key])
	if _, isLeader := kv.rf.GetState(); !isLeader {
		// drop output (In fact, there is no blocked RPC )
		return
	}
	go awakenRPC(op.Ret, kv.state[op.Key])
}

// awaken blocked RPC, output to client
func awakenRPC(ch chan<- string, val string) {
	select {
	case <-time.After(time.Second):
		logrus.Warnf("failed to awaken RPC")
	case ch <- val:
	}
}

func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate <= 0 {
		return false
	}
	return kv.lastApplied-kv.snapshotInclude > kv.maxraftstate
}

func (kv *KVServer) snapshot() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	util.CheckErr(encoder.Encode(kv.state))
	util.CheckErr(encoder.Encode(kv.clientSerial))

	kv.rf.Snapshot(kv.lastApplied, buffer.Bytes())
	kv.snapshotInclude = kv.lastApplied
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)

	state := make(map[string]string)
	util.CheckErr(decoder.Decode(&state))
	kv.state = state

	cs := make(map[int]int)
	util.CheckErr(decoder.Decode(&cs))
	kv.clientSerial = cs
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("[KVServer%d]", kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

// TODO: check killed
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),
		state:        make(map[string]string),
		clientSerial: make(map[int]int),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyToStateMachine()
	logrus.Infof("%s started....", kv)
	return kv
}
