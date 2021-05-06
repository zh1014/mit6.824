package kvraft

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"mit6.824/labrpc"
)
import crand "crypto/rand"
import "math/big"

type Clerk struct {
	id        int
	serialNo  int
	servers   []*labrpc.ClientEnd
	curServer int
}

func (ck *Clerk) chooseAnotherServer() {
	ck.curServer++
	ck.curServer %= len(ck.servers)
	//if len(ck.servers) == 1 {
	//	return
	//}
	//var included []int
	//for i := range ck.servers {
	//	if i == ck.curServer {
	//		continue
	//	}
	//	included = append(included, i)
	//}
	//idx := rand.Intn(len(included))
	//ck.curServer = included[idx]
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(clientID int, servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		id:       clientID,
		serialNo: 1,
		servers:  servers,
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) (val string) {
	args := &GetArgs{
		ClientID: ck.id,
		SerialNo: ck.serialNo,
		Key:      key,
	}
	logrus.Infof("%s Get(%+v)", ck, args)
	defer func() {
		ck.serialNo++
		logrus.Infof("%s Get(%+v) finished, value=(%s)", ck, args, val)
	}()
	for {
		reply := &GetReply{}
		logrus.Tracef("%s.Get(%s)", ck, key)
		ok := ck.servers[ck.curServer].Call("KVServer.Get", args, reply)
		if !ok {
			logrus.Tracef("%s.Get(%s) not ok", ck, key)
			ck.chooseAnotherServer()
			continue
		}

		switch reply.Err {
		case OK:
			val = reply.Value
			return
		case ErrNoKey:
			val = ""
			return
		case ErrWrongLeader:
			logrus.Tracef("%s.Get(%s) reply ErrWrongLeader", ck, key)
			ck.chooseAnotherServer()
		case ErrRaftTimeout:
			ck.chooseAnotherServer()
		default:
			logrus.Warnf("Get reply.Err:%s", reply.Err)
		}
	}
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("[Client%d]", ck.id)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		ClientID: ck.id,
		SerialNo: ck.serialNo,
		Key:      key,
		Value:    value,
		Op:       op,
	}
	logrus.Infof("%s PutAppend(%+v)", ck, args)
	defer func() {
		ck.serialNo++
		logrus.Infof("%s PutAppend(%+v) finished", ck, args)
	}()
	for {
		reply := &PutAppendReply{}
		logrus.Tracef("%s.PutAppend(%+v)", ck, args)
		ok := ck.servers[ck.curServer].Call("KVServer.PutAppend", args, reply)
		if !ok {
			logrus.Tracef("%s.PutAppend(%+v) not ok", ck, args)
			ck.chooseAnotherServer()
			continue
		}

		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			logrus.Tracef("%s.PutAppend(%+v) ErrWrongLeader", ck, args)
			ck.chooseAnotherServer()
		case ErrRaftTimeout:
			ck.chooseAnotherServer()
		default:
			logrus.Warnf("PutAppend reply.Err:%s", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpTypePut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpTypeAppend)
}
