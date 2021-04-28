package kvraft

import (
	"github.com/sirupsen/logrus"
	"math/rand"
	"mit6.824/labrpc"
)
import crand "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	curLeader int
}

func (ck *Clerk) chooseAnotherServer() {
	if len(ck.servers) == 1 {
		return
	}
	var included []int
	for i := range ck.servers {
		if i == ck.curLeader {
			continue
		}
		included = append(included, i)
	}
	idx := rand.Intn(len(included))
	ck.curLeader = included[idx]
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
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
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key}
	reply := &GetReply{}
	for {
		ok := ck.servers[ck.curLeader].Call("KVServer.Get", args, reply)
		if !ok {
			ck.chooseAnotherServer()
			continue
		}

		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			ck.curLeader = reply.CurrentLeader
		default:
			logrus.Debugf("Get reply.Err:%s", reply.Err)
		}
	}
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
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := &PutAppendReply{}
	for {
		ok := ck.servers[ck.curLeader].Call("KVServer.PutAppend", args, reply)
		if !ok {
			ck.chooseAnotherServer()
			continue
		}

		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			ck.curLeader = reply.CurrentLeader
		default:
			logrus.Debugf("PutAppend reply.Err:%s", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpTypePut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpTypeAppend)
}
