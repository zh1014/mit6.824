package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRaftTimeout = "ErrRaftTimeout"

	OpTypeGet    = "Get"
	OpTypePut    = "Put"
	OpTypeAppend = "Append"

	RaftOpTimeout = time.Second
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientID int
	SerialNo int
	Key      string
	Value    string
	Op       string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientID int
	SerialNo int
	Key      string
}

type GetReply struct {
	Err   Err
	Value string
}
