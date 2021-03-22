package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	CreateTs     int64
}

// only vote for candidate whose log is at least as up-to-date as this log
func (args *RequestVoteArgs) AsUpToDateAs(lastLogTerm, lastLogIdx int) bool {
	return args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	lastLogTerm, lastLogIdx := rf.lastLogTermIndex()
	defer func() {
		logrus.Debugf("%s exec RequestVote, lastLog=[Index%d,Term%d], votedFor %d: args=%+v, reply=%+v",
			rf.desc(), lastLogIdx, lastLogTerm, rf.votedFor, args, reply)
		rf.persistIfDirty()
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		return
	}
	if !rf.voted() && args.AsUpToDateAs(lastLogTerm, lastLogIdx) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.markDirty()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) requestVoteFrom(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		CreateTs:    nowUnixNano(),
	}
	args.LastLogTerm, args.LastLogIndex = rf.lastLogTermIndex()
	logrus.Debugf("%s requestVoteFrom peer%d, args=%+v", rf.desc(), peerID, args)
	reply := new(RequestVoteReply)
	ok := rf.RequestVoteRPC(peerID, args, reply)
	if rf.killed() {
		return
	}
	if !ok {
		logrus.Debugf("%s requestVoteFrom peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
		return
	}
	logrus.Debugf("%s requestVoteFrom peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.role != candidate {
		// 此轮选举已经结束
		return
	}
	if !reply.VoteGranted {
		return
	}
	rf.voteGot[peerID] = true
	if rf.gotMajorityVote() {
		rf.becomeLeader()
	}
	rf.markDirty()
}

func (rf *Raft) RequestVoteRPC(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.RequestVote", args, reply)
	logrus.Debugf("RequestVoteRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.mu.Lock()
	return ok
}

func (rf *Raft) gotMajorityVote() bool {
	count := 0
	for _, got := range rf.voteGot {
		if got {
			count++
		}
	}
	return count > len(rf.voteGot)/2
}
