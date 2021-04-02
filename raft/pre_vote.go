package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

func (rf *Raft) requestPreVoteFrom(peerID int) {
	rf.Lock()
	defer rf.Unlock()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		CreateTs:    nowUnixNano(),
	}
	args.LastLogTerm, args.LastLogIndex = rf.Log.lastEntryTermIndex()
	logrus.Debugf("%s requestPreVoteFrom peer%d, args=%+v", rf.Brief(), peerID, args)
	reply := new(RequestVoteReply)
	ok := rf.PreVoteRPC(peerID, args, reply)
	if rf.killed() {
		return
	}
	if !ok {
		logrus.Debugf("%s requestPreVoteFrom peer%d RPC failed, CreateTs=%d", rf.Brief(), peerID, args.CreateTs)
		return
	}
	logrus.Debugf("%s requestPreVoteFrom peer%d returned, CreateTs=%d, reply=%+v", rf.Brief(), peerID, args.CreateTs, reply)
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.role != preCandidate {
		// 预选举已经结束
		return
	}
	if !reply.VoteGranted {
		return
	}
	rf.voteGot[peerID] = true
	rf.MarkDirty()
	if rf.gotMajorityVote() {
		rf.becomeCandidate()
	}
}

func (rf *Raft) PreVoteRPC(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.PreVote", args, reply)
	logrus.Debugf("PreVoteRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.Lock()
	return ok
}

func (rf *Raft) PreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	lastLogTerm, lastLogIdx := rf.Log.lastEntryTermIndex()
	defer func() {
		logrus.Debugf("%s exec PreVote, lastLog=[Index%d,Term%d], args=%+v, reply=%+v",
			rf.Brief(), lastLogIdx, lastLogTerm, args, reply)
		rf.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if !args.AsUpToDateAs(lastLogTerm, lastLogIdx) {
		return
	}
	if nowUnixNano()-rf.lastHeartbeat < ElectionTimeout {
		return
	}
	reply.VoteGranted = true
}
