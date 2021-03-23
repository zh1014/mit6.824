package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

func (rf *Raft) requestPreVoteFrom(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		CreateTs:    nowUnixNano(),
	}
	args.LastLogTerm, args.LastLogIndex = rf.Log.lastEntryTermIndex()
	logrus.Debugf("%s requestPreVoteFrom peer%d, args=%+v", rf.desc(), peerID, args)
	reply := new(RequestVoteReply)
	ok := rf.PreVoteRPC(peerID, args, reply)
	if rf.killed() {
		return
	}
	if !ok {
		logrus.Debugf("%s requestPreVoteFrom peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
		return
	}
	logrus.Debugf("%s requestPreVoteFrom peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
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
	rf.Dirty.Mark()
	if rf.gotMajorityVote() {
		rf.becomeCandidate()
	}
}

func (rf *Raft) PreVoteRPC(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.PreVote", args, reply)
	logrus.Debugf("PreVoteRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.mu.Lock()
	return ok
}

func (rf *Raft) PreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	lastLogTerm, lastLogIdx := rf.Log.lastEntryTermIndex()
	defer func() {
		logrus.Debugf("%s exec PreVote, lastLog=[Index%d,Term%d], args=%+v, reply=%+v",
			rf.desc(), lastLogIdx, lastLogTerm, args, reply)
		rf.persistIfDirty()
		rf.mu.Unlock()
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
