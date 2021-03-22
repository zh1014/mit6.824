package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

func (rf *Raft) becomePreCandidate() {
	defer rf.persist()
	from := rf.desc()
	rf.role = preCandidate
	rf.resetTimeout()
	logrus.Infof("%s -> %s", from, rf.desc())
	rf.candidateState.voteGot = make([]bool, len(rf.peers))

	// vote for self
	rf.votedFor = rf.me
	rf.candidateState.voteGot[rf.me] = true
	if rf.gotMajorityVote() {
		rf.initiateNewElection()
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestPreVoteFrom(i)
	}
}

func (rf *Raft) requestPreVoteFrom(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		CreateTs:    nowUnixNano(),
	}
	args.LastLogTerm, args.LastLogIndex = rf.lastLogTermIndex()
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
		rf.changeToFollower(reply.Term)
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
	rf.candidateState.voteGot[peerID] = true
	rf.markDirty()
	if rf.gotMajorityVote() {
		rf.initiateNewElection()
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
	lastLogTerm, lastLogIdx := rf.lastLogTermIndex()
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
		rf.changeToFollower(args.Term)
	}
	if !args.AsUpToDateAs(lastLogTerm, lastLogIdx) {
		return
	}
	if nowUnixNano()-rf.lastHeartbeat < ElectionTimeout {
		return
	}
	reply.VoteGranted = true
}
