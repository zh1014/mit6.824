package raft

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"mit6.824/labrpc"
	"time"
)

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	CreateTs     int64
	Entries      []*labrpc.LogEntry
}

func (args *AppendEntryArgs) String() string {
	return fmt.Sprintf("{Term=%v,LeaderID=%v,PrevLog[Idx%v Term%v],LeaderCommit=%v,CreateTs=%d, [%d]Entries=%s}",
		args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.CreateTs, len(args.Entries), args.StringEntries())
}

func (args *AppendEntryArgs) StringEntries() string {
	const display = 3
	if len(args.Entries) <= display {
		return entriesString(args.PrevLogIndex+1, args.Entries)
	}
	start := len(args.Entries) - display
	startMonoIdx := args.PrevLogIndex + start + 1
	return "..." + entriesString(startMonoIdx, args.Entries[start:])
}

type AppendEntryReply struct {
	Term            int
	Success         bool
	LastIndexOfTerm int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.Lock()
	defer rf.Unlock()
	logrus.Debugf("%s receive AppendEntry, args=%s", rf.desc(), args)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	defer rf.Log.updateCommitIdx(args.LeaderCommit) // 收到leader（term不小于自己）的消息，就可能更新 commitIndex
	rf.lastHeartbeat = nowUnixNano()
	rf.resetTimeout()
	if (args.Term == rf.currentTerm && rf.role != follower) || args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	prev, err := rf.Log.findEntryWithTerm(args.PrevLogIndex, args.PrevLogTerm)
	if err == NotInLog {
		reply.Success = false
		reply.LastIndexOfTerm = rf.Log.lastIndexOfTerm(args.PrevLogTerm)
		return
	}
	rf.Log.updateMatchIndex(args) // 只要PrevLog匹配成功，就可能更新matchIndex
	rf.Log.appendEntries(prev+1, args.Entries)
	if rf.Log.Len() > snapshotTriggerCond {
		rf.snapshot()
	}
	reply.Success = true
}

func (rf *Raft) AppendEntryRPC(peerID int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.AppendEntry", args, reply)
	logrus.Debugf("AppendEntryRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.Lock()
	return ok
}

func (rf *Raft) syncLogEntriesTo(peerID int) {
	rf.Lock()
	defer rf.Unlock()
	logrus.Debugf("%s syncLogEntriesTo %d start....", rf.desc(), peerID)

	// the first heartbeat announces the newly elected leader
	args := &AppendEntryArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.Log.lastEntryTermIndex()

	for {
		// prepare for new AppendEntry RPC
		rf.leaderState.lastHeartbeat[peerID] = nowUnixNano()
		args.LeaderCommit = rf.Log.commitIndex
		args.CreateTs = nowUnixNano()

		reply := &AppendEntryReply{}
		logrus.Debugf("%s AppendEntry to peer%d, args=%s", rf.desc(), peerID, args)
		ok := rf.AppendEntryRPC(peerID, args, reply)
		// peer in bigger term found
		if reply.Term > rf.currentTerm {
			logrus.Debugf("%s syncLogEntriesTo [Term%d|Peer%d] end", rf.desc(), reply.Term, peerID)
			rf.becomeFollower(reply.Term)
			break
		}
		if err := rf.checkStopSyncLog(args.Term); err != nil {
			logrus.Debugf("%s syncLogEntriesTo peer%d end, after AppendEntryRPC: %v", rf.desc(), peerID, err)
			break
		}
		if !ok {
			logrus.Debugf("%s AppendEntry to peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
			// retry
			continue
		}

		logrus.Debugf("%s AppendEntry to peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
		if reply.Success {
			rf.leaderState.nextIndex[peerID] = args.PrevLogIndex + len(args.Entries) + 1
			rf.leaderState.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
			rf.checkCommit()

			rf.waitNewLogEntry(peerID)
			if err := rf.checkStopSyncLog(args.Term); err != nil {
				logrus.Debugf("%s syncLogEntriesTo peer%d end, after waitNewLogEntry: %v", rf.desc(), peerID, err)
				break
			}
		}
		rf.prepareNextAppend(peerID, args, reply)
	}
}

func (rf *Raft) prepareNextAppend(peerID int, args *AppendEntryArgs, reply *AppendEntryReply) {
	var (
		realIndexMatch int // 下一个可能match的位置
		err            error
	)
	if reply.Success {
		realIndexMatch, err = rf.Log.MToR(rf.leaderState.matchIndex[peerID])
	} else {
		// find match index quickly
		if reply.LastIndexOfTerm > 0 {
			realIndexMatch, err = rf.Log.findEntryWithTerm(reply.LastIndexOfTerm, args.PrevLogTerm)
		} else {
			// try decrease term
			realIndexMatch, err = rf.Log.findEntryTermLess(args.PrevLogTerm)
		}
	}
	if err == nil {
		args.PrevLogIndex = rf.Log.RToM(realIndexMatch)
		args.PrevLogTerm = rf.Log.slice[realIndexMatch].Term
	} else if err == LastAppliedNotInLog {
		args.PrevLogIndex = rf.Log.lastIncluded
		args.PrevLogTerm = rf.Log.lastIncludeTerm
	} else { // NotInLog
		rf.sendSnapshotTo(peerID)
	}
	args.Entries = rf.Log.slice[realIndexMatch+1:]
}

func (rf *Raft) checkStopSyncLog(syncTerm int) error {
	if rf.killed() {
		return errors.New("killed")
	}
	if rf.role != leader {
		return errors.New("not leader")
	}
	if syncTerm < rf.currentTerm {
		return errors.New("stale sync goroutine")
	}
	return nil
}

func (rf *Raft) sendHeartbeatTo(peerID int) {
	rf.Lock()
	defer rf.Unlock()

	now := nowUnixNano()
	rf.leaderState.lastHeartbeat[peerID] = now
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.Log.commitIndex,
		CreateTs:     now,
	}
	realIndexMatch, err := rf.Log.MToR(rf.leaderState.matchIndex[peerID])
	if err == nil {
		args.PrevLogIndex = rf.Log.RToM(realIndexMatch)
		args.PrevLogTerm = rf.Log.slice[realIndexMatch].Term
	} else if err == LastAppliedNotInLog {
		args.PrevLogIndex = rf.Log.lastIncluded
		args.PrevLogTerm = rf.Log.lastIncludeTerm
	} else { // NotInLog
		rf.sendSnapshotTo(peerID)
		args.PrevLogTerm, args.PrevLogIndex = rf.Log.lastEntryTermIndex()
	}
	reply := &AppendEntryReply{}
	logrus.Debugf("%s sendHeartbeatTo to peer%d, args=%s", rf.desc(), peerID, args)
	ok := rf.AppendEntryRPC(peerID, args, reply)
	if !ok { // retry
		logrus.Debugf("%s sendHeartbeatTo to peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
		return
	}
	if reply.Term > rf.currentTerm {
		logrus.Debugf("%s sendHeartbeatTo [Term%d|Peer%d], newer term found", rf.desc(), reply.Term, peerID)
		rf.becomeFollower(reply.Term)
		return
	}
	logrus.Debugf("%s sendHeartbeatTo to peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
}
