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
	logrus.Tracef("%s receive AppendEntry, args=%s, log=%s", rf.Brief(), args, rf.Log.Brief())

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// 收到 合法leader（term不小于自己）的消息
	rf.onReceiveHeartbeat(args.LeaderID, args.Term)
	prev, err := rf.Log.findEntryWithTerm(args.PrevLogIndex, args.PrevLogTerm)
	if err == NotInLog {
		// PrevLog匹配失败，通过 LastIndexOfTerm 减少重试次数
		reply.LastIndexOfTerm = rf.Log.lastIndexOfTerm(args.PrevLogTerm)
		reply.Success = false
	} else {
		// PrevLog匹配成功
		rf.Log.mergeEntries(prev+1, args.Entries)
		rf.Log.updateMatchWithLeaderIfNeed(args.PrevLogIndex + len(args.Entries))
		reply.Success = true
	}
	rf.Log.updateCommitIdxIfNeed(args.LeaderCommit)
}

func (rf *Raft) onReceiveHeartbeat(leaderID, leaderTerm int) {
	rf.lastHeartbeat = nowUnixNano()
	rf.resetTimeout()
	if (leaderTerm == rf.currentTerm && rf.role != follower) || leaderTerm > rf.currentTerm {
		rf.becomeFollower(leaderTerm)
	}
}

func (rf *Raft) AppendEntryRPC(peerID int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	subject := rf.Brief()
	action := "AppendEntry to"
	if len(args.Entries) == 0 {
		action = "SendHeartbeat to"
	}
	logrus.Tracef("%s %s peer%d, args=%s", subject, action, peerID, args)
	rf.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.AppendEntry", args, reply)
	logrus.Tracef("%s %s peer%d [CreateTs:%d,cost:%v,RPC-OK:%v], reply=%+v", subject, action, peerID, args.CreateTs, time.Now().Sub(start), ok, reply)
	rf.Lock()
	return ok
}

func (rf *Raft) syncLogEntriesTo(peerID int) {
	rf.Lock()
	defer rf.Unlock()
	logrus.Tracef("%s syncLogEntriesTo %d start....", rf.Brief(), peerID)
	var err error
	args := &AppendEntryArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	for {
		if err = rf.checkStopSyncLog(args.Term); err != nil {
			break
		}
		var ok bool
		ok = rf.Log.prepareNextAppendArgs(peerID, args)
		if !ok {
			err = rf.sendSnapshotTo(peerID)
			if err != nil {
				break
			}
			continue
		}
		reply := &AppendEntryReply{}
		ok = rf.AppendEntryRPC(peerID, args, reply)
		if !ok {
			continue // retry
		}
		// check should we stop replication (at this term) after a long wait
		err = rf.checkAppendEntryReturned(args, reply)
		if err != nil {
			break
		}
		if !reply.Success {
			// adjust the next index and retry...
			ok = rf.Log.adjustNextIndex(peerID, args.PrevLogTerm, reply.LastIndexOfTerm)
			if !ok {
				// if failed on adjusting, have to send snapshot
				err = rf.sendSnapshotTo(peerID)
				if err != nil {
					// if failed to send snapshot, we can not continue
					break
				}
				continue
			}
			continue
		}
		rf.Log.leaderUpdateMatchIndex(rf.currentTerm, peerID, args.PrevLogIndex+len(args.Entries))
		rf.Log.waitNewLogEntry(peerID)
	}
	logrus.Debugf("%s syncLogEntriesTo peer%d end: %v", rf.Brief(), peerID, err)
}

func (rf *Raft) checkAppendEntryReturned(args *AppendEntryArgs, reply *AppendEntryReply) error {
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return ErrNewTermFound
	}
	if err := rf.checkStopSyncLog(args.Term); err != nil {
		return err
	}
	return nil
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
	rf.Log.leaderState.lastHeartbeat[peerID] = now
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.Log.commitIndex,
		CreateTs:     now,
	}
	realIndexMatch, err := rf.Log.MToR(rf.Log.leaderState.matchIndex[peerID])
	if err == nil {
		args.PrevLogIndex = rf.Log.RToM(realIndexMatch)
		args.PrevLogTerm = rf.Log.entries[realIndexMatch].Term
	} else if err == IncludedNotInLog {
		args.PrevLogIndex = rf.Log.lastIncluded
		args.PrevLogTerm = rf.Log.lastIncludeTerm
	} else { // NotInLog
		//rf.sendSnapshotTo(peerID)
		//args.PrevLogTerm, args.PrevLogIndex = rf.Log.lastEntryTermIndex()
	}
	reply := &AppendEntryReply{}
	ok := rf.AppendEntryRPC(peerID, args, reply)
	if !ok {
		return
	}
	if reply.Term > rf.currentTerm {
		logrus.Tracef("%s sendHeartbeatTo [Term%d|Peer%d], newer term found", rf.Brief(), reply.Term, peerID)
		rf.becomeFollower(reply.Term)
		return
	}
	logrus.Tracef("%s sendHeartbeatTo to peer%d returned, CreateTs=%d, reply=%+v", rf.Brief(), peerID, args.CreateTs, reply)
}
