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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()
	logrus.Debugf("%s receive AppendEntry, args=%s", rf.desc(), args)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	defer rf.updateCommitIdx(args.LeaderCommit) // 收到leader（term不小于自己）的消息，就可能更新 commitIndex
	rf.lastHeartbeat = nowUnixNano()
	rf.resetTimeout()
	if (args.Term == rf.currentTerm && rf.role != follower) || args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}

	var realIdxStart int // 从log中的start开始合并
	if args.PrevLogIndex > 0 {
		realIdxStart = rf.findEntryWithTerm(args.PrevLogIndex, args.PrevLogTerm) + 1
	}
	if realIdxStart < 0 {
		reply.Success = false
		reply.LastIndexOfTerm = rf.lastIndexOfTerm(args.PrevLogTerm)
		//logrus.Debugf("%s exec AppendEntry, log=%v", rf.desc(), rf.entriesString())
		return
	}
	rf.updateMatchIndex(args) // 只要PrevLog匹配成功，就可能更新matchIndex
	rf.appendEntries(realIdxStart, args.Entries)
	if len(rf.log) > snapshotTriggerCond {
		rf.snapshot()
	}
	reply.Success = true
}

func (rf *Raft) appendEntries(start int, entries []*labrpc.LogEntry) {
	if len(entries) == 0 {
		return
	}
	for i, entry := range entries {
		if start+i >= len(rf.log) {
			rf.log = append(rf.log, entries[i:]...)
			break
		}
		if entry.Term != rf.log[start+i].Term {
			rf.log = rf.log[:start+i]
			rf.log = append(rf.log, entries[i:]...)
			break
		}
	}
	rf.markDirty()
	//rf.printLog()
}

func (rf *Raft) AppendEntryRPC(peerID int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.mu.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.AppendEntry", args, reply)
	logrus.Debugf("AppendEntryRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.mu.Lock()
	return ok
}

func (rf *Raft) syncLogEntriesTo(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logrus.Debugf("%s syncLogEntriesTo %d start....", rf.desc(), peerID)

	// the first heartbeat announces the newly elected leader
	args := &AppendEntryArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()

	for {
		rf.leaderState.lastHeartbeat[peerID] = nowUnixNano()
		args.LeaderCommit = rf.commitIndex
		args.CreateTs = nowUnixNano()
		reply := &AppendEntryReply{}
		logrus.Debugf("%s AppendEntry to peer%d, args=%s", rf.desc(), peerID, args)
		ok := rf.AppendEntryRPC(peerID, args, reply)
		// peer in bigger term found
		if reply.Term > rf.currentTerm {
			logrus.Debugf("%s syncLogEntriesTo [Term%d|Peer%d] end", rf.desc(), reply.Term, peerID)
			rf.changeToFollower(reply.Term)
			rf.persist()
			break
		}
		if err := rf.checkStopSyncLog(args.Term); err != nil {
			logrus.Debugf("%s syncLogEntriesTo peer%d end, after AppendEntryRPC: %v", rf.desc(), peerID, err)
			break
		}
		if !ok { // retry
			logrus.Debugf("%s AppendEntry to peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
			continue
		}

		logrus.Debugf("%s AppendEntry to peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
		var realIndexMatch int // 下一个可能match的位置
		if reply.Success {
			rf.leaderState.nextIndex[peerID] = args.PrevLogIndex + len(args.Entries) + 1
			rf.leaderState.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
			rf.checkCommit()
			rf.persist()

			rf.waitNewLogEntry(peerID)
			if err := rf.checkStopSyncLog(args.Term); err != nil {
				logrus.Debugf("%s syncLogEntriesTo peer%d end, after waitNewLogEntry: %v", rf.desc(), peerID, err)
				break
			}

			realIndexMatch = rf.getRealIndex(rf.leaderState.matchIndex[peerID])
		} else {
			realIndexMatch = rf.findMatchQuickly(args, reply)
		}
		if realIndexMatch <= realIndexInvalid {
			rf.sendSnapshotTo(peerID)
		} else if realIndexMatch == realIndexLastApplied {
			args.PrevLogIndex = rf.snapshotMeta.lastIncluded
			args.PrevLogTerm = rf.snapshotMeta.lastIncludeTerm
		} else {
			args.PrevLogIndex = rf.getMonoIndex(realIndexMatch)
			args.PrevLogTerm = rf.log[realIndexMatch].Term
		}
		args.Entries = rf.log[realIndexMatch+1:]
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	now := nowUnixNano()
	rf.leaderState.lastHeartbeat[peerID] = now
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		CreateTs:     now,
	}
	realIndexMatch := rf.getRealIndex(rf.leaderState.matchIndex[peerID])
	if realIndexMatch <= realIndexInvalid {
		rf.sendSnapshotTo(peerID)
		args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()
	} else if realIndexMatch == realIndexLastApplied {
		args.PrevLogIndex = rf.snapshotMeta.lastIncluded
		args.PrevLogTerm = rf.snapshotMeta.lastIncludeTerm
	} else {
		args.PrevLogIndex = rf.getMonoIndex(realIndexMatch)
		args.PrevLogTerm = rf.log[realIndexMatch].Term
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
		rf.changeToFollower(reply.Term)
		return
	}
	logrus.Debugf("%s sendHeartbeatTo to peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
}
