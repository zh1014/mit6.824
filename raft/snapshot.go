package raft

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Snapshoter interface {
	Snapshot(lastIncluded int, snapshot []byte)
	//CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool
}

type InstallSnapshotArgs struct {
	Term         int
	Included     int
	IncludedTerm int
	Snapshot     []byte
	CreateTs     int64
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if (args.Term == rf.currentTerm && rf.role != follower) || args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.Included <= rf.Log.matchWithLeader {
		return
	}
	rf.Log.InstallSnapshot(args.Snapshot, args.Included, args.IncludedTerm)
}

func (rf *Raft) sendSnapshotTo(peerID int) error {
	// outer locked
	var err error
	args := &InstallSnapshotArgs{
		Term: rf.currentTerm,
	}
	for {
		if err = rf.checkStopSyncLog(args.Term); err != nil {
			break
		}
		args.CreateTs = nowUnixNano()
		args.Snapshot = rf.persister.ReadSnapshot()
		args.Included = rf.Log.lastIncluded
		args.IncludedTerm = rf.Log.lastIncludeTerm
		reply := new(InstallSnapshotReply)
		ok := rf.InstallSnapshotRPC(peerID, args, reply)
		if !ok {
			continue // retry
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			err = ErrNewTermFound
			break
		}
		rf.Log.leaderUpdateMatchIndex(rf.currentTerm, peerID, args.Included)
		break
	}
	if err != nil {
		logrus.Debugf("%s sendSnapshotTo peer%d end: %v", rf.Brief(), peerID, err)
	}
	return err
}

func (rf *Raft) InstallSnapshotRPC(peerID int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	logrus.Debugf("%s InstallSnapshot to peer %d, log=%s", rf.Brief(), peerID, rf.Log.Brief())
	rf.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.InstallSnapshot", args, reply)
	logrus.Debugf("peer%d->peer%d InstallSnapshot [ok=%v CreateTs %d,cost %v]", rf.me, peerID, ok, args.CreateTs, time.Now().Sub(start))
	rf.Lock()
	return ok
}

func (rf *Raft) Snapshot(lastIncluded int, snapshot []byte) {
	rf.Lock()
	defer rf.Unlock()
	from := rf.Log.Brief()
	rf.Log.Snapshot(lastIncluded)
	rf.PersistStateAndSnapshot(snapshot)
	logrus.Debugf("%s snapshot done %s -> %s, snapshotSize %d", rf.Brief(), from, rf.Log.Brief(), rf.persister.SnapshotSize())
}

//func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
//	return false
//}
