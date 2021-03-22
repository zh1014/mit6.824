package raft

import "github.com/sirupsen/logrus"

type Snapshot struct {
	lastIncluded    int
	lastIncludeTerm int
}

type InstallSnapshotArgs struct {
}

type InstallSnapshotReply struct {
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// TODO howZ:
}

func (rf *Raft) sendSnapshotTo(peerID int) {
	// TODO howz
	logrus.Debugf("need snapshot to peer %d", peerID)
	//rf.printLog()
	panic("snapshot")
}
