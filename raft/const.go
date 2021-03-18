package raft

import "time"

const (
	ElectionTimeout = int64(500 * time.Millisecond)

	heartbeatIntv = 50 * time.Millisecond

	snapshotTriggerCond = 1000

	logOutput = "/Users/zhanghao1/code/6.824/raft/raft.log"
	//logOutput = ""

	realIndexLastApplied = -1
	realIndexInvalid     = -2
)
