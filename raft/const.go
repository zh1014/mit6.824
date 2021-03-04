package raft

import "time"

const (
	heartbeatIntv = 50 * time.Millisecond

	snapshotTriggerCond = 1000

	logOutput = "/Users/zhanghao1/code/6.824/raft/raft.log"
	//logOutput = ""
)

