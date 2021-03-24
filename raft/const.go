package raft

import "time"

const (
	ElectionTimeout = int64(time.Second)

	heartbeatIntv = 100 * time.Millisecond

	snapshotTriggerCond = 1000

	logOutput = "/Users/zhanghao1/code/6.824/raft/raft.log"
	//logOutput = ""
)
