package raft

import (
	"errors"
	"time"
)

const (
	ElectionTimeout = int64(time.Second)

	HeartbeatIntv = ElectionTimeout / 10

	SnapshotCond = 200

	LogOutput = "/Users/zhanghao1/code/6.824/raft/raft.txt"
)

var (
	ErrNewTermFound  = errors.New("new term found")
)
