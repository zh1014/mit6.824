package raft

import (
	"errors"
	"time"
)

const (
	ElectionTimeout = int64(time.Second)

	HeartbeatIntv = ElectionTimeout / 10

	LogEntrySize  = 50 // byte

	LogOutput = "/Users/zhanghao1/code/6.824/raft/raft.txt"
)

type ApplyMsgType int

const (
	MsgLogEntry ApplyMsgType = iota
	MsgInstallSnapshot
	MsgMakeSnapshot
)

var (
	ErrNewTermFound = errors.New("new term found")
)
