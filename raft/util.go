package raft

import (
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func logConfig() {
	logrus.SetLevel(logrus.DebugLevel)
	if logOutput != "" {
		f, err := os.Create(logOutput)
		if err != nil {
			panic(err)
		}
		logrus.SetOutput(f)
	}
	logrus.Infof("log ready....")
}

func min(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

func nowUnixNano() int64 {
	return time.Now().UnixNano()
}
