package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"log"
	"mit6.824/labrpc"
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
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors:               false,
		DisableColors:             false,
		ForceQuote:                false,
		DisableQuote:              false,
		EnvironmentOverrideColors: false,
		DisableTimestamp:          false,
		FullTimestamp:             false,
		TimestampFormat:           time.StampMilli,
		DisableSorting:            false,
		SortingFunc:               nil,
		DisableLevelTruncation:    false,
		PadLevelText:              false,
		QuoteEmptyFields:          false,
		FieldMap:                  nil,
		CallerPrettyfier:          nil,
	})
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

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func entriesString(i int, entries []*labrpc.LogEntry) string {
	s := "["
	for _, e := range entries {
		s += fmt.Sprintf("%d{term=%v,cmd=%v}, ", i, e.Term, e.Cmd)
		i++
	}
	s += "]"
	return s
}
