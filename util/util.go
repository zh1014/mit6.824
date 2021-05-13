package util

import (
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

const LogOutput = "/Users/zhanghao1/code/6.824/output.log"

func LogConfig() {
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
	if LogOutput != "" {
		f, err := os.Create(LogOutput)
		if err != nil {
			panic(err)
		}
		logrus.SetOutput(f)
	}
	logrus.Infof("log ready....")
}

func Min(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

func NowUnixNano() int64 {
	return time.Now().UnixNano()
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
