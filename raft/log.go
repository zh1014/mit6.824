package raft

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"mit6.824/labrpc"
	"sync"
)

const (
	realIndexLastApplied = -1
	realIndexInvalid     = -2
)

type Log struct {
	slice           []*labrpc.LogEntry
	matchIndex      int
	commitIndex     int
	lastApplied     int
	lastIncluded    int
	lastIncludeTerm int

	applyCond *sync.Cond
	Dirty
}

func (log *Log) appendEntries(start int, entries []*labrpc.LogEntry) {
	if len(entries) == 0 {
		return
	}
	for i, entry := range entries {
		if start+i >= len(log.slice) {
			log.slice = append(log.slice, entries[i:]...)
			break
		}
		if entry.Term != log.slice[start+i].Term {
			log.slice = log.slice[:start+i]
			log.slice = append(log.slice, entries[i:]...)
			break
		}
	}
	log.MarkDirty()
	//rf.printLog()
}

func (log *Log) Len() int {
	return len(log.slice)
}

// 只能提交leader已经提交，且肯定与leader匹配的部分LogEntry
func (log *Log) updateCommitIdx(leaderCommit int) {
	commit := min(leaderCommit, log.matchIndex)
	if commit > log.commitIndex {
		logrus.Debugf("update commitIndex %v, log=%s", commit, log.StringLog())
		log.commitIndex = commit
		log.applyCond.Signal()
		log.MarkDirty()
	}
}

func (log *Log) updateMatchIndex(args *AppendEntryArgs) {
	match := args.PrevLogIndex + len(args.Entries)
	if match > log.matchIndex {
		log.matchIndex = match
	}
}

// outer lock
func (log *Log) lastEntryTermIndex() (term, idx int) {
	lenLog := log.Len()
	if lenLog == 0 {
		term, idx = log.lastIncludeTerm, log.lastIncluded
		return
	}

	lastLog := log.slice[lenLog-1]
	term = lastLog.Term
	idx = log.lastIncluded + lenLog
	return
}

// outer lock
func (log *Log) findEntryWithTerm(idx, term int) (int, error) {
	ri, err := log.MToR(idx)
	if err == nil {
		if log.slice[ri].Term != term {
			err = NotInLog
		}
	} else if err == LastAppliedNotInLog {
		if log.lastIncludeTerm != term {
			err = NotInLog
		}
	}
	return ri, err
}

func (log *Log) findEntry(monoIdx int) *labrpc.LogEntry {
	realIndex, err := log.MToR(monoIdx)
	if err != nil {
		return nil
	}
	return log.slice[realIndex]
}

// 查找某个term的末尾entry，返回其单增索引. 找不到则返回 -1
func (log *Log) lastIndexOfTerm(t int) int {
	idx := 0
	for i := log.Len() - 1; i >= 0; i-- {
		if log.slice[i].Term == t {
			idx = log.RToM(i)
			break
		}
	}
	if idx == 0 && log.lastIncludeTerm == t {
		idx = log.lastIncluded
	}
	return idx
}

var (
	NotInLog            = errors.New("log does not contain this monotonically increasing index")
	LastAppliedNotInLog = errors.New("log at monotonically increasing index is the last applied")
)

// convert monotonically increasing index to real index
func (log *Log) MToR(monoIdx int) (int, error) {
	ri := (monoIdx - log.lastIncluded) - 1
	if ri == -1 {
		return -1, LastAppliedNotInLog
	}
	if ri < -1 || ri >= log.Len() {
		return -1, NotInLog
	}
	return ri, nil
}

// convert real index to monotonically increasing index
func (log *Log) RToM(ri int) int {
	if ri < 0 {
		panic(fmt.Sprintf("RToM: ri=%v", ri))
	}
	return log.lastIncluded + ri + 1
}

// 查找MonoIndex对应Entry的Term
// 没找到Entry则返回0
func (log *Log) getTermByMonoIndex(mi int) int {
	if mi <= 0 {
		return 0
	}

	if e := log.findEntry(mi); e != nil {
		return e.Term
	}
	if mi == log.lastIncluded {
		return log.lastIncludeTerm
	}
	return 0
}

func (log *Log) applyOne() ApplyMsg {
	log.lastApplied++
	applying, _ := log.MToR(log.lastApplied)
	return ApplyMsg{
		CommandValid: true,
		Command:      log.slice[applying].Cmd,
		CommandIndex: log.lastApplied,
	}
}

func (log *Log) canApply() bool {
	return log.lastApplied < log.commitIndex
}

func (log *Log) StringLog() string {
	const display = 3
	if log.Len() <= display {
		return entriesString(log.lastIncluded+1, log.slice)
	}
	start := log.Len() - display
	startMonoIdx := log.lastIncluded + start + 1
	return "..." + entriesString(startMonoIdx, log.slice[start:])
}

func (log *Log) findEntryTermLess(term int) (int, error) {
	var ri int
	var err error
	ri = log.Len() - 1
	for ; ri >= 0; ri-- {
		if log.slice[ri].Term < term {
			break
		}
	}
	if ri == -1 {
		if log.lastIncludeTerm != term {
			err = LastAppliedNotInLog
		} else {
			err = NotInLog
		}
	}
	return ri, err
}
