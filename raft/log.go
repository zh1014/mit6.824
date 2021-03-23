package raft

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"mit6.824/labrpc"
	"sync"
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
	log.Dirty.Mark()
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
		log.Dirty.Mark()
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

// 根据 单增索引 在log中查找对应条目，返回实际位置
// 返回值小于0，则log中没找到
// 返回值等于 -1， 时代表刚好是上一个apply的条目
// outer lock
func (log *Log) findEntryWithTerm(idx, term int) int {
	realIndex := (idx - log.lastIncluded) - 1
	if realIndex >= log.Len() {
		return -2
	}
	if realIndex == -1 && term != log.lastIncludeTerm {
		realIndex = -2
	}
	if realIndex >= 0 && log.slice[realIndex].Term != term {
		realIndex = -2
	}
	return realIndex
}

func (log *Log) findEntry(monoIdx int) *labrpc.LogEntry {
	realIndex := log.getRealIndex(monoIdx)
	if realIndex < 0 || realIndex >= log.Len() {
		return nil
	}
	return log.slice[realIndex]
}

func (log *Log) getRealIndex(monoIdx int) int {
	return (monoIdx - log.lastIncluded) - 1
}

// 查找某个term的末尾entry，返回其单增索引. 找不到则返回 -1
func (log *Log) lastIndexOfTerm(t int) int {
	idx := 0
	for i := log.Len() - 1; i >= 0; i-- {
		if log.slice[i].Term == t {
			idx = log.getMonoIndex(i)
			break
		}
	}
	if idx == 0 && log.lastIncludeTerm == t {
		idx = log.lastIncluded
	}
	return idx
}

func (log *Log) getMonoIndex(ri int) int {
	if ri < 0 {
		panic(fmt.Sprintf("getMonoIndex: ri=%v", ri))
	}
	return log.lastIncluded + ri + 1
}

func (log *Log) applyNextCmd() ApplyMsg {
	log.lastApplied++
	return ApplyMsg{
		CommandValid: true,
		Command:      log.slice[log.getRealIndex(log.lastApplied)].Cmd,
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

func (log *Log) findMatchQuickly(args *AppendEntryArgs, reply *AppendEntryReply) int {
	var realIndex int
	if reply.LastIndexOfTerm > 0 {
		// 快速获取match位置. 直接定位到对方的args.PrevLogTerm的最后一条的LogEntry
		realIndex = log.findEntryWithTerm(reply.LastIndexOfTerm, args.PrevLogTerm)
	} else {
		// 获取可能match的位置. 获取最后一条term小于args.PrevLogTerm的LogEntry
		realIndex = log.Len() - 1
		for ; realIndex >= 0; realIndex-- {
			if log.slice[realIndex].Term < args.PrevLogTerm {
				break
			}
		}
		if realIndex == -1 && log.lastIncludeTerm == args.PrevLogTerm {
			realIndex--
		}
	}
	return realIndex
}
