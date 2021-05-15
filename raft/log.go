package raft

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"sort"
	"sync"
)

type RaftHandle interface {
	Brief() string
	MarkDirty()
	PersistStateAndSnapshot(snapshot []byte)
	sync.Locker
}

const (
	realIndexLastApplied = -1
	realIndexInvalid     = -2
)

type LeaderState struct {
	nextIndex     []int
	matchIndex    []int // match with followers
	lastHeartbeat []int64
	newEntryCond  *sync.Cond
}

func (l *LeaderState) needHeartbeat(peerID int) bool {
	return nowUnixNano()-l.lastHeartbeat[peerID] > HeartbeatIntv
}

func (l *LeaderState) updateMatchIndex(peerID, match int) {
	if match <= l.matchIndex[peerID] {
		return
	}
	l.matchIndex[peerID] = match
	l.nextIndex[peerID] = match + 1
}

func (l *LeaderState) encode(encoder *labgob.LabEncoder) {
	checkErr(encoder.Encode(l.nextIndex))
	checkErr(encoder.Encode(l.matchIndex))
}

func (l *LeaderState) decode(decoder *labgob.LabDecoder) {
	checkErr(decoder.Decode(&l.nextIndex))
	checkErr(decoder.Decode(&l.matchIndex))
}

// matchWithLeader > commitIndex at most time
// commitIndex > lastApplied > lastIncluded at any time
type Log struct {
	entries         []*labrpc.LogEntry
	matchWithLeader int
	commitIndex     int
	lastApplied     int
	snapshotMaking  int // equal to lastIncluded at most time. bigger than lastIncluded only after send
	lastIncluded    int
	lastIncludeTerm int
	applyCond       *sync.Cond
	applyCh         chan ApplyMsg
	leaderState     *LeaderState
	raftHandle      RaftHandle
}

func (log *Log) initLeaderState(rf *Raft) {
	log.leaderState = &LeaderState{
		nextIndex:     make([]int, len(rf.peers)),
		matchIndex:    make([]int, len(rf.peers)),
		lastHeartbeat: make([]int64, len(rf.peers)),
		newEntryCond:  sync.NewCond(rf),
	}
	for i := range log.leaderState.nextIndex {
		log.leaderState.nextIndex[i] = log.RToM(log.Len())
	}
}

func (log *Log) mergeEntries(start int, entries []*labrpc.LogEntry) {
	if len(entries) == 0 {
		return
	}
	for i, entry := range entries {
		if start+i >= len(log.entries) {
			log.entries = append(log.entries, entries[i:]...)
			break
		}
		if entry.Term != log.entries[start+i].Term {
			log.entries = log.entries[:start+i]
			log.entries = append(log.entries, entries[i:]...)
			break
		}
	}
	log.raftHandle.MarkDirty()
}

func (log *Log) Len() int {
	return len(log.entries)
}

// 只能提交leader已经提交，且肯定与leader匹配的部分LogEntry
func (log *Log) updateCommitIdxIfNeed(leaderCommit int) {
	commit := min(leaderCommit, log.matchWithLeader)
	if commit > log.commitIndex {
		logrus.Tracef("update commitIndex %v, log=%s", commit, log.Brief())
		log.commitIndex = commit
		log.applyCond.Signal()
		log.raftHandle.MarkDirty()
	}
}

func (log *Log) updateMatchWithLeaderIfNeed(match int) {
	if match > log.matchWithLeader {
		log.matchWithLeader = match
	}
}

func (log *Log) leaderUpdateMatchIndex(currentTerm, peerID, match int) {
	log.leaderState.updateMatchIndex(peerID, match)
	log.checkCommit(currentTerm)
}

// outer lock
func (log *Log) lastEntryTermIndex() (term, idx int) {
	lenLog := log.Len()
	if lenLog == 0 {
		term, idx = log.lastIncludeTerm, log.lastIncluded
		return
	}

	lastLog := log.entries[lenLog-1]
	term = lastLog.Term
	idx = log.lastIncluded + lenLog
	return
}

// outer lock
func (log *Log) findEntryWithTerm(idx, term int) (int, error) {
	ri, err := log.MToR(idx)
	if err == nil {
		if log.entries[ri].Term != term {
			err = NotInLog
		}
	} else if err == IncludedNotInLog {
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
	return log.entries[realIndex]
}

// 查找某个term的末尾entry，返回其单增索引. 找不到则返回 -1
func (log *Log) lastIndexOfTerm(t int) int {
	idx := 0
	for i := log.Len() - 1; i >= 0; i-- {
		if log.entries[i].Term == t {
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
	NotInLog         = errors.New("log does not contain this monotonically increasing index")
	IncludedNotInLog = errors.New("log at monotonically increasing index is the last included")
)

// convert monotonically increasing index to real index
func (log *Log) MToR(monoIdx int) (int, error) {
	ri := (monoIdx - log.lastIncluded) - 1
	if ri == -1 {
		return -1, IncludedNotInLog
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
	applying, err := log.MToR(log.lastApplied)
	if err != nil {
		logrus.Fatalf("%v: log=%v", err, log.String())
	}
	msg := ApplyMsg{
		Type: 		  MsgLogEntry,
		Command:      log.entries[applying].Cmd,
		CommandIndex: log.lastApplied,
	}
	//logrus.Debugf("%s applying msg=%+v", log.raftHandle.Brief(), msg)
	return msg
}

func (log *Log) EstimateSize() int {
	return (log.lastApplied - log.snapshotMaking) * LogEntrySize
}

func (log *Log) canApply() bool {
	return log.lastApplied < log.commitIndex
}

func (log *Log) String() string {
	return fmt.Sprintf("{commit:%d,app:%d,include:%d,inclTerm:%d,entries:%s}",
		log.commitIndex, log.lastApplied, log.lastIncluded, log.lastIncludeTerm, entriesString(log.lastIncluded+1, log.entries))
}

func (log *Log) Brief() string {
	return fmt.Sprintf("{commit:%d,app:%d,include:%d,inclTerm:%d,(%d)entries:%s}",
		log.commitIndex, log.lastApplied, log.lastIncluded, log.lastIncludeTerm, log.Len(), log.EntriesBrief())
}

func (log *Log) EntriesBrief() string {
	// display displayNum both head and tail
	const displayNum = 3
	if log.Len() <= 2*displayNum {
		return entriesString(log.lastIncluded+1, log.entries)
	}
	headEnd := displayNum
	headStartMono := log.lastIncluded + 1
	tailStart := log.Len() - displayNum
	tailStartMono := log.lastIncluded + tailStart + 1
	return entriesString(headStartMono, log.entries[:headEnd]) + "..." + entriesString(tailStartMono, log.entries[tailStart:])
}

func (log *Log) findEntryTermLess(term int) (int, error) {
	var ri int
	ri = log.Len() - 1
	for ; ri >= 0; ri-- {
		if log.entries[ri].Term < term {
			break
		}
	}
	if ri >= 0 {
		return log.RToM(ri), nil
	}
	if log.lastIncludeTerm < term {
		return log.lastIncluded, nil
	}
	return 0, NotInLog
}

func (log *Log) prepareNextAppendArgs(peerID int, args *AppendEntryArgs) (ok bool) {
	prevRI, err := log.MToR(log.leaderState.nextIndex[peerID] - 1)
	if err == nil {
		args.PrevLogIndex = log.RToM(prevRI)
		args.PrevLogTerm = log.entries[prevRI].Term
		args.Entries = make([]*labrpc.LogEntry, len(log.entries[prevRI+1:]))
		copy(args.Entries, log.entries[prevRI+1:])
	} else if err == IncludedNotInLog {
		args.PrevLogIndex = log.lastIncluded
		args.PrevLogTerm = log.lastIncludeTerm
		args.Entries = make([]*labrpc.LogEntry, len(log.entries))
		copy(args.Entries, log.entries)
	} else {
		return false
	}
	args.LeaderCommit = log.commitIndex
	args.CreateTs = nowUnixNano()
	log.leaderState.lastHeartbeat[peerID] = nowUnixNano()
	return true
}

func (log *Log) adjustNextIndex(peerID, PrevLogTerm, LastIndexOfTerm int) bool {
	if LastIndexOfTerm > 0 {
		log.leaderState.nextIndex[peerID] = LastIndexOfTerm + 1
	} else {
		if PrevLogTerm == 1 {
			log.leaderState.nextIndex[peerID] = 1
		}
		mi, err := log.findEntryTermLess(PrevLogTerm)
		if err != nil {
			return false
		} else {
			log.leaderState.nextIndex[peerID] = mi + 1
		}
	}
	return true
}

// outer lock
func (log *Log) checkCommit(currentTerm int) {
	sortedMatch := make([]int, len(log.leaderState.matchIndex))
	copy(sortedMatch, log.leaderState.matchIndex)
	sort.Ints(sortedMatch)

	// len(sortedMatch) must > 0
	// matchIndex 中位数作为 提交值
	// 把 matchIndex 升序排序后，取中位数。若中位数有2个，应该取小的
	middle := 0
	if len(sortedMatch)%2 == 0 {
		middle = (len(sortedMatch) - 1) / 2
	} else {
		middle = len(sortedMatch) / 2
	}
	medianMatch := sortedMatch[middle]
	if medianMatch > log.commitIndex && log.findEntry(medianMatch).Term == currentTerm {
		log.commitIndex = medianMatch
		log.raftHandle.MarkDirty()
		log.applyCond.Signal()
		logrus.Tracef("%s checkCommit, update commitIndex %d, log=%s", log.raftHandle.Brief(), log.commitIndex, log.Brief())
	}
}

func (log *Log) appendEntry(me, currentTerm int, command interface{}) (index, term int) {
	log.entries = append(log.entries, &labrpc.LogEntry{
		Term: currentTerm,
		Cmd:  command,
	})
	term, index = log.lastEntryTermIndex()
	log.leaderState.matchIndex[me] = index
	log.leaderState.newEntryCond.Broadcast()
	log.raftHandle.MarkDirty()
	return
}

func (log *Log) waitNewLogEntry(peerID int) {
	for log.synced(peerID) {
		log.leaderState.newEntryCond.Wait()
	}
}

func (log *Log) synced(pid int) bool {
	_, idx := log.lastEntryTermIndex()
	return idx == log.leaderState.matchIndex[pid]
}

func (log *Log) Snapshot(lastIncluded int) {
	ri, err := log.MToR(lastIncluded)
	if err != nil {
		logrus.Debugf("%s need not to make snapshot: %v, lastIncluded=%d, log.Brief=%s", log.raftHandle.Brief(), err, lastIncluded, log.Brief())
		return
	}
	if log.snapshotMaking < lastIncluded {
		log.snapshotMaking = lastIncluded
	}
	log.lastIncludeTerm = log.entries[ri].Term
	log.lastIncluded = lastIncluded
	log.trimLeft(ri, true)
	log.raftHandle.MarkDirty()
}

func (log *Log) InstallSnapshot(snapshot []byte, included, includeTerm int) {
	from := log.Brief()
	ri := log.Len() - 1
	if _, end := log.lastEntryTermIndex(); included < end {
		ri, _ = log.MToR(included)
	}
	log.trimLeft(ri, false)
	log.matchWithLeader = included
	log.commitIndex = included
	log.lastApplied = included
	log.snapshotMaking = included
	log.lastIncluded = included
	log.lastIncludeTerm = includeTerm
	log.raftHandle.PersistStateAndSnapshot(snapshot)
	logrus.Debugf("%s InstallSnapshot from %s -> %s, snapshot size %d", log.raftHandle.Brief(), from, log.Brief(), len(snapshot))
	log.raftHandle.Unlock()
	log.applyCh <- ApplyMsg{
		Type:		  MsgInstallSnapshot,
		Command:      snapshot,
		CommandIndex: included,
	}
	log.raftHandle.Lock()
}

// drop entries [0,trimIdx] (including trimIdx)
func (log *Log) trimLeft(trimIdx int, allocate bool) {
	if allocate {
		newSli := make([]*labrpc.LogEntry, len(log.entries[trimIdx+1:]))
		copy(newSli, log.entries[trimIdx+1:])
		log.entries = newSli
	} else {
		keep := log.entries[trimIdx+1:]
		copy(log.entries, keep)
		log.entries = log.entries[:len(keep)]
	}
}

func (log *Log) encode(encoder *labgob.LabEncoder) {
	checkErr(encoder.Encode(log.entries))
	checkErr(encoder.Encode(log.matchWithLeader))
	checkErr(encoder.Encode(log.commitIndex))
	checkErr(encoder.Encode(log.lastIncluded))
	checkErr(encoder.Encode(log.lastIncludeTerm))
	log.leaderState.encode(encoder)
}

func (log *Log) decode(decoder *labgob.LabDecoder) {
	checkErr(decoder.Decode(&log.entries))
	checkErr(decoder.Decode(&log.matchWithLeader))
	checkErr(decoder.Decode(&log.commitIndex))
	checkErr(decoder.Decode(&log.lastIncluded))
	log.snapshotMaking = log.lastIncluded
	checkErr(decoder.Decode(&log.lastIncludeTerm))
	log.leaderState.decode(decoder)
}
