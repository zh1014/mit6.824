package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"mit6.824/labrpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type raftRole int

const (
	follower raftRole = iota
	preCandidate
	candidate
	leader
)

var roleString = map[raftRole]string{
	follower:     "follower",
	preCandidate: "preCandidate",
	candidate:    "candidate",
	leader:       "leader",
}

func (r raftRole) String() string {
	return roleString[r]
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers      []*labrpc.ClientEnd // RPC end points of all peers.
	me         int                 // this peer's index into peers[]
	persister  *Persister          // Object to hold this peer's persisted state
	dead       int32               // set by Kill()
	rand       *rand.Rand
	statusCond *sync.Cond

	mu            sync.Mutex // protect follow fields
	currentTerm   int
	role          raftRole
	votedFor      int
	voteGot       []bool // for preCandidate, candidate
	elecTimeout   int64
	lastHeartbeat int64
	Log           *Log
	leaderState   *LeaderState
	Dirty
}

type LeaderState struct {
	nextIndex     []int
	matchIndex    []int
	newEntryCond  *sync.Cond
	lastHeartbeat []int64
}

func (l *LeaderState) needHeartbeat(peerID int) bool {
	return nowUnixNano()-l.lastHeartbeat[peerID] > int64(heartbeatIntv)
}

// outer lock
func (rf *Raft) resetTimeout() {
	rf.elecTimeout = nowUnixNano() + rf.randElectionTimeout()
}

func (rf *Raft) timeout() bool {
	return nowUnixNano() > rf.elecTimeout
}

func (rf *Raft) ticker() {
	// TODO howZ: 每一个tick要check的东西太多了！ optimize：时间轮算法
	// fixme howz: 不停抢占锁
	const tickInterval = 5 * time.Millisecond
	for {
		<-time.After(tickInterval)
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.role != leader && rf.timeout() {
			rf.becomePreCandidate()
		}
		if rf.role == leader {
			for peerID := range rf.peers {
				if peerID == rf.me {
					continue
				}
				if !rf.leaderState.needHeartbeat(peerID) {
					continue
				}
				go rf.sendHeartbeatTo(peerID)
			}
		}
		rf.mu.Unlock()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

func (rf *Raft) voted() bool {
	return rf.votedFor >= 0
}

func (rf *Raft) printLog() {
	//logrus.Debugf("%s printLog, log=%v", rf.desc(), entriesString(rf.log))
}

func (rf *Raft) desc() string {
	desc := fmt.Sprintf("[Term%d|Peer%d|%v", rf.currentTerm, rf.me, rf.role)
	if rf.role != leader {
		timeoutLeft := (rf.elecTimeout - nowUnixNano()) / int64(time.Millisecond)
		desc += fmt.Sprintf("|ETo%d", timeoutLeft)
	}
	desc += "]"
	return desc
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	index := -1
	term := -1
	if rf.role != leader {
		return index, term, false
	}
	logrus.Debugf("%s Start(%v)", rf.desc(), command)
	rf.appendEntry(command)
	term, index = rf.Log.lastEntryTermIndex()
	rf.leaderState.matchIndex[rf.me] = index
	rf.leaderState.newEntryCond.Broadcast()
	if rf.Log.Len() > snapshotTriggerCond {
		rf.snapshot()
	}
	return index, term, true
}

func (rf *Raft) appendEntry(command interface{}) {
	rf.Log.slice = append(rf.Log.slice, &labrpc.LogEntry{
		Term: rf.currentTerm,
		Cmd:  command,
	})
	rf.Dirty.Mark()
	//rf.printLog()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) randElectionTimeout() int64 {
	return ElectionTimeout + rf.rand.Int63n(ElectionTimeout)
}

// outer lock
func (rf *Raft) becomeFollower(term int) {
	from := rf.desc()
	rf.role = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.Log.matchIndex = 0
	rf.resetTimeout()
	rf.Dirty.Mark()
	logrus.Infof("%s -> %s", from, rf.desc())
}

func (rf *Raft) becomePreCandidate() {
	defer rf.persist()
	from := rf.desc()
	rf.role = preCandidate
	rf.resetTimeout()
	logrus.Infof("%s -> %s", from, rf.desc())
	rf.voteGot = make([]bool, len(rf.peers))

	// vote for self
	rf.votedFor = rf.me
	rf.voteGot[rf.me] = true
	if rf.gotMajorityVote() {
		rf.becomeCandidate()
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestPreVoteFrom(i)
	}
}

func (rf *Raft) becomeCandidate() {
	defer rf.persist()
	from := rf.desc()
	rf.role = candidate
	rf.currentTerm++
	rf.resetTimeout()
	logrus.Infof("%s -> %s", from, rf.desc())
	rf.voteGot = make([]bool, len(rf.peers))

	// vote for self
	rf.votedFor = rf.me
	rf.voteGot[rf.me] = true
	if rf.gotMajorityVote() {
		rf.becomeLeader()
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVoteFrom(i)
	}
}

func (rf *Raft) becomeLeader() {
	logrus.Infof("%s -> leader", rf.desc())
	rf.role = leader
	rf.leaderState = &LeaderState{
		nextIndex:     make([]int, len(rf.peers)),
		matchIndex:    make([]int, len(rf.peers)),
		lastHeartbeat: make([]int64, len(rf.peers)),
		newEntryCond:  sync.NewCond(&rf.mu),
	}
	for i := range rf.leaderState.nextIndex {
		rf.leaderState.nextIndex[i] = rf.Log.getMonoIndex(rf.Log.Len())
	}

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		go rf.syncLogEntriesTo(peerID)
	}
	rf.Dirty.Mark()
}

func (rf *Raft) waitNewLogEntry(peerID int) {
	for rf.synced(peerID) {
		rf.leaderState.newEntryCond.Wait()
	}
}

// outer lock
func (rf *Raft) checkCommit() {
	sortedMatch := make([]int, len(rf.leaderState.matchIndex))
	copy(sortedMatch, rf.leaderState.matchIndex)
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
	if medianMatch > rf.Log.commitIndex && rf.Log.findEntry(medianMatch).Term == rf.currentTerm {
		rf.Log.commitIndex = medianMatch
		rf.Log.applyCond.Signal()
		logrus.Infof("%s checkCommit, update commitIndex %d, log=%s", rf.desc(), rf.Log.commitIndex, rf.Log.StringLog())
	}
}

// 查找MonoIndex对应Entry的Term
// 没找到Entry则返回0
func (rf *Raft) getTermByMonoIndex(mi int) int {
	if mi <= 0 {
		return 0
	}

	if e := rf.Log.findEntry(mi); e != nil {
		return e.Term
	}
	if mi == rf.Log.lastIncluded {
		return rf.Log.lastIncludeTerm
	}
	return 0
}

func (rf *Raft) synced(pid int) bool {
	_, idx := rf.Log.lastEntryTermIndex()
	return idx == rf.leaderState.matchIndex[pid]
}

func (rf *Raft) containsEntry(i, t int) bool {
	return rf.Log.findEntryWithTerm(i, t) >= -1
}

// 根据 单增索引 和 term 找到前一个entry的 单增索引 和 term
func (rf *Raft) getPreEntry(i, t int) (int, int) {
	ri := rf.Log.findEntryWithTerm(i, t)
	if ri <= -1 {
		return ri, -1
	}
	if ri == 0 {
		return rf.Log.lastIncluded, rf.Log.lastIncludeTerm
	}
	return rf.Log.getMonoIndex(ri - 1), rf.Log.slice[ri-1].Term
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.Log = &Log{
		applyCond: sync.NewCond(&rf.mu),
	}

	if rawData := persister.ReadRaftState(); len(rawData) > 0 {
		rf.readPersist(rawData)
	}
	if rf.role == follower {
		rf.resetTimeout()
	} else if rf.role == leader {
		rf.becomeLeader()
	}

	go rf.applyDamon(applyCh)
	go rf.ticker()
	return rf
}

func (rf *Raft) applyDamon(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	for {
		for !rf.Log.canApply() {
			rf.Log.applyCond.Wait()
		}
		if rf.killed() {
			break
		}

		for rf.Log.canApply() {
			msg := rf.Log.applyNextCmd()
			//logrus.Debugf("%s applying msg=%+v", rf.desc(), msg)
			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
			if rf.killed() {
				break
			}
			rf.persist()
			//logrus.Debugf("%s applied msg=%+v", rf.desc(), msg)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) snapshot() {
	ri := rf.Log.getRealIndex(rf.Log.lastApplied)
	if ri < 0 {
		logrus.Debugf("%s snapshot failed, len=%d, ri=%d, lastApplied=%v", rf.desc(), rf.Log.Len(), ri, rf.Log.lastApplied)
		return
	}
	// TODO howZ
	//rf.persister.SaveStateAndSnapshot()
}
