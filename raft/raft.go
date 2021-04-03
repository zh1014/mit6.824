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
	"github.com/zh1014/algorithm/queue"
	"math/rand"
	"mit6.824/labrpc"
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

	sync.Mutex    // protect follow fields
	currentTerm   int
	role          raftRole
	votedFor      int
	voteGot       []bool // for preCandidate, candidate
	elecTimeout   int64
	lastHeartbeat int64
	Log           *Log
	Dirty
}

func (rf *Raft) Unlock() {
	rf.PersistStateIfDirty()
	rf.Mutex.Unlock()
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
		rf.Lock()
		if rf.role != leader && rf.timeout() {
			rf.becomePreCandidate()
		}
		if rf.role == leader {
			for peerID := range rf.peers {
				if peerID == rf.me {
					continue
				}
				if !rf.Log.leaderState.needHeartbeat(peerID) {
					continue
				}
				go rf.sendHeartbeatTo(peerID)
			}
		}
		rf.Unlock()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.role == leader
}

func (rf *Raft) voted() bool {
	return rf.votedFor >= 0
}

func (rf *Raft) Brief() string {
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
	rf.Lock()
	defer rf.Unlock()

	index := -1
	term := -1
	if rf.role != leader {
		return index, term, false
	}
	logrus.Debugf("%s Start(%v) ...", rf.Brief(), command)
	index, term = rf.Log.appendEntry(rf.me, rf.currentTerm, command)
	return index, term, true
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
	rf.MarkDirty()
	from := rf.Brief()
	rf.role = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.Log.matchIndex = 0
	rf.resetTimeout()
	logrus.Infof("%s -> %s", from, rf.Brief())
}

func (rf *Raft) becomePreCandidate() {
	rf.MarkDirty()
	from := rf.Brief()
	rf.role = preCandidate
	rf.resetTimeout()
	logrus.Infof("%s -> %s", from, rf.Brief())
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
	rf.MarkDirty()
	from := rf.Brief()
	rf.role = candidate
	rf.currentTerm++
	rf.resetTimeout()
	logrus.Infof("%s -> %s", from, rf.Brief())
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
	rf.MarkDirty()
	logrus.Infof("%s -> leader", rf.Brief())
	rf.role = leader
	rf.Log.initLeaderState(rf)
	rf.startLogReplication()
}

func (rf *Raft) startLogReplication() {
	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		go rf.syncLogEntriesTo(peerID)
	}
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
	// init data-structure
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.resetTimeout()
	rf.initLog()
	if persist := persister.ReadRaftState(); len(persist) > 0 {
		rf.ReadPersist(persist)
	}

	rf.initStateMachine(me, persister, applyCh)
	if rf.role == leader {
		rf.startLogReplication()
	}
	go rf.ticker()
	logrus.Infof("%s server start...", rf.Brief())
	return rf
}

func (rf *Raft) initLog() {
	rf.Log = &Log{
		applyCh:    make(chan ApplyMsg),
		applyCond:  sync.NewCond(rf),
		raftHandle: rf,
	}
	rf.Log.initLeaderState(rf)
}

func (rf *Raft) initStateMachine(me int, persister *Persister, applyCh chan ApplyMsg) {
	go ApplyTransfer(rf.me, rf.Log.applyCh, applyCh)
	if rf.Log.lastIncluded > 0 {
		if persister.SnapshotSize() == 0 {
			logrus.Fatalf("peer%d snapshot missing, lastIncluded %d", me, rf.Log.lastIncluded)
		}
		rf.Log.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      persister.ReadSnapshot(),
			CommandIndex: rf.Log.lastIncluded,
		}
	}
	rf.Log.lastApplied = rf.Log.lastIncluded
	go rf.applyConstantly()
}

func (rf *Raft) applyConstantly() {
	rf.Lock()
	for {
		for !rf.Log.canApply() {
			rf.Log.applyCond.Wait()
		}
		if rf.killed() {
			break
		}

		for rf.Log.canApply() {
			msg := rf.Log.applyOne()
			rf.Unlock()
			rf.Log.applyCh <- msg
			rf.Lock()
			if rf.killed() {
				break
			}
			//logrus.Debugf("%s applied msg=%+v", rf.Brief(), msg)
		}
	}
	rf.Unlock()
}

func ApplyTransfer(peerID int, from <-chan ApplyMsg, to chan<- ApplyMsg) {
	var lastApplied int
	q := queue.NewLinkedQueue()
	for msg := range from {
		if msg.CommandValid {
			if msg.CommandIndex == lastApplied+1 {
				to <- msg
				lastApplied++
			} else if msg.CommandIndex > lastApplied+1 {
				q.PushBack(msg)
				logrus.Debugf("ApplyTransfer.lastApplied=%d peer%d PushBack msg=%+v", lastApplied, peerID, msg)
			} else {
				panic("unknown error")
			}
		} else {
			if msg.CommandIndex <= lastApplied {
				continue
			}
			to <- msg
			lastApplied = msg.CommandIndex
			for !q.IsEmpty() {
				front := q.Front().(ApplyMsg)
				if front.CommandIndex != lastApplied+1 {
					logrus.Fatalf("ApplyTransfer.lastApplied=%d, peer%d, queueFront=%+v", lastApplied, peerID, front)
				}
				to <- front
				lastApplied++
			}
		}
	}
}
