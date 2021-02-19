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
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "mit6.824/labrpc"

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
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// Persistent state
	currentTerm int
	votedFor    int
	log         []*labrpc.LogEntry

	// Volatile state
	peers           []*labrpc.ClientEnd // RPC end points of all peers.
	me              int                 // this peer's index into peers[]
	persister       *Persister          // Object to hold this peer's persisted state
	dead            int32               // set by Kill()

	mu              sync.Mutex // Lock to protect shared access to this peer's follow state
	currentLeader   int
	role            raftRole
	commitIndex     int
	elecTimeout     int64
	rand            *rand.Rand
	biggerTermFound bool
	statusCond      *sync.Cond
	applyCond       *sync.Cond
	snapshot        Snapshot

	candidateState *CandidateState
	leaderState    *LeaderState
}

type Snapshot struct {
	lastApplied     int
	lastAppliedTerm int
}

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
	appendEntry chan struct{} // 有新的cmd，或者需要发心跳包
	nextHeartbeat int64
}

type CandidateState struct {
	voteGot          []bool
	failedOnThisTerm bool // for candidate
}

// outer lock
func (rf *Raft) resetTimeout() {
	rf.elecTimeout = nowUnixNano() + rf.randElectionTimeout()
}

func (rf *Raft) ticker() {
	// TODO howZ: 每一个tick要check的东西太多了！ optimize：时间轮算法
	const tickInterval = time.Millisecond
	for {
		<- time.After(tickInterval)
		rf.mu.Lock()
		if rf.role == leader && nowUnixNano() > rf.leaderState.nextHeartbeat {
			rf.leaderState.nextHeartbeat = nowUnixNano() + heartbeatIntv
			rf.mu.Unlock()
			rf.leaderState.appendEntry <- struct{}{}
			rf.mu.Lock()
		}
		if rf.role != leader && nowUnixNano() > rf.elecTimeout {
			rf.statusCond.Signal()
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// only vote for candidate whose log is at least as up-to-date as this log
		lastLogTerm, lastLogIdx := rf.lastLogTermIndex()
		if args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIdx {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateID
		}
	} else if args.Term == rf.currentTerm && rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
}

// outer lock
func (rf *Raft) lastLogTermIndex() (term, idx int) {
	lenLog := len(rf.log)
	if lenLog == 0 {
		term, idx = rf.snapshot.lastAppliedTerm, rf.snapshot.lastApplied
		return
	}

	lastLog := rf.log[lenLog-1]
	term = lastLog.Term
	idx = rf.snapshot.lastApplied + lenLog
	return
}

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*labrpc.LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term            int
	Success         bool
	LastIndexOfTerm int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	switch rf.role {
	case follower:
		rf.resetTimeout()
	case candidate:
		rf.candidateState.failedOnThisTerm = true
		rf.statusCond.Signal()
	case leader:
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.biggerTermFound = true
		rf.statusCond.Signal()
	}

	var start int
	if args.PrevLogIndex < 0 {
		start = 0 // leader同步第一个LogEntry，前面没有LogEntry
	} else {
		start = rf.findEntryWithTerm(args.PrevLogIndex, args.PrevLogTerm) + 1 // 从log中的start开始合并
	}
	if start < 0 {
		reply.Success = false
		reply.LastIndexOfTerm = rf.lastIndexOfTerm(args.PrevLogTerm)
		return
	}
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}
	for i, entry := range args.Entries {
		if start+i >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if entry.Term != rf.log[start+i].Term {
			rf.log = rf.log[:start+i]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
	reply.Success = true
}

// 根据 单增索引 在log中查找对应条目，返回实际位置
// 返回值小于0，则log中没找到
// 返回值等于 -1， 时代表刚好是上一个apply的条目
// outer lock
func (rf *Raft) findEntryWithTerm(idx, term int) int {
	realIndex := (idx - rf.snapshot.lastApplied) - 1
	if realIndex >= len(rf.log) {
		return -2
	}
	if realIndex == -1 && term != rf.snapshot.lastAppliedTerm {
		realIndex = -2
	}
	return realIndex
}

func (rf *Raft) findEntry(idx int) *labrpc.LogEntry {
	realIndex := (idx - rf.snapshot.lastApplied) - 1
	if realIndex < 0 || realIndex >= len(rf.log) {
		return nil
	}
	return rf.log[realIndex]
}

// 查找某个term的末尾entry，返回其单增索引. 找不到则返回 -1
func (rf *Raft) lastIndexOfTerm(t int) int {
	idx := -1
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == t {
			idx = rf.getMonoIndex(i)
			break
		}
	}
	if idx < 0 && rf.snapshot.lastAppliedTerm == t {
		idx = rf.snapshot.lastApplied
	}
	return idx
}

func (rf *Raft) getMonoIndex(ri int) int {
	if ri < 0 {
		panic(fmt.Sprintf("getMonoIndex: ri=%v", ri))
	}
	return rf.snapshot.lastApplied + ri + 1
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := new(RequestVoteReply)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		logrus.Debugf("sendRequestVote to peer %d failed", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		logrus.Warnf("sendRequestVote: term %d in reply is less than current term %d", reply.Term, rf.currentTerm)
		return
	}
	if reply.Term > rf.currentTerm {
		logrus.Warnf("bigger term %d received, currentTerm=%d", reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.biggerTermFound = true
		rf.statusCond.Signal()
		return
	}
	if !reply.VoteGranted {
		logrus.Infof("peer %d refused to vote for candidate %d", server, rf.me)
		return
	}

	logrus.Infof("candidate %d got vote from peer %d", rf.me, server)
	rf.candidateState.voteGot[server] = true
	rf.statusCond.Signal()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == leader {
		rf.mu.Unlock()
		rf.leaderState.appendEntry <- struct{}{}
		rf.mu.Lock()
	} else {

	}

	return index, term, isLeader
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
	const (
		ElectionTimeoutLowerLimit = int64(150 * time.Millisecond)
		ElectionTimeoutRange      = int64(150 * time.Millisecond)
	)
	return ElectionTimeoutLowerLimit + rf.rand.Int63n(ElectionTimeoutRange)
}

// implement of status machine of Figure 4 on paper
func (rf *Raft) StatusMachineRun() {
	nextRole := follower
	for {
		switch nextRole {
		case follower:
			rf.turnToFollower()
			nextRole = rf.onRoleFollower()
		case candidate:
			rf.turnToCandidate()
			nextRole = rf.onRoleCandidate()
		case leader:
			rf.turnToLeader()
			nextRole = rf.onRoleLeader()
		default:
			logrus.Fatalf("can not convert to unknown role %v", rf.role)
		}
	}
}

func (rf *Raft) turnToFollower() {
	rf.mu.Lock()
	rf.role = follower
	rf.resetTimeout()
	rf.mu.Unlock()
	logrus.Debugf("term %v, peer %v become follower", rf.currentTerm, rf.me)
}

func (rf *Raft) turnToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.role = candidate
	rf.currentTerm++
	rf.resetTimeout()
	if rf.candidateState == nil {
		rf.candidateState = &CandidateState{voteGot: make([]bool, len(rf.peers))}
	}
	rf.candidateState.failedOnThisTerm = false

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	args.LastLogTerm, args.LastLogIndex = rf.lastLogTermIndex()
	for i := range rf.peers {
		if i == rf.me {
			rf.candidateState.voteGot[i] = true
			rf.statusCond.Signal()
			continue
		}
		go rf.sendRequestVote(i, args)
	}
	logrus.Debugf("term %v, peer %v become candidate", rf.currentTerm, rf.me)
}

func (rf *Raft) turnToLeader() {
	rf.mu.Lock()
	rf.role = leader
	rf.currentLeader = rf.me
	if rf.leaderState == nil {
		rf.leaderState = &LeaderState{
			nextIndex:  make([]int, len(rf.peers)),
			matchIndex: make([]int, len(rf.peers)),
			appendEntry: make(chan struct{}),
		}
	}
	for i := range rf.leaderState.nextIndex {
		rf.leaderState.nextIndex[i] = rf.getMonoIndex(len(rf.log))
	}
	rf.mu.Unlock()

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		go rf.syncLogEntriesTo(peerID)
	}
	logrus.Debugf("term %v, peer %v become leader", rf.currentTerm, rf.me)
}

func (rf *Raft) syncLogEntriesTo(pid int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()
	reply := new(AppendEntryReply)
	for {
		if rf.role != leader {
			logrus.Warnf("ready to call AppendEntry, but role is %v", rf.role)
			break
		}
		rf.mu.Unlock()
		ok := rf.peers[pid].Call("Raft.AppendEntry", args, reply)
		rf.mu.Lock()
		if !ok {
			logrus.Debugf("AppendEntry to peer %d failed", pid)
			continue
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.biggerTermFound = true
			rf.statusCond.Signal()
			break
		}
		if reply.Success {
			rf.leaderState.nextIndex[pid] = args.PrevLogIndex + len(args.Entries) + 1
			rf.leaderState.matchIndex[pid] = args.PrevLogIndex + len(args.Entries)
			rf.checkCommit()

			rf.mu.Unlock()
			<- rf.leaderState.appendEntry
			rf.mu.Lock()
			rf.leaderState.nextHeartbeat = nowUnixNano() + heartbeatIntv

			lastMatch := rf.leaderState.matchIndex[pid]
			lastMatchTerm := -1
			if lastMatch >= 0 {
				t := rf.getTermByMonoIndex(lastMatch)
				if t < 0 {
					// TODO howZ: 需要InstallSnapshot
				}
				lastMatchTerm = t
			}
			args.PrevLogIndex = lastMatch
			args.PrevLogTerm = lastMatchTerm
			args.LeaderCommit = rf.commitIndex
			args.Entries = rf.log[rf.findEntryWithTerm(lastMatch, lastMatchTerm)+1:]
			continue
		} else {
			if reply.LastIndexOfTerm >= 0 {
				ri := rf.findEntryWithTerm(reply.LastIndexOfTerm, args.Term)
				if ri < -1 {
					// TODO howZ: 可能是leader的entry 已经apply，所以找不到，需要InstallSnapshot
					logrus.Fatalf("syncLogEntriesTo: raft do not have entry index=%v, term=%v", reply.LastIndexOfTerm, args.Term)
				}
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				args.PrevLogTerm, args.PrevLogIndex = args.Term, reply.LastIndexOfTerm
				args.Entries = rf.log[ri+1:]
				continue
			} else {
				ri := rf.findEntryWithTerm(args.PrevLogIndex, args.PrevLogTerm)
				if ri <= -1 {
					// TODO howZ: 可能是leader的entry 已经apply，所以找不到，需要InstallSnapshot
					logrus.Fatalf("need snapshot to peer %d", pid)
				}
				for ri--; ri >= 0; ri-- {
					if rf.log[ri].Term != args.Term {
						break
					}
				}
				if ri == -1 {
					if rf.snapshot.lastAppliedTerm == args.PrevLogTerm {
						// TODO howZ: 可能是leader的entry 已经apply，所以找不到，需要InstallSnapshot
						logrus.Fatalf("need snapshot to peer %d", pid)
					} else {
						args.PrevLogIndex = rf.snapshot.lastApplied
						args.PrevLogTerm = rf.snapshot.lastAppliedTerm
						args.Entries = rf.log
						continue
					}
				}
				args.PrevLogIndex = rf.getMonoIndex(ri)
				args.PrevLogTerm = rf.log[ri].Term
				args.Entries = rf.log[ri+1:]
				continue
			}
		}
	}
}

// outer lock
func (rf *Raft) checkCommit() {
	sortedMatch := make([]int, len(rf.leaderState.matchIndex))
	copy(sortedMatch, rf.leaderState.matchIndex)
	sort.Ints(sortedMatch)
	i := len(sortedMatch)/2 - 1
	if i < 0 { // case: len(sortedMatch)==1
		i = 0
	}
	newCommit := sortedMatch[i]
	if newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) getTermByMonoIndex(mi int) int {
	if mi < 0 {
		panic(fmt.Sprintf("getTermByMonoIndex: mi %d", mi))
	}

	if e := rf.findEntry(mi); e != nil {
		return e.Term
	}
	if mi == rf.snapshot.lastApplied {
		return rf.snapshot.lastAppliedTerm
	}
	return -1
}

func (rf *Raft) syncWith(pid int) bool {
	_, idx := rf.lastLogTermIndex()
	return idx == rf.leaderState.matchIndex[pid]
}

func (rf *Raft) containsEntry(i, t int) bool {
	return rf.findEntryWithTerm(i, t) >= -1
}

// 根据 单增索引 和 term 找到前一个entry的 单增索引 和 term
func (rf *Raft) getPreEntry(i, t int) (int, int) {
	ri := rf.findEntryWithTerm(i, t)
	if ri <= -1 {
		return ri, -1
	}
	if ri == 0 {
		return rf.snapshot.lastApplied, rf.snapshot.lastAppliedTerm
	}
	return rf.getMonoIndex(ri - 1), rf.log[ri-1].Term
}

type InstallSnapshotArgs struct {
}

type InstallSnapshotReply struct {
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// TODO howZ:
}

func (rf *Raft) onRoleFollower() raftRole {
	var nextRole raftRole

	rf.statusCond.L.Lock()
	for {
		if nowUnixNano() > rf.elecTimeout { // 超时
			nextRole = candidate
			logrus.Debugf("follower %v timeout", rf.me)
			break
		}
		rf.statusCond.Wait()
	}
	rf.statusCond.L.Unlock()

	return nextRole
}

func (rf *Raft) onRoleCandidate() raftRole {
	var nextRole raftRole

	rf.statusCond.L.Lock()
	for {
		if rf.biggerTermFound {
			rf.biggerTermFound = false
			nextRole = follower
			logrus.Debugf("candidate %v update term, currentTerm=%v", rf.me, rf.currentTerm)
			break
		}
		if rf.candidateState.failedOnThisTerm {
			nextRole = follower
			logrus.Debugf("candidate %v failed, currentTerm=%v", rf.me, rf.currentTerm)
			break
		}
		if nowUnixNano() > rf.elecTimeout {
			nextRole = candidate
			logrus.Debugf("candidate %v timeout", rf.me)
			break
		}
		if rf.gotMajorityVote() {
			nextRole = leader
			logrus.Debugf("candidate %v got majority vote", rf.me)
			break
		}
		rf.statusCond.Wait()
	}
	rf.statusCond.L.Unlock()

	return nextRole
}

func (rf *Raft) gotMajorityVote() bool {
	count := 0
	for _, got := range rf.candidateState.voteGot {
		if got {
			count++
		}
	}
	return count > len(rf.candidateState.voteGot)/2
}

func (rf *Raft) onRoleLeader() raftRole {
	var nextRole raftRole

	rf.statusCond.L.Lock()
	for {
		if rf.biggerTermFound {
			rf.biggerTermFound = false
			nextRole = follower
			break
		}
		rf.statusCond.Wait()
	}
	rf.statusCond.L.Unlock()

	return nextRole
}

func nowUnixNano() int64 {
	return time.Now().UnixNano()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (2A, 2B, 2C).
	rf.snapshot = Snapshot{
		lastApplied:     -1,
		lastAppliedTerm: -1,
	}
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.statusCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	go rf.StatusMachineRun()
	go rf.ContinuousApplyTo(applyCh)
	go rf.ticker()

	return rf
}

func (rf *Raft) ContinuousApplyTo(applyCh chan ApplyMsg) {
	var i int
	var msg ApplyMsg
	for {
		rf.applyCond.L.Lock()
		for rf.snapshot.lastApplied == rf.commitIndex || len(rf.log) == 0 {
			rf.applyCond.Wait()
		}
		rf.applyCond.L.Unlock()

		rf.mu.Lock()
		i = rf.commitIndex - rf.snapshot.lastApplied
		rf.mu.Unlock()
		for ; i > 0; i-- {
			rf.mu.Lock()
			if len(rf.log) == 0 {
				rf.mu.Unlock()
				break
			}
			msg.CommandValid = true
			msg.Command = rf.log[0].Cmd
			msg.CommandIndex = rf.snapshot.lastApplied+1
			rf.mu.Unlock()

			applyCh <- msg

			rf.mu.Lock()
			rf.snapshot.lastApplied++
			rf.log = rf.log[1:] // fixme howz: memory leak
			rf.mu.Unlock()
		}
	}
}
