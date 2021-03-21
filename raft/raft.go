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
	"bytes"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"mit6.824/labgob"
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

var roleString = map[raftRole]string{
	follower:  "follower",
	candidate: "candidate",
	leader:    "leader",
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
	applyCond  *sync.Cond

	mu             sync.Mutex // protect follow fields
	currentTerm    int
	role           raftRole
	votedFor       int
	elecTimeout    int64
	log            []*labrpc.LogEntry
	matchIndex     int
	commitIndex    int
	lastApplied    int
	snapshotMeta   Snapshot
	candidateState CandidateState
	leaderState    *LeaderState
	dirty          bool
}

type Snapshot struct {
	lastIncluded    int
	lastIncludeTerm int
}

type LeaderState struct {
	nextIndex     []int
	matchIndex    []int
	newEntryCond  *sync.Cond
	lastHeartbeat []int64
}

type CandidateState struct {
	voteGot []bool
}

func (l *LeaderState) needHeartbeat(peerID int) bool {
	return nowUnixNano()-l.lastHeartbeat[peerID] > int64(heartbeatIntv)
}

// outer lock
func (rf *Raft) resetTimeout() {
	rf.elecTimeout = nowUnixNano() + rf.randElectionTimeout()
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
		if nowUnixNano() > rf.elecTimeout && rf.role != leader {
			rf.initiateNewElection()
			rf.persist()
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.role)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotMeta.lastIncluded)
	e.Encode(rf.snapshotMeta.lastIncludeTerm)
	e.Encode(rf.candidateState.voteGot)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.wipeDirty()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err := d.Decode(&rf.currentTerm)
	checkErr(err)
	err = d.Decode(&rf.role)
	checkErr(err)
	err = d.Decode(&rf.votedFor)
	checkErr(err)
	err = d.Decode(&rf.log)
	checkErr(err)
	err = d.Decode(&rf.snapshotMeta.lastIncluded)
	checkErr(err)
	err = d.Decode(&rf.snapshotMeta.lastIncludeTerm)
	checkErr(err)
	err = d.Decode(&rf.candidateState.voteGot)
	checkErr(err)
}

func (rf *Raft) markDirty() {
	rf.dirty = true
}

func (rf *Raft) wipeDirty() {
	rf.dirty = false
}

func (rf *Raft) isDirty() bool {
	return rf.dirty
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	CreateTs     int64
}

// only vote for candidate whose log is at least as up-to-date as this log
func (args *RequestVoteArgs) AsUpToDateAs(lastLogTerm, lastLogIdx int) bool {
	return args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx)
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
	lastLogTerm, lastLogIdx := rf.lastLogTermIndex()
	defer func() {
		logrus.Debugf("%s exec RequestVote, lastLog=[Index%d,Term%d], args=%+v, reply=%+v",
			rf.desc(), lastLogIdx, lastLogTerm, args, reply)
		rf.persistIfDirty()
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}
	if rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		return
	}
	if !rf.voted() && args.AsUpToDateAs(lastLogTerm, lastLogIdx) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.markDirty()
	}
}

func (rf *Raft) voted() bool {
	return rf.votedFor >= 0
}

// outer lock
func (rf *Raft) lastLogTermIndex() (term, idx int) {
	lenLog := len(rf.log)
	if lenLog == 0 {
		term, idx = rf.snapshotMeta.lastIncludeTerm, rf.snapshotMeta.lastIncluded
		return
	}

	lastLog := rf.log[lenLog-1]
	term = lastLog.Term
	idx = rf.snapshotMeta.lastIncluded + lenLog
	return
}

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	CreateTs     int64
	Entries      []*labrpc.LogEntry
}

func (args *AppendEntryArgs) String() string {
	return fmt.Sprintf("{Term=%v,LeaderID=%v,PrevLog[Idx%v Term%v],LeaderCommit=%v,CreateTs=%d, [%d]Entries=%s}",
		args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.CreateTs, len(args.Entries), args.StringEntries())
}

func (args *AppendEntryArgs) StringEntries() string {
	const display = 3
	if len(args.Entries) <= display {
		return entriesString(args.PrevLogIndex+1, args.Entries)
	}
	start := len(args.Entries) - display
	startMonoIdx := args.PrevLogIndex + start + 1
	return "..." + entriesString(startMonoIdx, args.Entries[start:])
}

type AppendEntryReply struct {
	Term            int
	Success         bool
	LastIndexOfTerm int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()
	logrus.Debugf("%s receive AppendEntry, args=%s", rf.desc(), args)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.role == follower {
		rf.resetTimeout()
	}
	if (args.Term == rf.currentTerm && rf.role != follower) || args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}
	defer rf.updateCommitIdx(args.LeaderCommit) // 收到leader（term不小于自己）的消息，就可能更新 commitIndex

	var realIdxStart int // 从log中的start开始合并
	if args.PrevLogIndex > 0 {
		realIdxStart = rf.findEntryWithTerm(args.PrevLogIndex, args.PrevLogTerm) + 1
	}
	if realIdxStart < 0 {
		reply.Success = false
		reply.LastIndexOfTerm = rf.lastIndexOfTerm(args.PrevLogTerm)
		//logrus.Debugf("%s exec AppendEntry, log=%v", rf.desc(), rf.entriesString())
		return
	}
	rf.updateMatchIndex(args) // 只要PrevLog匹配成功，就可能更新matchIndex
	rf.appendEntries(realIdxStart, args.Entries)
	if len(rf.log) > snapshotTriggerCond {
		rf.snapshot()
	}
	reply.Success = true
}

// 只能提交leader已经提交，且肯定与leader匹配的部分LogEntry
func (rf *Raft) updateCommitIdx(leaderCommit int) {
	commit := min(leaderCommit, rf.matchIndex)
	if commit > rf.commitIndex {
		logrus.Debugf("%s update commitIndex %v, log=%s", rf.desc(), commit, rf.StringLog())
		rf.commitIndex = commit
		rf.applyCond.Signal()
		rf.markDirty()
	}
}

func (rf *Raft) updateMatchIndex(args *AppendEntryArgs) {
	match := args.PrevLogIndex + len(args.Entries)
	if match > rf.matchIndex {
		rf.matchIndex = match
	}
}

func (rf *Raft) StringLog() string {
	const display = 3
	if len(rf.log) <= display {
		return entriesString(rf.snapshotMeta.lastIncluded+1, rf.log)
	}
	start := len(rf.log) - display
	startMonoIdx := rf.snapshotMeta.lastIncluded + start + 1
	return "..." + entriesString(startMonoIdx, rf.log[start:])
}

func (rf *Raft) appendEntry(command interface{}) {
	rf.log = append(rf.log, &labrpc.LogEntry{
		Term: rf.currentTerm,
		Cmd:  command,
	})
	rf.markDirty()
	//rf.printLog()
}

func (rf *Raft) appendEntries(start int, entries []*labrpc.LogEntry) {
	if len(entries) == 0 {
		return
	}
	for i, entry := range entries {
		if start+i >= len(rf.log) {
			rf.log = append(rf.log, entries[i:]...)
			break
		}
		if entry.Term != rf.log[start+i].Term {
			rf.log = rf.log[:start+i]
			rf.log = append(rf.log, entries[i:]...)
			break
		}
	}
	rf.markDirty()
	//rf.printLog()
}

func (rf *Raft) persistIfDirty() {
	if rf.isDirty() {
		rf.persist()
	}
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

// 根据 单增索引 在log中查找对应条目，返回实际位置
// 返回值小于0，则log中没找到
// 返回值等于 -1， 时代表刚好是上一个apply的条目
// outer lock
func (rf *Raft) findEntryWithTerm(idx, term int) int {
	realIndex := (idx - rf.snapshotMeta.lastIncluded) - 1
	if realIndex >= len(rf.log) {
		return -2
	}
	if realIndex == -1 && term != rf.snapshotMeta.lastIncludeTerm {
		realIndex = -2
	}
	if realIndex >= 0 && rf.log[realIndex].Term != term {
		realIndex = -2
	}
	return realIndex
}

func (rf *Raft) findEntry(monoIdx int) *labrpc.LogEntry {
	realIndex := rf.getRealIndex(monoIdx)
	if realIndex < 0 || realIndex >= len(rf.log) {
		return nil
	}
	return rf.log[realIndex]
}

func (rf *Raft) getRealIndex(monoIdx int) int {
	return (monoIdx - rf.snapshotMeta.lastIncluded) - 1
}

// 查找某个term的末尾entry，返回其单增索引. 找不到则返回 -1
func (rf *Raft) lastIndexOfTerm(t int) int {
	idx := 0
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == t {
			idx = rf.getMonoIndex(i)
			break
		}
	}
	if idx == 0 && rf.snapshotMeta.lastIncludeTerm == t {
		idx = rf.snapshotMeta.lastIncluded
	}
	return idx
}

func (rf *Raft) getMonoIndex(ri int) int {
	if ri < 0 {
		panic(fmt.Sprintf("getMonoIndex: ri=%v", ri))
	}
	return rf.snapshotMeta.lastIncluded + ri + 1
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
func (rf *Raft) requestVoteFrom(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		CreateTs:    nowUnixNano(),
	}
	args.LastLogTerm, args.LastLogIndex = rf.lastLogTermIndex()
	logrus.Debugf("%s requestVoteFrom peer%d, args=%+v", rf.desc(), peerID, args)
	reply := new(RequestVoteReply)
	ok := rf.RequestVoteRPC(peerID, args, reply)
	if rf.killed() {
		return
	}
	if !ok {
		logrus.Debugf("%s requestVoteFrom peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
		return
	}
	logrus.Debugf("%s requestVoteFrom peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
	if reply.Term > rf.currentTerm {
		rf.changeToFollower(reply.Term)
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.role != candidate {
		// 此轮选举已经结束
		return
	}
	if !reply.VoteGranted {
		return
	}
	rf.candidateState.voteGot[peerID] = true
	if rf.gotMajorityVote() {
		rf.changeToLeader()
	}
	rf.markDirty()
}

func (rf *Raft) AppendEntryRPC(peerID int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rf.mu.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.AppendEntry", args, reply)
	logrus.Debugf("AppendEntryRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.mu.Lock()
	return ok
}

func (rf *Raft) RequestVoteRPC(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Unlock()
	start := time.Now()
	ok := rf.peers[peerID].Call("Raft.RequestVote", args, reply)
	logrus.Debugf("RequestVoteRPC CreateTs %d, cost %v", args.CreateTs, time.Now().Sub(start))
	rf.mu.Lock()
	return ok
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
	term, index = rf.lastLogTermIndex()
	rf.leaderState.matchIndex[rf.me] = index
	rf.leaderState.newEntryCond.Broadcast()
	if len(rf.log) > snapshotTriggerCond {
		rf.snapshot()
	}
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
func (rf *Raft) changeToFollower(term int) {
	from := rf.desc()
	rf.role = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.matchIndex = 0
	rf.resetTimeout()
	rf.markDirty()
	logrus.Infof("%s -> %s", from, rf.desc())
}

func (rf *Raft) initiateNewElection() {
	rf.markDirty()
	from := rf.desc()
	rf.role = candidate
	rf.currentTerm++
	logrus.Infof("%s -> %s", from, rf.desc())
	rf.resetTimeout()
	rf.candidateState.voteGot = make([]bool, len(rf.peers))

	// vote for self
	rf.votedFor = rf.me
	rf.candidateState.voteGot[rf.me] = true
	if rf.gotMajorityVote() {
		rf.changeToLeader()
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVoteFrom(i)
	}
}

func (rf *Raft) changeToLeader() {
	logrus.Infof("%s -> leader", rf.desc())
	rf.role = leader
	rf.leaderState = &LeaderState{
		nextIndex:     make([]int, len(rf.peers)),
		matchIndex:    make([]int, len(rf.peers)),
		lastHeartbeat: make([]int64, len(rf.peers)),
		newEntryCond:  sync.NewCond(&rf.mu),
	}
	for i := range rf.leaderState.nextIndex {
		rf.leaderState.nextIndex[i] = rf.getMonoIndex(len(rf.log))
	}

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		go rf.syncLogEntriesTo(peerID)
	}
	rf.markDirty()
}

func (rf *Raft) syncLogEntriesTo(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logrus.Debugf("%s syncLogEntriesTo %d start....", rf.desc(), peerID)

	// the first heartbeat announces the newly elected leader
	args := &AppendEntryArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()

	for {
		rf.leaderState.lastHeartbeat[peerID] = nowUnixNano()
		args.LeaderCommit = rf.commitIndex
		args.CreateTs = nowUnixNano()
		reply := &AppendEntryReply{}
		logrus.Debugf("%s AppendEntry to peer%d, args=%s", rf.desc(), peerID, args)
		ok := rf.AppendEntryRPC(peerID, args, reply)
		// peer in bigger term found
		if reply.Term > rf.currentTerm {
			logrus.Debugf("%s syncLogEntriesTo [Term%d|Peer%d] end", rf.desc(), reply.Term, peerID)
			rf.changeToFollower(reply.Term)
			rf.persist()
			break
		}
		if err := rf.checkStopSyncLog(args.Term); err != nil {
			logrus.Debugf("%s syncLogEntriesTo peer%d end, after AppendEntryRPC: %v", rf.desc(), peerID, err)
			break
		}
		if !ok { // retry
			logrus.Debugf("%s AppendEntry to peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
			continue
		}

		logrus.Debugf("%s AppendEntry to peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
		var realIndexMatch int // 下一个可能match的位置
		if reply.Success {
			rf.leaderState.nextIndex[peerID] = args.PrevLogIndex + len(args.Entries) + 1
			rf.leaderState.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
			rf.checkCommit()
			rf.persist()

			rf.waitNewLogEntry(peerID)
			if err := rf.checkStopSyncLog(args.Term); err != nil {
				logrus.Debugf("%s syncLogEntriesTo peer%d end, after waitNewLogEntry: %v", rf.desc(), peerID, err)
				break
			}

			realIndexMatch = rf.getRealIndex(rf.leaderState.matchIndex[peerID])
		} else {
			realIndexMatch = rf.findMatchQuickly(args, reply)
		}
		if realIndexMatch <= realIndexInvalid {
			rf.sendSnapshotTo(peerID)
		} else if realIndexMatch == realIndexLastApplied {
			args.PrevLogIndex = rf.snapshotMeta.lastIncluded
			args.PrevLogTerm = rf.snapshotMeta.lastIncludeTerm
		} else {
			args.PrevLogIndex = rf.getMonoIndex(realIndexMatch)
			args.PrevLogTerm = rf.log[realIndexMatch].Term
		}
		args.Entries = rf.log[realIndexMatch+1:]
	}
}

func (rf *Raft) sendHeartbeatTo(peerID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistIfDirty()

	now := nowUnixNano()
	rf.leaderState.lastHeartbeat[peerID] = now
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		CreateTs:     now,
	}
	realIndexMatch := rf.getRealIndex(rf.leaderState.matchIndex[peerID])
	if realIndexMatch <= realIndexInvalid {
		rf.sendSnapshotTo(peerID)
		args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()
	} else if realIndexMatch == realIndexLastApplied {
		args.PrevLogIndex = rf.snapshotMeta.lastIncluded
		args.PrevLogTerm = rf.snapshotMeta.lastIncludeTerm
	} else {
		args.PrevLogIndex = rf.getMonoIndex(realIndexMatch)
		args.PrevLogTerm = rf.log[realIndexMatch].Term
	}
	reply := &AppendEntryReply{}
	logrus.Debugf("%s sendHeartbeatTo to peer%d, args=%s", rf.desc(), peerID, args)
	ok := rf.AppendEntryRPC(peerID, args, reply)
	if !ok { // retry
		logrus.Debugf("%s sendHeartbeatTo to peer%d RPC failed, CreateTs=%d", rf.desc(), peerID, args.CreateTs)
		return
	}
	if reply.Term > rf.currentTerm {
		logrus.Debugf("%s sendHeartbeatTo [Term%d|Peer%d], newer term found", rf.desc(), reply.Term, peerID)
		rf.changeToFollower(reply.Term)
		return
	}
	logrus.Debugf("%s sendHeartbeatTo to peer%d returned, CreateTs=%d, reply=%+v", rf.desc(), peerID, args.CreateTs, reply)
}

func (rf *Raft) checkStopSyncLog(syncTerm int) error {
	if rf.killed() {
		return errors.New("killed")
	}
	if rf.role != leader {
		return errors.New("not leader")
	}
	if syncTerm < rf.currentTerm {
		return errors.New("stale sync goroutine")
	}
	return nil
}

func (rf *Raft) findMatchQuickly(args *AppendEntryArgs, reply *AppendEntryReply) int {
	var realIndex int
	if reply.LastIndexOfTerm > 0 {
		// 快速获取match位置. 直接定位到对方的args.PrevLogTerm的最后一条的LogEntry
		realIndex = rf.findEntryWithTerm(reply.LastIndexOfTerm, args.PrevLogTerm)
	} else {
		// 获取可能match的位置. 获取最后一条term小于args.PrevLogTerm的LogEntry
		realIndex = len(rf.log) - 1
		for ; realIndex >= 0; realIndex-- {
			if rf.log[realIndex].Term < args.PrevLogTerm {
				break
			}
		}
		if realIndex == -1 && rf.snapshotMeta.lastIncludeTerm == args.PrevLogTerm {
			realIndex--
		}
	}
	return realIndex
}

func (rf *Raft) sendSnapshotTo(peerID int) {
	// TODO howz
	logrus.Debugf("need snapshot to peer %d", peerID)
	//rf.printLog()
	panic("snapshot")
}

func (rf *Raft) waitNewLogEntry(peerID int) {
	for rf.syncWith(peerID) {
		//if nowUnixNano()-rf.leaderState.lastHeartbeat[peerID] > int64(heartbeatIntv) {
		//	logrus.Debugf("%s prepare to send heartbeat to %d", rf.desc(), peerID)
		//	break
		//}
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
	if medianMatch > rf.commitIndex && rf.findEntry(medianMatch).Term == rf.currentTerm {
		rf.commitIndex = medianMatch
		rf.applyCond.Signal()
		logrus.Infof("%s checkCommit, update commitIndex %d, log=%s", rf.desc(), rf.commitIndex, rf.StringLog())
	}
}

// 查找MonoIndex对应Entry的Term
// 没找到Entry则返回0
func (rf *Raft) getTermByMonoIndex(mi int) int {
	if mi <= 0 {
		return 0
	}

	if e := rf.findEntry(mi); e != nil {
		return e.Term
	}
	if mi == rf.snapshotMeta.lastIncluded {
		return rf.snapshotMeta.lastIncludeTerm
	}
	return 0
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
		return rf.snapshotMeta.lastIncluded, rf.snapshotMeta.lastIncludeTerm
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

func (rf *Raft) gotMajorityVote() bool {
	count := 0
	for _, got := range rf.candidateState.voteGot {
		if got {
			count++
		}
	}
	return count > len(rf.candidateState.voteGot)/2
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
	rf.applyCond = sync.NewCond(&rf.mu)

	if rawData := persister.ReadRaftState(); len(rawData) > 0 {
		rf.readPersist(rawData)
	}
	if rf.role == follower {
		rf.resetTimeout()
	} else if rf.role == leader {
		rf.changeToLeader()
	}

	go rf.applyDamon(applyCh)
	go rf.ticker()
	return rf
}

func (rf *Raft) applyDamon(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	for {
		for rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			break
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.getRealIndex(rf.lastApplied)].Cmd,
				CommandIndex: rf.lastApplied,
			}
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
	ri := rf.getRealIndex(rf.lastApplied)
	if ri < 0 {
		logrus.Debugf("%s snapshot failed, len=%d, ri=%d, lastApplied=%v", rf.desc(), len(rf.log), ri, rf.lastApplied)
		return
	}
	// TODO howZ
	//rf.persister.SaveStateAndSnapshot()
}

func (rf *Raft) Debug(content string) {
	logrus.Debugf("%s %s", rf.desc(), content)
}
