package raft

import (
	"bytes"
	"mit6.824/labgob"
)

//
// restore previously persisted state.
//
func (rf *Raft) ReadPersist(data []byte) {
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	checkErr(decoder.Decode(&rf.currentTerm))
	checkErr(decoder.Decode(&rf.role))
	checkErr(decoder.Decode(&rf.votedFor))
	checkErr(decoder.Decode(&rf.voteGot))
	rf.Log.decode(decoder)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) marshal() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	checkErr(encoder.Encode(rf.currentTerm))
	checkErr(encoder.Encode(rf.role))
	checkErr(encoder.Encode(rf.votedFor))
	checkErr(encoder.Encode(rf.voteGot))
	rf.Log.encode(encoder)
	return buffer.Bytes()
}

func (rf *Raft) PersistState() {
	state := rf.marshal()
	rf.persister.SaveRaftState(state)
	rf.WipeDirty()
}

func (rf *Raft) PersistStateAndSnapshot(snapshot []byte) {
	state := rf.marshal()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	rf.WipeDirty()
}

func (rf *Raft) PersistStateIfDirty() {
	if rf.IsDirty() {
		rf.PersistState()
	}
}

type Dirty struct {
	marked bool
}

func (d *Dirty) MarkDirty() {
	d.marked = true
}

func (d *Dirty) WipeDirty() {
	d.marked = false
}

func (d *Dirty) IsDirty() bool {
	return d.marked
}
