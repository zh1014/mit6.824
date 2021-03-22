package raft

import (
	"bytes"
	"mit6.824/labgob"
)

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
	e.Encode(rf.voteGot)
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
	err = d.Decode(&rf.voteGot)
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

func (rf *Raft) persistIfDirty() {
	if rf.isDirty() {
		rf.persist()
	}
}
