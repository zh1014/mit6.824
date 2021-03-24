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
	e.Encode(rf.Log.slice)
	e.Encode(rf.Log.lastIncluded)
	e.Encode(rf.Log.lastIncludeTerm)
	e.Encode(rf.voteGot)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.WipeDirty()
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
	err = d.Decode(&rf.Log.slice)
	checkErr(err)
	err = d.Decode(&rf.Log.lastIncluded)
	checkErr(err)
	err = d.Decode(&rf.Log.lastIncludeTerm)
	checkErr(err)
	err = d.Decode(&rf.voteGot)
	checkErr(err)
}

func (rf *Raft) persistIfDirty() {
	if rf.IsDirty() || rf.Log.IsDirty() {
		rf.persist()
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
