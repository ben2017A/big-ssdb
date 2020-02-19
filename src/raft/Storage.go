package raft

import (
	// "os"
	"log"
	"store"

	// "myutil"
)

type Storage struct{
	LastIndex int64
	LastTerm int32
	CommitIndex int64

	node *Node

	dir string

	state *State
	stateWAL *store.WALFile

	// entries may not be continuous(for follower)
	entries map[int64]*Entry
	entryWAL *store.WALFile
	subscribers []Subscriber
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.dir = dir

	ret.state = new(State)
	ret.stateWAL = store.OpenWALFile(dir + "/state.wal")

	ret.entries = make(map[int64]*Entry)
	ret.entryWAL = store.OpenWALFile(dir + "/entry.wal")
	ret.subscribers = make([]Subscriber, 0)

	ret.loadState()
	ret.loadEntries()

	log.Println("open storage at:", dir)
	return ret
}

func (st *Storage)Close(){
	if st.stateWAL != nil {
		st.stateWAL.Close()
	}
	if st.entryWAL != nil {
		st.entryWAL.Close()
	}
}

func (st *Storage)SetNode(node *Node){
	st.node = node

	s := st.state
	node.Id = s.Id
	node.Addr = s.Addr
	node.Term = s.Term
	node.VoteFor = s.VoteFor

	for nodeId, nodeAddr := range s.Members {
		node.ConnectMember(nodeId, nodeAddr)
	}

	st.AddSubscriber(node)
	st.applyEntries()
}

func (st *Storage)AddSubscriber(sub Subscriber){
	st.subscribers = append(st.subscribers, sub)
}

/* #################### State ###################### */

func (st *Storage)SaveState(){
	st.state = NewStateFromNode(st.node)
	st.stateWAL.Append(st.state.Encode())
	log.Println("[WAL]", st.state.Encode())
}

func (st *Storage)loadState(){
	last := st.stateWAL.ReadLast()
	if last != "" {
		st.state.Decode(last)
	}
	// st.compactStateWAL()
}

// func (st *Storage)compactStateWAL(){
// 	wal := store.OpenWALFile(st.dir + "/state.wal.tmp")
// 	wal.Append(st.state.Encode())

// 	// TODO
// 	st.stateWAL.Close()
// 	os.Remove(st.stateWAL.Filename)
// 	os.Rename(wal.Filename, st.stateWAL.Filename)

// 	st.stateWAL = wal
// 	log.Println("[state WAL] compacted")
// }

/* #################### Entry ###################### */

func (st *Storage)loadEntries(){
	wal := st.entryWAL
	wal.SeekTo(0)
	for {
		r := wal.Read()
		if r == "" {
			break
		}
		ent := DecodeEntry(r)
		if ent == nil {
			log.Println("bad entry format:", r)
		} else {
			if ent.Index > 0 && ent.Term > 0 {
				st.entries[ent.Index] = ent
				st.LastIndex = ent.Index
				st.LastTerm = ent.Term
			}
			if ent.CommitIndex > 0 {
				st.CommitIndex = ent.CommitIndex
			}
		}
	}
}

func (st *Storage)GetEntry(index int64) *Entry{
	// TODO:
	return st.entries[index]
}

func (st *Storage)AppendEntry(ent Entry){
	if ent.Index < st.CommitIndex {
		return
	}

	// TODO:
	st.entries[ent.Index] = &ent

	// 更新 LastTer 和 LastIndex, 忽略空洞
	for{
		ent := st.GetEntry(st.LastIndex + 1)
		if ent == nil {
			break;
		}
		ent.CommitIndex = st.CommitIndex

		st.entryWAL.Append(ent.Encode())
		log.Println("[WAL]", ent.Encode())

		st.LastIndex = ent.Index
		st.LastTerm = ent.Term
	}
}

func (st *Storage)CommitEntry(commitIndex int64){
	if commitIndex <= st.CommitIndex {
		return
	}

	ent := NewCommitEntry(commitIndex)
	st.entryWAL.Append(ent.Encode())
	log.Println("[WAL]", ent.Encode())

	st.CommitIndex = commitIndex
	st.applyEntries()
}

func (st *Storage)applyEntries(){
	for _, sub := range st.subscribers {
		for sub.LastApplied() < st.CommitIndex {
			ent := st.GetEntry(sub.LastApplied() + 1)
			if ent == nil {
				log.Fatal("lost entry#", sub.LastApplied() + 1)
				break;
			}
			sub.ApplyEntry(ent)
		}
	}
}
