package raft

import (
	"log"
	"store"

	"myutil"
)

type Storage struct{
	LastIndex uint64
	LastTerm uint32
	CommitIndex uint64

	node *Node

	dir string

	state *State
	stateWAL *store.WALFile

	// entries may not be continuous(for follower)
	entries map[uint64]*Entry
	entryWAL *store.WALFile
	subscribers []Subscriber
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.dir = dir

	ret.state = new(State)
	ret.stateWAL = store.OpenWALFile(dir + "/raft/state.wal")

	ret.entries = make(map[uint64]*Entry)
	ret.entryWAL = store.OpenWALFile(dir + "/raft/entry.wal")
	ret.subscribers = make([]Subscriber, 0)

	ret.loadState()
	ret.loadEntries()

	log.Println("open storage at:", dir)
	return ret
}

func (store *Storage)Close(){
	if store.stateWAL != nil {
		store.stateWAL.Close()
	}
	if store.entryWAL != nil {
		store.entryWAL.Close()
	}
}

func (store *Storage)SetNode(node *Node){
	store.node = node

	s := store.state
	node.Id = s.Id
	node.Addr = s.Addr
	node.Term = s.Term
	node.VoteFor = s.VoteFor

	for nodeId, nodeAddr := range s.Members {
		node.ConnectMember(nodeId, nodeAddr)
	}

	store.AddSubscriber(node)
	store.applyEntries()
}

/* #################### State ###################### */

func (store *Storage)loadState(){
	last := store.stateWAL.ReadLast()
	if last != "" {
		store.state.Decode(last)
	}
}

func (store *Storage)SaveState(){
	store.state = NewStateFromNode(store.node)
	store.stateWAL.Append(store.state.Encode())
	log.Println("[WAL]", store.state.Encode())
}

/* #################### Entry ###################### */

func (store *Storage)loadEntries(){
	wal := store.entryWAL
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
				store.entries[ent.Index] = ent
				store.LastIndex = ent.Index
				store.LastTerm = ent.Term
			}
			if ent.CommitIndex > 0 {
				store.CommitIndex = ent.CommitIndex
			}
		}
	}
}

func (store *Storage)AddSubscriber(sub Subscriber){
	store.subscribers = append(store.subscribers, sub)
}

func (store *Storage)GetEntry(index uint64) *Entry{
	// TODO:
	return store.entries[index]
}

func (store *Storage)AppendEntry(ent Entry){
	if ent.Index < store.CommitIndex {
		return
	}

	// TODO:
	store.entries[ent.Index] = &ent

	for{
		ent := store.GetEntry(store.LastIndex + 1)
		if ent == nil {
			break;
		}
		ent.CommitIndex = store.CommitIndex

		store.entryWAL.Append(ent.Encode())
		log.Println("[WAL]", ent.Encode())

		store.LastIndex = ent.Index
		store.LastTerm = ent.Term
	}
}

func (store *Storage)CommitEntry(commitIndex uint64){
	commitIndex = myutil.MinU64(commitIndex, store.LastIndex)
	if commitIndex <= store.CommitIndex {
		return
	}

	ent := NewCommitEntry(commitIndex)
	store.entryWAL.Append(ent.Encode())
	log.Println("[WAL]", ent.Encode())

	store.CommitIndex = commitIndex
	store.applyEntries()
}

func (store *Storage)applyEntries(){
	for _, sub := range store.subscribers {
		for sub.LastApplied() < store.CommitIndex {
			ent := store.GetEntry(sub.LastApplied() + 1)
			if ent == nil {
				log.Fatal("lost entry#", sub.LastApplied() + 1)
				break;
			}
			sub.ApplyEntry(ent)
		}
	}
}
