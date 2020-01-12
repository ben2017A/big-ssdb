package raft

import (
	"log"
	"store"
)

type Storage struct{
	LastIndex uint64
	LastTerm uint32
	CommitIndex uint64

	dir string

	node *Node
	stateWAL *store.WALFile

	// entries may not be continuous(for follower)
	entries map[uint64]*Entry
	entryWAL *store.WALFile
	subscribers []Subscriber
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.dir = dir

	ret.stateWAL = store.OpenWALFile(dir + "/state.wal")

	ret.entries = make(map[uint64]*Entry)
	ret.entryWAL = store.OpenWALFile(dir + "/entry.wal")

	ret.subscribers = make([]Subscriber, 0)
	log.Println("open store", dir)
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

/* #################### State ###################### */

func (store *Storage)loadState(){

}

func (store *Storage)SetNode(node *Node){
	store.node = node
}

func (store *Storage)SaveState(){
	s := NewStateFromNode(store.node)
	store.stateWAL.Append(s.Encode())
	log.Println("stateWAL.append:", s.Encode())
}

/* #################### Entry ###################### */

func (store *Storage)loadEntries(){
	
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
		log.Println("entryWAL.append:", ent.Encode())

		store.LastIndex = ent.Index
		store.LastTerm = ent.Term
	}
}

func (store *Storage)CommitEntry(commitIndex uint64){
	if commitIndex <= store.CommitIndex {
		return
	}
	if commitIndex > store.LastIndex {
		commitIndex = store.LastIndex
	}

	ent := NewCommitEntry(commitIndex)
	store.entryWAL.Append(ent.Encode())
	log.Println("entryWAL.append:", ent.Encode())

	store.CommitIndex = commitIndex

	for _, sub := range store.subscribers {
		if sub.LastApplied() < store.CommitIndex {
			ent := store.GetEntry(sub.LastApplied() + 1)
			sub.ApplyEntry(ent)
		}
	}
}
