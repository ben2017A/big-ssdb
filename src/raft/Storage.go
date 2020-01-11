package raft

import (
	"log"
	"store"
)

type Storage struct{
	LastIndex uint64
	LastTerm uint32
	CommitIndex uint64
	// entries may not be continuous(for follower)
	entries map[uint64]*Entry

	dir string
	wal *store.WALFile

	subscribers []Subscriber
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.entries = make(map[uint64]*Entry)
	ret.dir = dir
	ret.wal = store.OpenWALFile(dir + "/entry.wal")
	ret.subscribers = make([]Subscriber, 0)
	log.Println("open store", dir)
	return ret
}

func (store *Storage)Close(){
	if store.wal != nil {
		store.wal.Close()
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

		store.wal.Append(ent.Encode())
		log.Println("WALFile.append:", ent.Encode())

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
	store.wal.Append(ent.Encode())
	log.Println("WALFile.append:", ent.Encode())
	store.CommitIndex = commitIndex

	for _, sub := range store.subscribers {
		if sub.LastApplied() < store.CommitIndex {
			ent := store.GetEntry(sub.LastApplied() + 1)
			sub.ApplyEntry(ent)
		}
	}
}
