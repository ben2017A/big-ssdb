package raft

import (
	"log"
)

type Store struct{
	LastIndex uint64
	LastTerm uint32
	CommitIndex uint64
	// entries may not be continuous(for follower)
	entries map[uint64]*Entry
}

func NewStore() *Store{
	ret := new(Store)
	ret.entries = make(map[uint64]*Entry)
	return ret
}

func (store *Store)GetEntry(index uint64) *Entry{
	// TODO:
	return store.entries[index]
}

func (store *Store)AppendEntry(entry Entry){
	// TODO:
	store.entries[entry.Index] = &entry
	store.FlushEntryBuffer()
}

func (store *Store)CommitEntry(commitIndex uint64){
	for idx := store.CommitIndex + 1; idx <= commitIndex ; idx ++{
		// TODO: commit idx
		// for each entry, apply
		log.Println("commit #", idx)
		store.CommitIndex = idx
	}
}

func (store *Store)FlushEntryBuffer(){
	for{
		next := store.GetEntry(store.LastIndex + 1)
		if next == nil {
			break;
		}
		next.CommitIndex = store.CommitIndex

		// TODO:
		log.Println("WALFile.append", next.Encode())
		store.LastIndex = next.Index
		store.LastTerm = next.Term
	}
}
