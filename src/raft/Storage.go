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
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.entries = make(map[uint64]*Entry)
	ret.dir = dir
	ret.wal = store.OpenWALFile(dir + "/entry.wal")
	log.Println("open store", dir)
	return ret
}

func (store *Storage)Close(){
	if store.wal != nil {
		store.wal.Close()
	}
}

func (store *Storage)GetEntry(index uint64) *Entry{
	// TODO:
	return store.entries[index]
}

func (store *Storage)AppendEntry(entry Entry){
	if entry.Index < store.CommitIndex {
		return
	}

	// TODO:
	store.entries[entry.Index] = &entry

	for{
		next := store.GetEntry(store.LastIndex + 1)
		if next == nil {
			break;
		}
		next.CommitIndex = store.CommitIndex

		log.Println("WALFile.append", next.Encode())
		store.wal.Append(next.Encode())

		store.LastIndex = next.Index
		store.LastTerm = next.Term
	}
}

func (store *Storage)CommitEntry(commitIndex uint64){
	if commitIndex <= store.CommitIndex {
		return
	}
	for idx := store.CommitIndex + 1; idx <= commitIndex ; idx ++{
		// for each entry, apply to state machine

		// TODO: commit idx
		log.Println("commit #", idx)
		store.CommitIndex = idx
	}
}
