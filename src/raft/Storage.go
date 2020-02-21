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
	services []Service
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.dir = dir

	ret.state = new(State)
	ret.stateWAL = store.OpenWALFile(dir + "/state.wal")

	ret.entries = make(map[int64]*Entry)
	ret.entryWAL = store.OpenWALFile(dir + "/entry.wal")
	ret.services = make([]Service, 0)

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

	st.AddService(node)
	st.applyEntries()
}

func (st *Storage)AddService(svc Servide){
	st.services = append(st.services, svc)
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
}

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

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
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

// 如果存在空洞, 不会跳过空洞 commit
func (st *Storage)CommitEntry(commitIndex int64){
	if commitIndex <= st.CommitIndex {
		return
	}
	commitIndex := myutil.MinInt64(commitIndex, st.LastIndex)

	ent := NewCommitEntry(commitIndex)
	st.entryWAL.Append(ent.Encode())
	log.Println("[WAL]", ent.Encode())

	st.CommitIndex = commitIndex
	st.applyEntries()
}

func (st *Storage)applyEntries(){
	for _, svc := range st.services {
		for svc.LastApplied() < st.CommitIndex {
			ent := st.GetEntry(svc.LastApplied() + 1)
			if ent == nil {
				log.Fatal("lost entry#", svc.LastApplied() + 1)
				break;
			}
			svc.ApplyEntry(ent)
		}
	}
}
