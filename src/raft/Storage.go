package raft

import (
	"fmt"
	"log"
	"strings"
	"store"
	"myutil"
)

type Storage struct{
	LastIndex int64
	LastTerm int32
	CommitIndex int64

	node *Node

	state State
	// entries may not be continuous(for follower)
	entries map[int64]*Entry
	services []Service
	
	dir string
	db *store.KVStore
}

func OpenStorage(dir string) *Storage{
	ret := new(Storage)
	ret.entries = make(map[int64]*Entry)
	ret.services = make([]Service, 0)
	
	ret.dir = dir
	ret.db = store.OpenKVStore(dir)

	ret.loadState()
	ret.loadEntries()

	log.Println("open storage at:", dir)
	return ret
}

func (st *Storage)Close(){
	if st.db != nil {
		st.db.Close()
	}
}

func (st *Storage)State() State{
	return st.state
}

func (st *Storage)SetNode(node *Node){
	st.node = node

	s := st.state
	if s.Id != "" {
		node.Term = s.Term
		node.VoteFor = s.VoteFor
	}

	st.AddService(node)
	st.applyEntries()
}

func (st *Storage)AddService(svc Service){
	st.services = append(st.services, svc)
}

/* #################### State ###################### */

func (st *Storage)SaveState(){
	st.state.LoadFromNode(st.node)
	// st.db.Set("State", st.state.Encode())
	log.Println("[RAFT] State", st.state.Encode())
}

func (st *Storage)loadState(){
	last := st.db.Get("State")
	st.state.Decode(last)
}

/* #################### Entry ###################### */

func (st *Storage)loadEntries(){
	for k, v := range st.db.All() {
		if !strings.HasPrefix(k, "log#") {
			continue
		}
		
		ent := DecodeEntry(v)
		if ent == nil {
			log.Println("bad entry format:", v)
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
func (st *Storage)AddEntry(ent Entry){
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

		st.db.Set(fmt.Sprintf("log#%d", ent.Index), ent.Encode())
		log.Println("[RAFT] Log", ent.Encode())

		st.LastIndex = ent.Index
		st.LastTerm = ent.Term
	}
}

// 如果存在空洞, 不会跳过空洞 commit
func (st *Storage)CommitEntry(commitIndex int64){
	commitIndex = myutil.MinInt64(commitIndex, st.LastIndex)
	if commitIndex <= st.CommitIndex {
		return
	}

	st.db.Set("CommitIndex", fmt.Sprintf("%d", commitIndex))
	log.Println("[RAFT] CommitIndex", commitIndex)

	st.CommitIndex = commitIndex
	st.applyEntries()
}

func (st *Storage)applyEntries(){
	idx := st.CommitIndex // Node 可能会改变 CommitIndex
	for _, svc := range st.services {
		for svc.LastApplied() < idx {
			ent := st.GetEntry(svc.LastApplied() + 1)
			if ent == nil {
				log.Fatal("lost entry#", svc.LastApplied() + 1)
				break;
			}
			svc.ApplyEntry(ent)
		}
	}
}
