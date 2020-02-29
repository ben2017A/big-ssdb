package raft

import (
	"fmt"
	"log"
	"strings"
	"util"
)

type Helper struct{
	// Discovered from log entries
	LastTerm int32
	LastIndex int64
	// Discovered from log entries, also in db @CommitIndex
	CommitIndex int64

	node *Node
	// notify channel reader there is new entry to be replicated
	C chan int

	// entries may not be continuous(for follower)
	entries map[int64]*Entry
	services []Service
	
	db Storage
}

func NewHelper(node *Node, db Storage) *Helper{
	ret := new(Helper)
	ret.entries = make(map[int64]*Entry)
	ret.services = make([]Service, 0)
	
	ret.db = db
	ret.C = make(chan int, 10)
	ret.node = node
	ret.AddService(node)

	ret.CommitIndex = util.Atoi64(ret.db.Get("@CommitIndex"))
	ret.loadEntries()

	log.Println("CommitIndex:", ret.CommitIndex, "LastTerm:", ret.LastTerm, "LastIndex:", ret.LastIndex)
	log.Println("State:", ret.LoadState().Encode())

	return ret
}

func (st *Helper)Close(){
	if st.db != nil {
		st.db.Close()
	}
}

func (st *Helper)AddService(svc Service){
	st.services = append(st.services, svc)
}

/* #################### State ###################### */

func (st *Helper)LoadState() *State{
	var s State
	data := st.db.Get("@State")
	s.Decode(data)
	return &s
}

func (st *Helper)SaveState(){
	s := new(State)
	s.Term = st.node.Term
	s.VoteFor = st.node.VoteFor
	s.Members = make(map[string]string)
	
	for _, m := range st.node.Members {
		s.Members[m.Id] = m.Addr
	}
	
	st.db.Set("@State", s.Encode())
	log.Println("[RAFT] State", s.Encode())
}

/* #################### Entry ###################### */

func (st *Helper)loadEntries(){
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
			}
			st.LastTerm = util.MaxInt32(st.LastTerm, ent.Term)
			st.LastIndex = util.MaxInt64(st.LastIndex, ent.Index)
		}
	}
}

func (st *Helper)GetEntry(index int64) *Entry{
	return st.entries[index]
}

// return a copy of appended entry
func (st *Helper)AddNewEntry(type_, data string) *Entry{
	ent := new(Entry)
	ent.Type = type_
	ent.Index = st.LastIndex + 1
	ent.Term = st.node.Term
	ent.Data = data
	
	st.AppendEntry(ent)
	st.C <- 0
	return ent
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
func (st *Helper)AppendEntry(ent *Entry){
	if ent.Index < st.CommitIndex {
		log.Println(ent.Index, "<", st.CommitIndex)
		return
	}

	st.entries[ent.Index] = ent

	// 更新 LastTer 和 LastIndex, 忽略空洞
	for{
		ent := st.GetEntry(st.LastIndex + 1)
		if ent == nil {
			break;
		}
		ent.CommitIndex = st.CommitIndex

		st.db.Set(fmt.Sprintf("log#%03d", ent.Index), ent.Encode())
		log.Println("[RAFT] Log", ent.Encode())

		st.LastTerm = ent.Term
		st.LastIndex = ent.Index
	}
}

// 如果存在空洞, 不会跳过空洞 commit
func (st *Helper)CommitEntry(commitIndex int64){
	commitIndex = util.MinInt64(commitIndex, st.LastIndex)
	if commitIndex <= st.CommitIndex {
		// log.Printf("msg.CommitIndex: %d <= CommitIndex: %d\n", commitIndex, st.CommitIndex)
		return
	}

	st.CommitIndex = commitIndex
	st.ApplyEntries()

	// save after applied
	st.db.Set("@CommitIndex", fmt.Sprintf("%d", st.CommitIndex))
	log.Printf("[RAFT] CommitIndex: %d", st.CommitIndex)
}

func (st *Helper)ApplyEntries(){
	for _, svc := range st.services {
		for idx := svc.LastApplied() + 1; idx <= st.CommitIndex; idx ++ {
			ent := st.GetEntry(idx)
			if ent == nil {
				log.Fatal("lost entry#", idx)
				break;
			}
			svc.ApplyEntry(ent)
		}
	}
}
