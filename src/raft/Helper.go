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
	state State

	node *Node
	// notify channel reader there is new entry to be replicated
	C chan int

	// entries may not be continuous(for follower)
	entries map[int64]*Entry
	services []Service
	
	// must store at least one log entry after first term
	db Storage
}

func NewHelper(node *Node, db Storage) *Helper{
	st := new(Helper)
	st.entries = make(map[int64]*Entry)
	st.services = make([]Service, 0)
	
	st.db = db
	st.C = make(chan int, 10)
	st.node = node
	st.AddService(node)

	st.loadState()
	st.loadEntries()

	log.Println("    CommitIndex:", st.CommitIndex, "LastTerm:", st.LastTerm, "LastIndex:", st.LastIndex)
	log.Println("    State:", st.state.Encode())

	return st
}

func (st *Helper)Close(){
	st.SaveState()
	if st.db != nil {
		st.db.Close()
	}
}

func (st *Helper)AddService(svc Service){
	st.services = append(st.services, svc)
}

/* #################### State ###################### */

func (st *Helper)State() *State{
	return &st.state
}

func (st *Helper)loadState() {
	data := st.db.Get("@State")
	st.state.Decode(data)
	if st.state.Members == nil {
		st.state.Members = make(map[string]string)
	}
	st.CommitIndex = util.Atoi64(st.db.Get("@CommitIndex"))
}

func (st *Helper)SaveState(){
	st.state.Term = st.node.Term
	st.state.VoteFor = st.node.VoteFor
	st.state.Members = make(map[string]string)
	for _, m := range st.node.Members {
		st.state.Members[m.Id] = m.Addr
	}
	
	st.db.Set("@State", st.state.Encode())
	st.db.Set("@CommitIndex", fmt.Sprintf("%d", st.CommitIndex))

	log.Println("    CommitIndex:", st.CommitIndex, "LastTerm:", st.LastTerm, "LastIndex:", st.LastIndex)
	log.Println("    State:", st.state.Encode())
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
