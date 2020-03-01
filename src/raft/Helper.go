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
	state *State

	node *Node
	// notify channel reader there is new entry to be replicated
	C chan int

	// entries may not be continuous(for follower)
	entries map[int64]*Entry
	services []Service
	
	db Storage
}

func NewHelper(node *Node, db Storage) *Helper{
	st := new(Helper)
	st.state = NewState()
	st.entries = make(map[int64]*Entry)
	st.services = make([]Service, 0)
	
	st.db = db
	st.C = make(chan int, 10)
	st.node = node
	st.AddService(node)

	st.loadState()
	st.loadEntries()

	// init Raft state from persistent storage
	node.Term = st.state.Term
	node.VoteFor = st.state.VoteFor

	log.Printf("Init raft node[%s]:", st.node.Id)
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
	return st.state
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
	
	st.state.Members[st.node.Id] = st.node.Addr
	for _, m := range st.node.Members {
		st.state.Members[m.Id] = m.Addr
	}
	
	st.db.Set("@State", st.state.Encode())
	st.db.Set("@CommitIndex", fmt.Sprintf("%d", st.CommitIndex))

	log.Printf("Save raft state[%s]:", st.node.Id)
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
	ent.Term = st.node.Term
	ent.Index = st.LastIndex + 1
	ent.CommitIndex = st.CommitIndex
	ent.Data = data

	st.AppendEntry(ent)
	// notify xport to send
	st.C <- 0
	return ent
}

// called when install snapshot
func (st *Helper)SaveEntry(ent *Entry){
	st.entries[ent.Index] = ent
	st.db.Set(fmt.Sprintf("log#%03d", ent.Index), ent.Encode())
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
func (st *Helper)AppendEntry(ent *Entry){
	if ent.Index <= st.CommitIndex {
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
				// TODO:
				log.Printf("lost entry#%d, svc.LastApplied: %d", idx, svc.LastApplied())
				// log.Fatalf("lost entry#%d, svc.LastApplied: %d", idx, svc.LastApplied())
				break;
			}
			svc.ApplyEntry(ent)
		}
	}
}

/* #################### Snapshot ###################### */

func (st *Helper)MakeMemSnapshot() *Snapshot {
	return NewSnapshotFromHelper(st)
}

// install 之前, Node 需要配置好 Members
func (st *Helper)InstallSnapshot(sn *Snapshot) bool {
	state := sn.State()
	st.node.Term = state.Term
	st.node.VoteFor = state.VoteFor
	
	ent := sn.LastEntry()
	st.CommitIndex = ent.Index
	st.LastTerm = ent.Term
	st.LastIndex = ent.Index
	
	st.db.CleanAll()

	// TODO: 需要实现保存的原子性, SaveEntry 和 SaveState 中间是有可能失败的
	for _, ent := range sn.Entries() {
		st.SaveEntry(ent)
	}
	st.SaveState()

	return false
}
