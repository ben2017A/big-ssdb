package raft

import (
	"fmt"
	"log"
	"math"
	"strings"
	"util"
)

type Storage struct{
	// Discovered from log entries
	FirstIndex int64
	LastTerm int32
	LastIndex int64
	// Saved in db @CommitIndex
	// All committed entries are immediately applied to Raft it self,
	// but may asynchronously be applied to Service
	CommitIndex int64
	state *State

	node *Node
	// notify Raft there is new entry to be replicated
	C chan int

	// entries may not be continuous(for follower)
	entries map[int64]*Entry
	Service Service
	
	db Db
}

func NewStorage(db Db) *Storage {
	st := new(Storage)
	st.state = NewState()
	st.entries = make(map[int64]*Entry)
	
	st.db = db
	st.C = make(chan int, 10)

	st.loadState()
	st.loadEntries()

	return st
}

func (st *Storage)Close(){
	st.SaveState()
	if st.db != nil {
		st.db.Close()
	}
}

func (st *Storage)SetNode(node *Node) {
	st.node = node
}

/* #################### State ###################### */

func (st *Storage)State() *State{
	return st.state
}

func (st *Storage)loadState() {
	data := st.db.Get("@State")
	st.state.Decode(data)
	if st.state.Members == nil {
		st.state.Members = make(map[string]string)
	}
	st.CommitIndex = util.Atoi64(st.db.Get("@CommitIndex"))
}

func (st *Storage)SaveState(){
	st.state.Term = st.node.Term
	st.state.VoteFor = st.node.VoteFor
	st.state.Members = make(map[string]string)
	
	st.state.Members[st.node.Id] = st.node.Addr
	for _, m := range st.node.Members {
		st.state.Members[m.Id] = m.Addr
	}
	
	st.db.Set("@State", st.state.Encode())
	st.db.Set("@CommitIndex", fmt.Sprintf("%d", st.CommitIndex))

	log.Printf("save raft state[%s]:", st.node.Id)
	log.Println("    CommitIndex:", st.CommitIndex, "LastTerm:", st.LastTerm, "LastIndex:", st.LastIndex)
	log.Println("    State:", st.state.Encode())
}

/* #################### Entry ###################### */

func (st *Storage)loadEntries(){
	st.FirstIndex = math.MaxInt64
	for k, v := range st.db.All() {
		if !strings.HasPrefix(k, "log#") {
			continue
		}
		
		ent := DecodeEntry(v)
		if ent == nil {
			log.Println("bad entry format:", v)
		} else {
			st.entries[ent.Index] = ent
			st.FirstIndex  = util.MinInt64(st.FirstIndex, ent.Index)
			st.LastTerm    = util.MaxInt32(st.LastTerm, ent.Term)
			st.LastIndex   = util.MaxInt64(st.LastIndex, ent.Index)
		}
	}
}

func (st *Storage)GetEntry(index int64) *Entry{
	return st.entries[index]
}

// return a copy of appended entry
func (st *Storage)AddNewEntry(type_ EntryType, data string) *Entry{
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
func (st *Storage)SaveEntry(ent *Entry){
	st.db.Set(fmt.Sprintf("log#%03d", ent.Index), ent.Encode())
	st.entries[ent.Index] = ent
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
func (st *Storage)AppendEntry(ent *Entry){
	if ent.Index <= st.CommitIndex {
		log.Println("ent.Index", ent.Index, "<", "commitIndex", st.CommitIndex)
		return
	}

	st.entries[ent.Index] = ent
	st.FirstIndex = util.MinInt64(st.FirstIndex, ent.Index)

	// 更新 LastTer 和 LastIndex, 忽略空洞
	for{
		ent := st.GetEntry(st.LastIndex + 1)
		if ent == nil {
			break;
		}

		st.db.Set(fmt.Sprintf("log#%03d", ent.Index), ent.Encode())
		log.Println("[RAFT] append Log", ent.Encode())

		st.LastTerm = ent.Term
		st.LastIndex = ent.Index
	}
}

// 如果存在空洞, 不会跳过空洞 commit
func (st *Storage)CommitEntry(commitIndex int64){
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

func (st *Storage)ApplyEntries(){
	for idx := st.node.LastApplied() + 1; idx <= st.CommitIndex; idx ++ {
		ent := st.GetEntry(idx)
		if ent == nil {
			log.Fatalf("entry#%d not found", idx)
		}
		st.node.ApplyEntry(ent)
	}
	if st.Service != nil {
		for idx := st.Service.LastApplied() + 1; idx <= st.CommitIndex; idx ++ {
			ent := st.GetEntry(idx)
			if ent == nil {
				log.Printf("lost entry#%d, svc.LastApplied: %d, notify Service to install snapshot",
						idx, st.Service.LastApplied())
				st.Service.InstallSnapshot()
				break
			}
			st.Service.ApplyEntry(ent)
		}
	}
}

/* #################### Snapshot ###################### */

func (st *Storage)CreateSnapshot() *Snapshot {
	return NewSnapshotFromStorage(st)
}

// install 之前, Node 需要配置好 Members, 因为 SaveState() 会从 node.Members 获取
func (st *Storage)InstallSnapshot(sn *Snapshot) bool {
	st.db.CleanAll()

	st.node.Term    = sn.State().Term
	st.node.VoteFor = ""
	st.LastTerm     = sn.LastTerm()
	st.LastIndex    = sn.LastIndex()
	st.CommitIndex  = sn.LastIndex()

	// TODO: 需要实现保存的原子性, SaveEntry 和 SaveState 中间是有可能失败的
	st.SaveState()
	for _, ent := range sn.Entries() {
		st.SaveEntry(ent)
	}

	return true
}

func (st *Storage)CleanAll() bool {
	st.CommitIndex = 0
	st.LastTerm = 0
	st.LastIndex = 0
	st.db.CleanAll()
	st.SaveState()
	return true
}
