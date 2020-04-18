package raft

import (
	"log"
)

type Binlog struct {
	node *Node
	service Service

	// volatile
	lastTerm int32
	lastIndex int64
	entries map[int64]*Entry

	// persistent
	fsyncIndex int64 // 本地日志持久化的进度
	fsyncReadyC chan int64 // 当有日志在本地持久化时
}

func NewBinlog(node *Node) *Binlog {
	st := new(Binlog)
	st.node = node
	st.entries = make(map[int64]*Entry)
	st.fsyncReadyC = make(chan int64, 10)
	return st
}

// func OpenBinlog(node *Node, db_path string) *Binlog {
// }

func (st *Binlog)Close() {
}

func (st *Binlog)Fsync() {
}

func (st *Binlog)CleanAll() {
	st.lastTerm = 0
	st.lastIndex = 0
	st.fsyncIndex = 0
	st.entries = make(map[int64]*Entry)
}

func (st *Binlog)GetEntry(index int64) *Entry {
	return st.entries[index]
}

func (st *Binlog)AppendEntry(type_ EntryType, data string) *Entry {
	ent := new(Entry)
	ent.Type = type_
	ent.Term = st.node.Term()
	ent.Index = st.lastIndex + 1
	// ent.Commit = st.node.CommitIndex
	ent.Data = data

	st.WriteEntry(*ent)
	return ent
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
// 参数值拷贝
func (st *Binlog)WriteEntry(ent Entry) {
	log.Println("[Write]", ent.Encode())
	st.entries[ent.Index] = &ent

	// first entry
	if st.lastIndex == 0 {
		st.lastIndex = ent.Index - 1
	}

	// TODO: 异步持久化
	// 找出连续的 entries, 更新 lastTerm 和 lastIndex,
	for{
		ent := st.GetEntry(st.lastIndex + 1)
		if ent == nil {
			break;
		}
		st.lastTerm = ent.Term
		st.lastIndex = ent.Index

		st.fsyncIndex = st.lastIndex
		st.fsyncReadyC <- st.fsyncIndex
	}
}

func (st *Binlog)ResetFromSnapshot(sn *Snapshot) {
	st.CleanAll()
	for _, ent := range sn.entries {
		st.WriteEntry(ent)
	}
	st.Fsync()
}
