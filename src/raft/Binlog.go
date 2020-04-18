package raft

import (
	"log"
)

type Binlog struct {
	node *Node
	service Service

	lastEntry *Entry // 最新一条持久的日志
	entries map[int64]*Entry

	// persistent
	fsyncIndex int64 // 本地日志持久化的进度
	fsyncReadyC chan int64 // 当有日志在本地持久化时
}

func NewBinlog(node *Node) *Binlog {
	st := new(Binlog)
	st.node = node
	st.lastEntry = new(Entry)
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
	st.fsyncIndex = 0
	st.lastEntry = new(Entry)
	st.entries = make(map[int64]*Entry)
}

func (st *Binlog)LastIndex() int64 {
	return st.LastEntry().Index
}

func (st *Binlog)LastEntry() *Entry {
	return st.lastEntry
}

func (st *Binlog)GetEntry(index int64) *Entry {
	return st.entries[index]
}

func (st *Binlog)AppendEntry(type_ EntryType, data string) *Entry {
	ent := new(Entry)
	ent.Type = type_
	ent.Term = st.node.Term()
	ent.Index = st.LastIndex() + 1
	ent.Data = data

	st.WriteEntry(*ent)
	return ent
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
// 参数值拷贝
func (st *Binlog)WriteEntry(ent Entry) {
	st.entries[ent.Index] = &ent

	// TODO: 异步持久化
	// 找出连续的 entries, 持久化, 更新 lastEntry
	need_fsync := false
	for{
		var e *Entry = nil
		// first entry
		if st.LastIndex() == 0 {
			e = &ent
		} else {
			e = st.GetEntry(st.LastIndex() + 1)
		}
		if e == nil {
			break;
		}
		st.lastEntry = e
		need_fsync = true

		// TODO: save
		log.Println("[Write]", e.Encode())
	}

	if need_fsync {
		st.Fsync()
		st.fsyncIndex = st.LastIndex()
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
