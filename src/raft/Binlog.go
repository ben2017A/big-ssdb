package raft

import (
)

type Binlog struct{
	node *Node
	Service Service

	// volatile
	LastTerm int32
	LastIndex int64
	entries map[int64]*Entry

	// persistent
	FsyncIndex int64 // 本地日志持久化的进度
	FsyncNotify chan *Entry // 当有日志在本地持久化时
}

func NewBinlog(node *Node) *Binlog {
	st := new(Binlog)
	st.node = node
	st.entries = make(map[int64]*Entry)
	st.FsyncNotify = make(chan *Entry, 10)
	return st
}

func (st *Binlog)Close(){
}

func (st *Binlog)GetEntry(index int64) *Entry{
	return st.entries[index]
}

func (st *Binlog)AppendEntry(type_ EntryType, data string) *Entry{
	ent := new(Entry)
	ent.Type = type_
	ent.Term = st.node.Term()
	ent.Index = st.LastIndex + 1
	// ent.Commit = st.node.CommitIndex
	ent.Data = data

	st.WriteEntry(*ent)
	return ent
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
// 参数值拷贝
func (st *Binlog)WriteEntry(ent Entry){
	st.entries[ent.Index] = &ent

	// 找出连续的 entries, 更新 LastTerm 和 LastIndex,
	for{
		ent := st.GetEntry(st.LastIndex + 1)
		if ent == nil {
			break;
		}
		st.LastTerm = ent.Term
		st.LastIndex = ent.Index

		st.FsyncNotify <- ent
	}
}
