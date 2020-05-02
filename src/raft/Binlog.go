package raft

import (
	"log"
	"util"
	"store"
)

type Binlog struct {
	node *Node
	service Service

	lastEntry *Entry // 最新一条持久的日志
	entries map[int64]*Entry

	wal *store.WalFile
}

func OpenBinlog(dir string) *Binlog {
	fn := dir + "/binlog.wal"
	wal := store.OpenWalFile(fn)
	if wal == nil {
		log.Printf("Failed to open wal file: %s", fn)
		return nil
	}

	st := new(Binlog)
	st.wal = wal
	st.lastEntry = new(Entry)
	st.entries = make(map[int64]*Entry)

	// TODO: 优化点
	st.wal.SeekTo(0)
	for st.wal.Next() {
		r := wal.Item()
		e := DecodeEntry(r)
		if e == nil {
			log.Fatalf("Failed to decode entry: %s", r)
		}
		st.entries[e.Index] = e
		st.lastEntry = e
	}

	return st
}

func (st *Binlog)Close() {
	st.wal.Close()
}

func (st *Binlog)Fsync() {
	if err := st.wal.Fsync(); err != nil {
		log.Fatal(err)
	}
}

func (st *Binlog)LastIndex() int64 {
	return st.LastEntry().Index
}

// 最新一条持久化的日志
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
	ent.Commit = st.node.CommitIndex()
	ent.Data = data

	st.WriteEntry(ent)
	return ent
}

// 如果存在空洞, 仅仅先缓存 entry, 不更新 lastTerm 和 lastIndex
func (st *Binlog)WriteEntry(ent *Entry) {
	// TODO: 优化点, 不能全部放内存
	st.entries[ent.Index] = ent

	// TODO: 异步持久化
	// 找出连续的 entries, 持久化, 更新 lastEntry
	need_fsync := false
	for{
		var e *Entry = nil
		// first entry
		if st.LastIndex() == 0 {
			e = ent
		} else {
			e = st.GetEntry(st.LastIndex() + 1)
		}
		if e == nil {
			break;
		}

		data := e.Encode()
		log.Println("[Append]", util.StringEscape(data))
		st.wal.Append(data)
		st.lastEntry = e
		need_fsync = true
	}

	if need_fsync {
		st.Fsync()
		st.node.append_c <- true
	}
}

func (st *Binlog)Clean() {
	st.lastEntry = new(Entry)
	st.entries = make(map[int64]*Entry)
	if err := st.wal.Clean(); err != nil {
		log.Fatal(err)
	}
}

func (st *Binlog)RecoverFromSnapshot(sn *Snapshot) {
	st.Clean()
	st.WriteEntry(sn.lastEntry)
	st.Fsync()
}
