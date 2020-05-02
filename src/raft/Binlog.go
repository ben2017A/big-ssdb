package raft

import (
	"log"
	"sync"
	"util"
	"store"
)

type Binlog struct {
	node *Node
	service Service

	nextIndex int64
	lastEntry *Entry // 最新一条持久的日志
	entries map[int64]*Entry
	write_c chan bool // write signal
	stop_c  chan bool // stop signal

	wal *store.WalFile

	mux sync.Mutex
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
	st.reset()

	st.stop_c  = make(chan bool)
	st.write_c = make(chan bool, 1/*TODO*/)

	st.startWriter()

	return st
}

func (st *Binlog)reset() {
	st.lastEntry = new(Entry)
	st.entries = make(map[int64]*Entry)

	// TODO: 优化点
	st.wal.SeekTo(0)
	for st.wal.Next() {
		r := st.wal.Item()
		e := DecodeEntry(r)
		if e == nil {
			log.Fatalf("Failed to decode entry: %s", r)
		}
		st.entries[e.Index] = e
		st.lastEntry = e
	}

	st.nextIndex = st.lastEntry.Index + 1
}

func (st *Binlog)Close() {
	st.stopWriter()
	st.wal.Close()
	close(st.stop_c)
	close(st.write_c)
}

func (st *Binlog)startWriter() {
	go func() {
		// log.Println("start")
		for {
			if b := <- st.write_c; b == false {
				break
			}
			st.Fsync()
		}
		// log.Println("quit")
		st.stop_c <- true
	}()
}

func (st *Binlog)stopWriter() {
	st.write_c <- false
	<- st.stop_c
}

func (st *Binlog)LastIndex() int64 {
	return st.LastEntry().Index
}

// 最新一条持久化的日志
func (st *Binlog)LastEntry() *Entry {
	return st.lastEntry
}

func (st *Binlog)GetEntry(index int64) *Entry {
	st.mux.Lock()
	defer st.mux.Unlock()
	return st.entries[index]
}

// 需要 node.mux.Lock(), 是否不应该放在这里? 应该移到 Node 里?
func (st *Binlog)NewEntry(type_ EntryType, data string) *Entry {
	ent := new(Entry)
	ent.Type = type_
	ent.Data = data

	ent.Term = st.node.Term()
	ent.Commit = st.node.CommitIndex()
	ent.Index = st.nextIndex

	st.nextIndex += 1
	return ent
}

// 将 ent 放入写缓冲即返回, 会处理空洞
func (st *Binlog)AppendEntry(ent *Entry) {
	st.mux.Lock()
	// first entry
	if st.lastEntry.Index == 0 {
		st.lastEntry.Index = ent.Index - 1
	}
	// TODO: 优化点, 不能全部放内存
	st.entries[ent.Index] = ent
	st.mux.Unlock()

	st.write_c <- true
}

func (st *Binlog)Fsync() {
	st.mux.Lock()
	defer st.mux.Unlock()

	// 找出连续的 entries, 持久化, 更新 lastEntry
	last := st.lastEntry
	for{
		e := st.entries[last.Index + 1]
		if e == nil {
			break
		}
		last = e

		data := last.Encode()
		st.wal.Append(data)
		log.Println("[Append]", util.StringEscape(data))
	}

	if last != st.lastEntry {
		err := st.wal.Fsync()
		if err != nil {
			log.Fatal(err)
		}
		st.lastEntry = last
		st.node.append_c <- true
	}
}

func (st *Binlog)Clean() {
	st.mux.Lock()
	defer st.mux.Unlock()

	st.stopWriter()
	if err := st.wal.Clean(); err != nil {
		log.Fatal(err)
	}

	st.reset()
	st.startWriter()
}

func (st *Binlog)RecoverFromSnapshot(sn *Snapshot) {
	st.Clean()
	st.AppendEntry(sn.lastEntry)
	st.Fsync()
}
