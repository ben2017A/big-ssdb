package raft

import (
	"log"
	"sync"
	"util"
	"store"
)

type Binlog struct {
	sync.Mutex

	node *Node
	service Service

	stop_c  chan bool // stop signal
	write_c chan bool // write signal
	accept_c chan bool
	commit_c chan bool

	entries map[int64]*Entry
	lastEntry *Entry // 最新一条持久的日志
	appendIndex int64
	// acceptIndex int64
	commitIndex int64

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
	st.init()
	
	st.stop_c   = make(chan bool)
	st.write_c  = make(chan bool, 1/*TODO*/)
	st.accept_c = make(chan bool, 1/*TODO*/) // log been persisted
	st.commit_c = make(chan bool, 1/*TODO*/) // log been committed

	st.startWriter()

	return st
}

func (st *Binlog)init() {
	st.lastEntry = new(Entry)
	st.entries = make(map[int64]*Entry)
	st.appendIndex = 0
	st.commitIndex = 0

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
		st.commitIndex = util.MaxInt64(st.commitIndex, e.Commit)
	}
	st.appendIndex = st.lastEntry.Index

	// validate persitent state
	if st.CommitIndex() > st.AcceptIndex() {
		log.Fatalf("Data corruption, commit: %d > accept: %d", st.CommitIndex(), st.AcceptIndex())
	}
}

func (st *Binlog)reset() {
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
			run := <- st.write_c
			for run && len(st.write_c) > 0 {
				run = <- st.write_c
			}
			if !run {
				break;
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

func (st *Binlog)AppendIndex() int64 {
	return st.appendIndex
}

func (st *Binlog)AcceptIndex() int64 {
	return st.LastEntry().Index
}

func (st *Binlog)CommitIndex() int64 {
	return st.commitIndex
}

// 最新一条持久化的日志
func (st *Binlog)LastEntry() *Entry {
	return st.lastEntry
}

// how many logs(unstable + stable) uncommitted
func (st *Binlog)UncommittedSize() int {
	return (int)(st.appendIndex - st.commitIndex)
}

func (st *Binlog)GetEntry(index int64) *Entry {
	st.Lock()
	defer st.Unlock()
	return st.entries[index]
}

func (st *Binlog)Append(term int32, typo EntryType, data string) *Entry {
	ent := new(Entry)
	ent.Term = term
	ent.Type = typo
	ent.Data = data
	
	st.Lock()
	st.appendIndex += 1
	ent.Index = st.appendIndex
	st.Unlock()

	st.Write(ent)
	return ent
}

// 将 ent 放入写缓冲即返回, 会处理空洞
func (st *Binlog)Write(ent *Entry) {
	drop := false

	st.Lock()
	{
		old := st.entries[ent.Index]
		if old != nil {
			if old.Term > ent.Term {
				// after assigning index to a proposing entry, but before saving it,
				// we just received an entry with same index from new leader
				log.Printf("drop entry %d:%d, old entry has newer term %d", ent.Term, ent.Index, old.Term)
				drop = true
			} else if old.Term < ent.Term {
				// TODO: how?
				log.Println("TODO: delete conflict entry, and entries that follow")
			} else {
				log.Printf("drop duplicated entry %d:%d", ent.Term, ent.Index)
				drop = true
			}
		}

		if !drop {
			// TODO: 优化点, 不能全部放内存
			st.entries[ent.Index] = ent
		}
	}
	st.Unlock()

	if !drop {
		st.write_c <- true
	}
}

func (st *Binlog)Fsync() {
	has_new := false

	st.Lock()
	{
		// 找出连续的 entries, 持久化, 更新 lastEntry
		for {
			ent := st.entries[st.lastEntry.Index + 1]
			if ent == nil {
				break
			}
			st.lastEntry = ent
			has_new = true

			ent.Commit = util.MinInt64(ent.Index, st.commitIndex)

			data := ent.Encode()
			st.wal.Append(data)
			log.Println("[Append]", util.StringEscape(data))
		}
		if has_new {
			err := st.wal.Fsync()
			if err != nil {
				log.Fatal(err)
			}
			// when is follower
			if st.appendIndex < st.lastEntry.Index {
				st.appendIndex = st.lastEntry.Index
			}
		}
	}
	st.Unlock()

	if has_new {
		st.accept_c <- true
	}
}

func (st *Binlog)Commit(commitIndex int64) {
	commitIndex = util.MinInt64(commitIndex, st.AcceptIndex())
	if commitIndex <= st.commitIndex {
		return
	}

	log.Printf("%s commit %d => %d", st.node.Id(), st.commitIndex, commitIndex)
	st.commitIndex = commitIndex
	st.commit_c <- true

	// synchronously apply to Config
	for index := st.node.conf.applied + 1; index <= st.commitIndex; index ++ {
		ent := st.GetEntry(index)
		if ent == nil {
			log.Fatalf("Lost entry@%d", index)
		}
		st.node.conf.ApplyEntry(ent)
	}

	// TODO: asynchronously apply to Service
}

func (st *Binlog)Clean() {
	st.Lock()
	defer st.Unlock()

	st.stopWriter()
	if err := st.wal.Clean(); err != nil {
		log.Fatal(err)
	}

	st.init()
	st.startWriter()
}

func (st *Binlog)RecoverFromSnapshot(sn *Snapshot) {
	st.Clean()
	st.appendIndex = sn.LastIndex()
	st.commitIndex = sn.LastIndex()
	// lastEntry will be updated inside Fsync
	st.lastEntry.Index = sn.LastIndex() - 1
	st.Write(sn.lastEntry)
	// may be called by writer, but force fsync as well
	st.Fsync()
}
