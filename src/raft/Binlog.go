package raft

import (
	"sync"
	log "glog"
	"util"
	"store"
)

// max qps = WriteBufferSize * fsync_qps
const WriteBufferSize = 100

type Binlog struct {
	sync.Mutex

	node *Node

	stop_c   chan bool
	done_c   chan bool
	append_c chan bool // write signal
	accept_c chan bool

	entries map[int64]*Entry
	lastEntry *Entry // last accepted entry
	appendIndex int64 // last append(by local) index

	wal *store.WalFile
}

func OpenBinlog(dir string) *Binlog {
	fn := dir + "/binlog.wal"
	wal := store.OpenWalFile(fn)
	if wal == nil {
		log.Error("Failed to open wal file: %s", fn)
		return nil
	}

	st := new(Binlog)
	st.wal = wal
	
	st.stop_c   = make(chan bool)
	st.done_c   = make(chan bool)
	st.append_c = make(chan bool, WriteBufferSize) // signal logs to be persisted
	st.accept_c = make(chan bool) // signal logs has been persisted

	st.init()
	st.startWriter()

	return st
}

func (st *Binlog)init() {
	st.lastEntry = new(Entry)
	st.entries = make(map[int64]*Entry)

	// TODO: 优化点
	st.wal.SeekTo(0)
	for st.wal.Next() {
		r := st.wal.Item()
		e := DecodeEntry(r)
		if e == nil {
			log.Fatal("Failed to decode entry: %s", r)
		}
		st.entries[e.Index] = e
		st.lastEntry = e
	}
	st.appendIndex = st.lastEntry.Index
}

func (st *Binlog)Close() {
	st.stopWriter()
	st.wal.Close()
	close(st.append_c)
	close(st.accept_c)
}

func (st *Binlog)startWriter() {
	go func() {
		// log.Info("start")
		defer func(){
			st.done_c <- true
			// log.Info("quit")
		}()
		for {
			select {
			case <- st.stop_c:
				return
			case <- st.append_c:
				st.fsync()
			}
		}
	}()
}

func (st *Binlog)stopWriter() {
	st.stop_c <- true
	<- st.done_c
}

func (st *Binlog)acceptIndex() int64 {
	return st.lastEntry.Index
}

// 最新一条持久化的日志
func (st *Binlog)LastEntry() *Entry {
	return st.lastEntry
}

func (st *Binlog)GetEntry(index int64) *Entry {
	st.Lock()
	defer st.Unlock()
	// TODO: 优化点, 不能全部 entry 都放内存
	return st.entries[index]
}

// 申请一个连续的 index, 然后将 entry 写入缓冲区
func (st *Binlog)Append(term int32, typo EntryType, data string) *Entry {
	ent := new(Entry)
	ent.Term = term
	ent.Type = typo
	ent.Data = data
	
	st.Lock()
	{
		st.appendIndex += 1
		ent.Index = st.appendIndex
	}
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
				log.Info("drop entry %d:%d, old entry has newer term %d", ent.Term, ent.Index, old.Term)
				drop = true
			} else if old.Term < ent.Term {
				// TODO: how?
				log.Info("TODO: delete conflicted entry, and entries that follow")
			} else {
				log.Info("drop duplicated entry %d:%d", ent.Term, ent.Index)
				drop = true
			}
		}

		if !drop {
			// TODO: 优化点, 不能全部放内存
			st.entries[ent.Index] = ent
		}
	}
	st.Unlock()

	// append_c consumer needs holding lock, so produce append_c outside
	if !drop {
		st.append_c <- true
	}
}

func (st *Binlog)fsync() {
	new_count := 0

	st.Lock()
	{
		// 找出连续的 entries, 持久化, 更新 lastEntry
		for {
			ent := st.entries[st.lastEntry.Index + 1]
			if ent == nil {
				break
			}
			st.lastEntry = ent
			new_count += 1

			data := ent.Encode()
			st.wal.Append(data)
			log.Debug("[Accept] %s", util.StringEscape(data))
		}
		// when is follower
		if st.appendIndex < st.lastEntry.Index {
			st.appendIndex = st.lastEntry.Index
		}
	}
	st.Unlock()

	// accept_c consumer need holding lock, so produce accept_c outside
	if new_count > 0 {
		err := st.wal.Fsync()
		if err != nil {
			log.Fatalln(err)
		}
		st.accept_c <- true
		log.Debug("batch accepted %d entry(s)", new_count)
	}
}

func (st *Binlog)Clean() {
	st.Lock()
	defer st.Unlock()

	st.stopWriter()
	if err := st.wal.Clean(); err != nil {
		log.Fatalln(err)
	}

	st.init()
	st.startWriter()
}

func (st *Binlog)RecoverFromSnapshot(sn *Snapshot) {
	st.Clean()
	st.appendIndex = sn.LastIndex()
	// lastEntry will be updated inside fsync()
	st.lastEntry.Index = sn.LastIndex() - 1
	st.Write(sn.lastEntry)
	// may have been called by writer, but force fsync as well
	st.fsync()
}
