package raft

import (
	"log"
	"encoding/json"
	"util"
)

// Raft's snapshot, not service's
type Snapshot struct {
	state *State
	// 新节点需要至少存储两条日志(如果 commitIndex > 2), 否则收到 Heartbeat 时校验 prevEntry 会失败
	entries []*Entry
}

func newSnapshot() *Snapshot {
	sn := new(Snapshot)
	sn.state = NewState()
	sn.entries = make([]*Entry, 0)
	return sn
}

func NewSnapshotFromStorage(store *Storage) *Snapshot {
	sn := newSnapshot()
	sn.state.CopyFrom(store.State())
	sn.entries = make([]*Entry, 0)
	
	const NUM int64 = 2
	start := util.MaxInt64(1, store.CommitIndex - NUM + 1)
	for idx := start; idx <= store.CommitIndex; idx ++ {
		ent := store.GetEntry(idx)
		if ent == nil {
			log.Fatal("lost entry#", idx)
			return nil
		}
		ent.CommitIndex = ent.Index
		sn.entries = append(sn.entries, ent)
	}

	return sn
}

func NewSnapshotFromString(data string) *Snapshot {
	sn := newSnapshot()
	if !sn.Decode(data) {
		return nil
	}
	return sn
}

func (sn *Snapshot)LastTerm() int32 {
	if len(sn.entries) == 0 {
		return 0
	}
	return sn.lastEntry().Term
}

func (sn *Snapshot)LastIndex() int64 {
	if len(sn.entries) == 0 {
		return 0
	}
	return sn.lastEntry().Index
}

func (sn *Snapshot)lastEntry() *Entry {
	return sn.entries[len(sn.entries)-1]
}

func (sn *Snapshot)State() *State {
	return sn.state
}

// Lastest committed entries
func (sn *Snapshot)Entries() []*Entry {
	return sn.entries
}

func (sn *Snapshot)Encode() string {
	var arr []string

	arr = append(arr, sn.state.Encode())
	for _, ent := range sn.entries {
		arr = append(arr, ent.Encode())
	}
	
	bs, _ := json.Marshal(arr)
	data := string(bs)
	return data
}

func (sn *Snapshot)Decode(data string) bool {
	var arr []string
	err := json.Unmarshal([]byte(data), &arr)
	if err != nil {
		log.Println("json_decode error:", err, "data:", data)
		return false
	}
	if len(arr) == 0 {
		log.Println("bad data:", data)
		return false
	}
	if sn.state.Decode(arr[0]) != true {
		log.Println("decode state error:", data)
		return false
	}

	for _, s := range arr[1:] {
		var ent Entry
		if ent.Decode(s) == false {
			log.Println("decode entry error:", data)
			return false
		}
		sn.entries = append(sn.entries, &ent)
	}

	return true
}
