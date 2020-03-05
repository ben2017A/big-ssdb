package ssdb

import (
	"log"
	"os"
	"store"
	"util"
)

type RedoManager struct{
	wal *store.WalFile
	path string
	commitIndex int64
	checkIndex int64
}

func OpenRedoManager(path string) *RedoManager {
	ret := new(RedoManager)
	ret.path = path
	
	if !ret.recover() {
		return nil
	}

	return ret
}

func (rd *RedoManager)Close() {
	if rd.checkIndex != rd.commitIndex {
		rd.Check()
	}
	rd.wal.Close()
}

func (rd *RedoManager)recover() bool {
	rd.wal = store.OpenWalFile(rd.path)
	if rd.wal == nil {
		log.Println("could not open", rd.path)
		return false
	}
	
	rd.commitIndex = 0
	
	var begin int64 = 0
	rd.wal.SeekTo(0)
	for rd.wal.Next() {
		r := rd.wal.Item()
		var ent RedoEntry
		if ent.Decode(r) == false {
			log.Fatalf("invalid entry: %s", r)
			return false
		}

		switch ent.Type {
		case RedoTypeCheck:
			if begin > 0 {
				log.Fatalf("invalid '%s' after 'begin': %s", ent.Type, r)
				return false
			}
			rd.checkIndex = ent.Index
		case RedoTypeBegin:
			if begin > 0 {
				log.Fatalf("invalid '%s' after 'begin': %s", ent.Type, r)
				return false
			}
			begin = ent.Index
		case RedoTypeCommit:
			begin = 0
			rd.commitIndex = ent.Index
		case RedoTypeRollback:
			begin = 0
		default:
			if begin == 0 {
				log.Fatalf("invalid '%s' before 'begin': %s", ent.Type, r)
				return false
			}
		}
	}
	if begin > 0 {
		rd.wal.Append(NewRedoRollbackEntry(begin - 1).Encode())
	}

	return true
}

func (rd *RedoManager)SeekAfterLastCheckpoint() {
	lineno := 0
	rd.wal.SeekTo(0)
	for num := 0; rd.wal.Next(); num ++ {
		r := rd.wal.Item()
		var ent RedoEntry
		if ent.Decode(r) == false {
			log.Fatalf("error")
		}
		if ent.Type == RedoTypeCheck {
			lineno = num + 1
		}
	}
	rd.wal.SeekTo(lineno)
}

func (rd *RedoManager)GetAllUncheckedEntries() []*RedoEntry {
	rd.wal.SeekTo(0)
	valid := 0
	ret := make([]*RedoEntry, 0)
	for rd.wal.Next() {
		r := rd.wal.Item()
		var ent RedoEntry
		if ent.Decode(r) == false {
			log.Fatalf("bad entry: %s", r)
			break
		}
		switch ent.Type {
		case RedoTypeCheck:
			ret = make([]*RedoEntry, 0)
		case RedoTypeBegin:
			//
		case RedoTypeCommit:
			valid = len(ret)
		case RedoTypeRollback:
			ret = ret[0 : valid]
		default:
			ret = append(ret, &ent)
		}
	}
	return ret
}

func (rd *RedoManager)CommitIndex() int64 {
	return rd.commitIndex
}

func (rd *RedoManager)Check() {
	rd.wal.Append(NewRedoCheckEntry(rd.commitIndex).Encode())
	rd.checkIndex = rd.commitIndex
}

func (rd *RedoManager)WriteBatch(ents []*RedoEntry) {
	var min int64 = 0
	var max int64 = 0
	for _, ent := range ents {
		if min == 0 {
			min = ent.Index
		}
		min = util.MinInt64(min, ent.Index)
		max = util.MaxInt64(max, ent.Index)
	}
	if min == 0 || max == 0 {
		log.Fatal("error")
	}
	
	rd.wal.Append(NewRedoBeginEntry(min).Encode())
	for _, ent := range ents {
		if ent.Type == RedoTypeSet || ent.Type == RedoTypeDel {
			rd.wal.Append(ent.Encode())
		}
	}
	rd.wal.Append(NewRedoCommitEntry(max).Encode())
	
	rd.commitIndex = max
}

func (rd *RedoManager)Set(idx int64, key string, val string) {
	rd.WriteBatch([]*RedoEntry{NewRedoSetEntry(idx, key, val)})
}

func (rd *RedoManager)Del(idx int64, key string) {
	rd.WriteBatch([]*RedoEntry{NewRedoDelEntry(idx, key)})
} 

///////////////////////////////////////////////////////////////////

func (rd *RedoManager)CleanAll() {
	rd.commitIndex = 0
	rd.checkIndex = 0

	rd.wal.Close()
	err := os.Remove(rd.path)
	if err != nil {
		log.Fatalf("Remove %s error: %s", rd.path, err.Error())
	}
	
	rd.wal = store.OpenWalFile(rd.path)
	if rd.wal == nil {
		log.Fatalf("Open %s error: %s", rd.path, err.Error())
	}
}
