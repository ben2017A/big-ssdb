package ssdb

import (
	"log"
	"path/filepath"
	"store"
	"util"
)

// 非线程安全
type Db struct {
	dir string
	kv *store.KVStore
	redo *RedoManager
}

func OpenDb(dir string) *Db {
	dir, _ = filepath.Abs(dir)
	
	db := new(Db)
	db.dir = dir
	db.kv = store.OpenKVStore(dir)
	db.redo = OpenRedoManager(dir + "/redo.log")
	
	if db.kv == nil || db.redo == nil {
		return nil
	}
	if !db.recover() {
		return nil
	}
	
	log.Printf("Open Db %s", db.dir)
	log.Printf("    CommitIndex: %d", db.CommitIndex())
	
	return db
}

func (db *Db)Close() {
	db.kv.Close()
	db.redo.Close()
}

func (db *Db)recover() bool {
	ents := db.redo.GetAllUncheckedEntries()

	count := 0
	for _, ent := range ents {
		switch ent.Type {
		case RedoTypeSet:
			db.kv.Set(ent.Key, ent.Val)
		case RedoTypeDel:
			db.kv.Del(ent.Key)
		}
	}
	
	if count > 0 {
		db.redo.Check()
	} else {
		log.Println("nothing to redo")
	}
	return true
}

func (db *Db)CommitIndex() int64 {
	return db.redo.CommitIndex()
}

/////////////////////////////////////////////////////////////////////

func (db *Db)Get(key string) string {
	return db.kv.Get(key)
}

func (db *Db)Set(idx int64, key string, val string) {
	db.redo.Set(idx, key, val)
	db.kv.Set(key, val)
}

func (db *Db)Del(idx int64, key string) {
	db.redo.Del(idx, key)
	db.kv.Del(key)
}

func (db *Db)Incr(idx int64, key string, delta string) string {
	old := db.kv.Get(key)
	num := util.Atoi64(old) + util.Atoi64(delta)
	val := util.I64toa(num)
	
	db.redo.Set(idx, key, val)
	db.kv.Set(key, val)

	return val
}

//////////////////////////////////////////////////////////////////////

func (db *Db)CleanAll() {
	log.Printf("Clean Db %s", db.dir)
	db.redo.CleanAll()
	db.kv.CleanAll()
}

func (db *Db)MakeFileSnapshot(path string) bool {
	sn := NewSnapshotWriter(db.CommitIndex(), path)
	if sn == nil {
		return false
	}
	defer sn.Close()
	
	for k, v := range db.kv.All() {
		ent := &store.KVEntry{"set", k, v}
		sn.Append(ent.Encode())
	}

	return true
}
