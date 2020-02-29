package ssdb

import (
	"log"
	"path/filepath"
	"store"
	"xna"
	"util"
)

// TODO: 线程安全
type Db struct {
	kv *store.KVStore
	redo *xna.Redolog
}

func OpenDb(dir string) *Db {
	dir, _ = filepath.Abs(dir)
	
	db := new(Db)
	db.kv = store.OpenKVStore(dir)
	db.redo = xna.OpenRedolog(dir + "/redo.log")
	
	if db.kv == nil || db.redo == nil {
		return nil
	}
	if !db.recover() {
		return nil
	}
	
	log.Printf("Open Db %s", dir)
	log.Printf("    LastIndex: %d", db.LastIndex())
	
	return db
}

func (db *Db)Close() {
	db.kv.Close()
	db.redo.Close()
}

func (db *Db)recover() bool {
	db.redo.SeekAfterLastCheckpoint()
	count := 0
	for {
		tx := db.redo.NextTransaction()
		if tx == nil {
			break
		}
		count ++
		for _, ent := range tx.Entries() {
			log.Println("    Redo", ent)
		}
		db.applyTransaction(tx)
	}
	if count > 0 {
		db.redo.WriteCheckpoint()
	} else {
		log.Println("nothing to redo")
	}
	return true
}

func (db *Db)LastIndex() int64 {
	return db.redo.LastIndex()
}

// TODO: isolation
func (db *Db)StartTransaction() *xna.Transaction {
	return nil
}

func (db *Db)CommitTransaction(tx *xna.Transaction) {
	if !db.redo.WriteTransaction(tx) {
		log.Fatal("Write redolog failed.")
		return
	}
	db.applyTransaction(tx)
}

func (db *Db)applyTransaction(tx *xna.Transaction) {
	for _, ent := range tx.Entries() {
		switch ent.Type {
		case xna.EntryTypeSet:
			db.kv.Set(ent.Key, ent.Val)
		case xna.EntryTypeDel:
			db.kv.Del(ent.Key)
		}
	}
}

/////////////////////////////////////////////////////////////////////

func (db *Db)Get(key string) string {
	return db.kv.Get(key)
}

func (db *Db)Set(idx int64, key string, val string) {
	tx := xna.NewTransaction()
	tx.AddEntry(xna.NewSetEntry(idx, key, val))

	db.CommitTransaction(tx)
}

func (db *Db)Del(idx int64, key string) {
	tx := xna.NewTransaction()
	tx.AddEntry(xna.NewDelEntry(idx, key))
	
	db.CommitTransaction(tx)
}

func (db *Db)Incr(idx int64, key string, delta string) string {
	old := db.kv.Get(key)
	num := util.Atoi64(old) + util.Atoi64(delta)
	val := util.I64toa(num)
	
	tx := xna.NewTransaction()
	tx.AddEntry(xna.NewSetEntry(idx, key, val))
	
	db.CommitTransaction(tx)
	return val
}

//////////////////////////////////////////////////////////////////////

func (db *Db)MakeFileSnapshot(path string) bool {
	sn := NewSnapshotWriter(db.LastIndex(), path)
	for k, v := range db.kv.All() {
		ent := &store.KVEntry{"set", k, v}
		sn.Append(ent.Encode())
	}
	sn.Close()
	return true
}
