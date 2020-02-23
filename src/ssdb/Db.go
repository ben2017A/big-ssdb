package ssdb

import (
	"log"
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
	db := new(Db)
	db.kv = store.OpenKVStore(dir)
	db.redo = xna.OpenRedolog(dir + "/redo.log")
	db.replayRedolog()
	return db
}

func (db *Db)replayRedolog() {
	db.redo.SeekToLastCheckpoint()
	count := 0
	for {
		tx := db.redo.NextTransaction()
		if tx == nil {
			break
		}
		count ++
		for _, ent := range tx.Entries() {
			log.Println("    Redo", ent)
			switch ent.Type {
			case "set":
				db.kv.Set(ent.Key, ent.Val)
			case "del":
				db.kv.Del(ent.Key)
			}
		}
	}
	if count > 0 {
		db.redo.WriteCheckpoint()
	} else {
		log.Println("nothing to redo")
	}
}

func (db *Db)CommitIndex() int64 {
	return db.redo.CommitIndex()
}

func (db *Db)Get(key string) string {
	return db.kv.Get(key)
}

func (db *Db)Set(idx int64, key string, val string) {
	tx := xna.NewTransaction()
	tx.Set(idx, key, val)

	db.redo.WriteTransaction(tx)
	db.kv.Set(key, val)
}

func (db *Db)Del(idx int64, key string) {
	tx := xna.NewTransaction()
	tx.Del(idx, key)
	
	db.redo.WriteTransaction(tx)
	db.kv.Del(key)
}

func (db *Db)Incr(idx int64, key string, delta string) string {
	old := db.kv.Get(key)
	num := util.Atoi64(old) + util.Atoi64(delta)
	val := util.Itoa64(num)
	
	tx := xna.NewTransaction()
	tx.Set(idx, key, val)
	
	db.redo.WriteTransaction(tx)
	db.kv.Set(key, val)
	
	return val
}
