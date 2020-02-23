package ssdb

import (
	"store"
	"xna"
)

type Db struct {
	kv *store.KVStore
	redo *xna.Redolog
}

func OpenDb(dir string) *Db {
	db := new(Db)
	return db
}
