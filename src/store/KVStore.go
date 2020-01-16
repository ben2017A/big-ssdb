package store

import (
	"os"
	"log"
	"fmt"
	"strings"
	// "path/filepath"
	"myutil"
)

type KVStore struct{
	dir string
	mm map[string]string
	wal *WALFile
}
/*
# WAL switch
create(NEW)
compact(log, OLD)
remove(log)
rename(NEW, log)
*/

func OpenKVStore(dir string) *KVStore{
	if !myutil.IsDir(dir) {
		os.MkdirAll(dir, 0755)
	}
	if !myutil.IsDir(dir) {
		return nil
	}

	db := new(KVStore)
	db.dir = dir
	db.mm = make(map[string]string)

	// normally, at most 2 of these 3 files should exists
	fn_cur := dir + "/log.wal"
	fn_old := fn_cur + ".OLD"
	fn_new := fn_cur + ".NEW"
	if myutil.FileExists(fn_new) {
		if myutil.FileExists(fn_old) {
			os.Rename(fn_old, fn_cur)
		}
	} else {
		if myutil.FileExists(fn_cur) {
			os.Rename(fn_cur, fn_new)
		}
		if myutil.FileExists(fn_old) {
			os.Rename(fn_old, fn_cur)
		}
	}

	{
		db.loadWALFile(fn_cur)
		db.loadWALFile(fn_new)

		wal := OpenWALFile(fn_old)
		for k, v := range db.mm {
			r := fmt.Sprintf("set %s %s", k, v);
			wal.Append(r)
		}
		wal.Close()

		os.Remove(fn_cur);
		os.Remove(fn_new);
	}

	db.wal = OpenWALFile(fn_cur)

	return db
}

func (db *KVStore)loadWALFile(fn string){
	log.Println("load", fn)
	wal := OpenWALFile(fn)
	wal.SeekTo(0)
	for {
		r := wal.Read()
		if r == "" {
			break;
		}
		ps := strings.Split(r, " ")
		switch ps[0] {
		case "set":
			db.mm[ps[1]] = ps[2]
		case "del":
			delete(db.mm, ps[1])
		}
	}
}

func (db *KVStore)Close(){
	if db.wal != nil {
		db.wal.Close()
	}
}

func (db *KVStore)Get(key string) (string, bool){
	v, ok := db.mm[key]
	return v, ok
}

func (db *KVStore)Set(key string, val string) error{
	r := fmt.Sprintf("set %s %s", key, val);
	db.wal.Append(r)
	db.mm[key] = val
	return nil
}

func (db *KVStore)Del(key string) error{
	r := fmt.Sprintf("del %s", key);
	db.wal.Append(r)
	delete(db.mm, key)
	return nil
}
