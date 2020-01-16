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

	fn_new := dir + "/log.wal" + ".NEW"
	fn_cur := dir + "/log.wal"
	fn_old := dir + "/log.wal" + ".OLD"
	fn_tmp := dir + "/log.wal" + ".TMP"

	if myutil.FileExists(fn_old) {
		db.loadWALFile(fn_old)
	}
	if myutil.FileExists(fn_cur) {
		db.loadWALFile(fn_cur)
	}

	wal := OpenWALFile(fn_tmp)
	for k, v := range db.mm {
		r := fmt.Sprintf("set %s %s", k, v);
		wal.Append(r)
	}
	wal.Close()

	os.Rename(fn_tmp, fn_old)
	if myutil.FileExists(fn_new) {
		os.Rename(fn_new, fn_cur)
	} else {
		os.Remove(fn_cur)
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
