package store

import (
	"os"
	"log"
	"fmt"
	"sort"
	"path/filepath"
	"util"
)

// kvdb
type KVStore struct{
	dir string
	mm map[string]string
	wal *WalFile

	wal_new string
	wal_cur string
	wal_old string
	wal_tmp string
}

func OpenKVStore(dir string) *KVStore{
	dir, _ = filepath.Abs(dir)
	if !util.IsDir(dir) {
		os.MkdirAll(dir, 0755)
	}
	if !util.IsDir(dir) {
		return nil
	}
	log.Println("Open KVStore", dir)

	db := new(KVStore)
	db.dir = dir
	db.mm = make(map[string]string)
	
	if !db.recover() {
		return nil
	}

	return db
}

func (db *KVStore)Close(){
	if db.wal != nil {
		db.wal.Close()
	}
}

func (db *KVStore)recover() bool {
	db.wal_new = db.dir + "/log.wal" + ".NEW"
	db.wal_cur = db.dir + "/log.wal"
	db.wal_old = db.dir + "/log.wal" + ".OLD"
	db.wal_tmp = db.dir + "/log.wal" + ".TMP"

	if util.FileExists(db.wal_old) {
		db.loadWalFile(db.wal_old)
	}
	if util.FileExists(db.wal_cur) {
		db.loadWalFile(db.wal_cur)
	}
	db.compactWAL()
	return true
}

func (db *KVStore)compactWAL(){
	if db.wal != nil {
		db.wal.Close()
	}

	db.wal = OpenWalFile(db.wal_new)

	os.Remove(db.wal_tmp)
	wal := OpenWalFile(db.wal_tmp)
	{
		arr := make([][2]string, len(db.mm))
		n := 0
		for k, v := range db.mm {
			// testing
			// if strings.HasPrefix(k, "log#") {
			// 	idx := util.Atoi(k[4:]);
			// 	k = fmt.Sprintf("log#%03d", idx)
			// }
			arr[n] = [2]string{k, v}
			n ++
		}
		sort.Slice(arr, func(i, j int) bool{
			return arr[i][0] < arr[j][0]
		})
		
		for _, kv := range arr {
			r := fmt.Sprintf("set %s %s", kv[0], kv[1]);
			wal.Append(r)
		}
	}
	wal.Close()

	os.Rename(db.wal_tmp, db.wal_old)
	os.Rename(db.wal_new, db.wal_cur)
}

func (db *KVStore)loadWalFile(fn string){
	log.Println("    load", fn)
	wal := OpenWalFile(fn)
	defer wal.Close()

	ent := new(KVEntry)
	
	wal.SeekTo(0)
	for wal.Next() {
		r := wal.Item()
		if !ent.Decode(r) {
			log.Println("bad record:", r)
			continue
		}
		switch ent.Cmd {
		case "set":
			db.mm[ent.Key] = ent.Val
		case "del":
			delete(db.mm, ent.Key)
		}
	}
}

// 目前是无序的
func (db *KVStore)All() map[string]string {
	return db.mm
}

func (db *KVStore)Get(key string) string{
	v, _ := db.mm[key]
	return v
}

func (db *KVStore)Set(key string, val string){
	r := fmt.Sprintf("set %s %s", key, val);
	db.wal.Append(r)
	db.mm[key] = val
}

func (db *KVStore)Del(key string){
	r := fmt.Sprintf("del %s", key);
	db.wal.Append(r)
	delete(db.mm, key)
}
