package store

import (
	"os"
	"log"
	"fmt"
	"sort"
	"strings"
	// "path/filepath"
	"myutil"
)

type KVStore struct{
	dir string
	mm map[string]string
	wal *WALFile

	filenums map[int]string // num : ext
	minnum int
	maxnum int
	nextnum int
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
	db.filenums = make(map[int]string)
	db.minnum = 100000
	db.maxnum = 999999
	db.nextnum = db.minnum

	db.scanFiles()

	nums := make([]int, 0)
	for num, ext := range db.filenums {
		if ext == "wal" {
			nums = append(nums, num)
		}
	}
	sort.Ints(nums)
	for _, num := range nums {
		name := fmt.Sprintf("%s/%d.wal", db.dir, num)
		log.Println("load", name)
		wal := OpenWALFile(name)
		defer wal.Close()
		db.loadWAL(wal)
	}

	log.Println(db.mm)

	db.wal = db.nextWAL()
	for k, v := range db.mm {
		r := fmt.Sprintf("set %s %s", k, v);
		db.wal.Append(r)
	}

	for _, num := range nums {
		name := fmt.Sprintf("%s/%d.wal", db.dir, num)
		os.Remove(name)
	}

	/*
	# WAL switch
	create(wal.NEW)
	write(wal.NEW, data)
	rename(wal, wal.OLD)
	rename(wal.NEW, wal)
	remove(wal.OLD)
	*/
	return db
}

func (db *KVStore)scanFiles(){
	f, _ := os.Open(db.dir)
	defer f.Close()
	names, _ := f.Readdirnames(-1)
	for _, n := range names {
		ps := strings.Split(n, ".")
		if len(ps) != 2 {
			continue
		}
		ext := ps[1]
		if ext == "wal" {
			num := myutil.Atoi(ps[0])
			if num > db.minnum {
				db.filenums[num] = ext
			}
			if num > db.nextnum {
				db.nextnum = num
			}
		}
	}
}

func (db *KVStore)nextWAL() *WALFile{
	for db.filenums[db.nextnum] != "" {
		db.nextnum ++;
		if db.nextnum > db.maxnum {
			db.nextnum = db.minnum
		}
	}
	name := fmt.Sprintf("%s/%d.wal", db.dir, db.nextnum)
	wal := OpenWALFile(name)
	db.filenums[db.nextnum] = "wal"
	return wal
}

func (db *KVStore)loadWAL(wal *WALFile){
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
