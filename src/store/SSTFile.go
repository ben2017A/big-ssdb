package store

import (
	// "os"
	"log"
	"sort"
	"strings"
	"util"
)

type SSTFile struct{
	readonly bool
	valid bool
	key string
	val string
	wal *WALFile
}

func OpenSSTFile(filename string) *SSTFile{
	sst := new(SSTFile)
	sst.valid = false
	if util.FileExists(filename) {
		sst.readonly = true
	}else{
		sst.readonly = false
	}
	sst.wal = OpenWALFile(filename) // TODO: 不使用 WALFile
	if sst.wal == nil {
		return nil
	}
	return sst;
}

func (sst *SSTFile)Close(){
	sst.wal.Close()
}

func (sst *SSTFile)Save(kvs map[string]string) bool{
	if sst.readonly {
		return false
	}
	// only allow save once
	sst.readonly = true

	arr := make([][2]string, len(kvs))
	n := 0
	for k, v := range kvs {
		arr[n] = [2]string{k, v}
		n ++
	}
	sort.Slice(arr, func(i, j int) bool{
		return arr[i][0] < arr[j][0]
	})

	for _, kv := range arr {
		s := kv[0] + " " + kv[1]
		sst.wal.Append(s)
	}

	return true
}

func (sst *SSTFile)Get(key string) (val string, found bool){
	sst.Seek(key)
	if sst.Valid() && sst.key == key {
		return sst.val, true
	}
	return "", false
}

// seek to first record equal to or greater than key
func (sst *SSTFile)Seek(key string){
	sst.valid = false

	wal := sst.wal
	wal.SeekTo(0)
	for wal.Next() {
		r := wal.Item()
		ps := strings.SplitN(r, " ", 2)
		if len(ps) != 2 {
			log.Println("bad record:", r)
			continue
		}
		if key <= ps[0] {
			sst.valid = true
			sst.key = ps[0]
			sst.val = ps[1]
			return
		}
	}
}

func (sst *SSTFile)Valid() bool{
	return sst.valid
}

func (sst *SSTFile)Read() (string, string){
	k := sst.key
	v := sst.val

	sst.valid = false
	if r := sst.wal.Read(); r != "" {
		ps := strings.SplitN(r, " ", 2)
		if len(ps) != 2 {
			log.Println("bad record:", r)
		} else {
			sst.valid = true
			sst.key = ps[0]
			sst.val = ps[1]
		}
	}

	return k, v
}
