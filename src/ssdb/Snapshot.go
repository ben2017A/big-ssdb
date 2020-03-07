package ssdb

import (
	"fmt"
	"os"
	"log"
	"store"
	"util"
)

type Snapshot struct {
	commitIndex int64
	wal *store.WalFile
}

func NewSnapshotWriter(commitIndex int64, path string) *Snapshot {
	if util.FileExists(path) {
		err := os.Remove(path)
		if err != nil {
			log.Fatal(err)
		}
	}

	sn := new(Snapshot)
	sn.commitIndex = commitIndex
	sn.wal = store.OpenWalFile(path)
	
	sn.wal.Append(fmt.Sprintf("%d", commitIndex))
	
	return sn
}

func NewSnapshotReader(path string) *Snapshot {
	if !util.FileExists(path) {
		log.Println("snapshot file not found:", path)
		return nil
	}
	
	sn := new(Snapshot)
	sn.wal = store.OpenWalFile(path)
	if sn.Next() {
		sn.commitIndex = util.Atoi64(sn.wal.Item())
	} else {
		sn.commitIndex =  = 0
	}
	
	return sn
}

func (sn *Snapshot)Close() {
	sn.wal.Close()
}

func (sn *Snapshot)CommitIndex() int64 {
	return sn.commitIndex
}

func (sn *Snapshot)Next() bool {
	return sn.wal.Next()
}

func (sn *Snapshot)Item() string {
	return sn.wal.Item()
}

func (sn *Snapshot)Append(record string) {
	sn.wal.Append(record)
}
