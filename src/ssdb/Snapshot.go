package ssdb

import (
	"fmt"
	"store"
	"util"
)

type Snapshot struct {
	lastIndex int64
	wal *store.WalFile
}

func NewSnapshotWriter(lastIndex int64, path string) *Snapshot {
	sn := new(Snapshot)
	sn.lastIndex = lastIndex
	sn.wal = store.OpenWalFile(path)
	
	sn.wal.Append(fmt.Sprintf("%d", lastIndex))
	
	return sn
}

func NewSnapshotReader(path string) *Snapshot {
	sn := new(Snapshot)
	sn.wal = store.OpenWalFile(path)
	
	if !sn.Next() {
		return nil
	}
	sn.lastIndex = util.Atoi64(sn.wal.Item())
	
	return sn
}

func (sn *Snapshot)Close() {
	sn.wal.Close()
}

func (sn *Snapshot)LastIndex() int64 {
	return sn.lastIndex
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
