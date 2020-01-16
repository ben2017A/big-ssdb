package store

type KVMeta struct{

}

/*
# WAL switch
create(wal.NEW)
write(wal.NEW, data)
rename(wal, wal.OLD)
rename(wal.NEW, wal)
remove(wal.OLD)
*/
func OpenKVMeta(filename string) *KVMeta{
	return nil
}

func (meta *KVMeta)Close(){

}

func (meta *KVMeta)AddWALNum(num int){
}

func (meta *KVMeta)DelWALNum(num int){
}

func (meta *KVMeta)WALNums() []int{
	return nil
}
