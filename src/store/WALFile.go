package store

type WALFile struct{
}

func (wal *WALFile)Append(record []byte){

}

func (wal *WALFile)Get(recordNum int) []byte{
	return nil
}
