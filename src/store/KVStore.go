package store

type KVStore struct{
	mm map[string]string
	wal *WALFile
}

func OpenKVStore(dir string) *KVStore{
	return nil
}

func (db *KVStore)Close(){

}

func (db *KVStore)Get(key string) (string, bool){
	return "", false
}

func (db *KVStore)Set(key string, val string) error{
	return nil
}

func (db *KVStore)Del(key string) error{
	return nil
}
