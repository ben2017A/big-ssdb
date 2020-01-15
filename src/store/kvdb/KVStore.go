package kvdb

type KVStore struct{
	mm map[string]string
}

func (store *KVStore)Get(key string) (string, bool){
	return "", false
}

func (store *KVStore)Set(key, val string) bool{
	return true
}

func (store *KVStore)Del(key string) bool{
	return true
}
