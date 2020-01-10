package store

type Cache struct{

}

func (store *Cache)Get(key string) (string, bool){
	return "", false
}

func (store *Cache)Set(key, val string) bool{
	return true
}

func (store *Cache)Del(key string) bool{
	return true
}
