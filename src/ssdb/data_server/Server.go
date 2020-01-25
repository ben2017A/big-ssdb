package data_server

import (
	"store"
)

type Server struct{
	serv_range [2]string
	lock_range [2]string
	data_range [2]string

	meta_db *store.KVStore
	data_db *store.KVStore
}

func NewServer(dir string) *Server {
	ret := new(Server)

	ret.meta_db = store.OpenKVStore(dir + "/meta")
	ret.data_db = store.OpenKVStore(dir + "/data")

	ret.serv_range[0], _ = ret.meta_db.Get("serv_range[0]")
	ret.serv_range[1], _ = ret.meta_db.Get("serv_range[1]")
	ret.lock_range[0], _ = ret.meta_db.Get("lock_range[0]")
	ret.lock_range[1], _ = ret.meta_db.Get("lock_range[1]")

	return ret
}

func (serv *Server)LockRange(begin string, end string) bool{
	serv.meta_db.Set("lock_range[0]", begin)
	serv.meta_db.Set("lock_range[1]", end)
	serv.lock_range = [2]string{begin, end}
	return true;
}

func (serv *Server)UnlockRange(begin string, end string) bool{
	serv.meta_db.Set("lock_range[0]", "")
	serv.meta_db.Set("lock_range[1]", "")
	serv.lock_range = [2]string{begin, end}
	return true;
}

func (serv *Server)FreeRange(begin string, end string) bool{
	serv.meta_db.Set("serv_range[0]", serv.lock_range[1])
	serv.meta_db.Set("serv_range[1]", serv.serv_range[1])

	serv.meta_db.Set("lock_range[0]", "")
	serv.meta_db.Set("lock_range[1]", "")
	serv.lock_range = [2]string{"", ""}
	return true;
}

func in_range(s tring, r [2]string) bool{
	if r[0] == "" && r[1] == "" {
		return false;
	}
	if s < r[0] || s >= r[1] {
		return false;
	}
	return true
}

func (serv *Server)Get(key string) string{
	if !in_range(key, serv.serv_range) {
		log.Println(key, "not in serv_range", serv.serv_range)
		return ""
	}
	v, _ := serv.data_db.Get(key)
	return v
}

func (serv *Server)Set(key string, val string){
	if !in_range(key, serv.serv_range) {
		log.Println(key, "not in serv_range", serv.serv_range)
		return ""
	}
	if in_range(key, serv.lock_range) {
		log.Println(key, "locked in lock_range", serv.serv_range)
		return ""
	}
	serv.data_db.Set(key, val)
}

func (serv *Server)Del(key string){
	if !in_range(key, serv.serv_range) {
		log.Println(key, "not in serv_range", serv.serv_range)
		return ""
	}
	if in_range(key, serv.lock_range) {
		log.Println(key, "locked in lock_range", serv.serv_range)
		return ""
	}
	_ := serv.data_db.Del(key)
}

/*
meta -> data1: LockRange
meta -> data1: read_key
meta -> data2: write_key
meta -> data1: FreeRange/UnlockRange
*/
