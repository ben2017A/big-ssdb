package raft

type Db interface {
	Close()
	Fsync() error
	Get(key string) string
	Set(key string, val string)
	All() map[string]string
	CleanAll()
} 
