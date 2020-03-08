package raft

type Db interface {
	Close()
	All() map[string]string
	Get(key string) string
	Set(key string, val string)
	CleanAll()
} 
