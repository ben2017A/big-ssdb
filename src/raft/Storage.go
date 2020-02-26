package raft

type Storage interface{
	All() map[string]string
	Get(key string) string
	Set(key string, val string)
	Close()
} 
