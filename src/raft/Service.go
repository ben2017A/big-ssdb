package raft

type Service interface{
	LastApplied() int64
	ApplyEntry(ent *Entry)
}
