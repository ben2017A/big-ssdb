package raft

type Service interface{
	// LastCommitted() int64;

	LastApplied() int64
	ApplyEntry(ent *Entry)
}
