package raft

// rename to Service?
type Subscriber interface{
	LastApplied() uint64
	// LastCommitted() uint64;
	ApplyEntry(ent *Entry)
}
