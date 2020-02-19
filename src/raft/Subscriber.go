package raft

// rename to Service?
type Subscriber interface{
	// LastCommitted() int64;

	LastApplied() int64
	ApplyEntry(ent *Entry)
}
