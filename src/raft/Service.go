package raft

type Service interface{
	// raft 可以据此淘汰太久远的 log
	// LastCommitted() int64;

	LastApplied() int64
	ApplyEntry(ent *Entry)
}
