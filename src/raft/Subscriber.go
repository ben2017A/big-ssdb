package raft

type Subscriber interface{
	LastApplied() uint64
	ApplyEntry(ent *Entry)
}
