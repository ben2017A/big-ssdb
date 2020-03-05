package raft

type Service interface{
	// Last checkpoint of applied entries within service
	LastApplied() int64
	// If entry is not idempotent, service must apply entry
	// and update lastApplied in one transaction for atomicity
	ApplyEntry(ent *Entry)
	
	InstallSnapshot()
}
