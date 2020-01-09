package raft

type Replication struct{
	Index uint64
	Term uint32
	Data string
	AckReceived map[string]string
	ResendTimeout int
}

func NewReplication() *Replication{
	ret := new(Replication)
	ret.AckReceived = make(map[string]string)
	return ret
}
