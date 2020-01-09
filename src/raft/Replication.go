package raft

type Replication struct{
	Term uint32
	Index uint64
	Data string
	AckReceived map[string]string
	ResendTimeout int
}

func NewReplication() *Replication{
	ret := new(Replication)
	ret.AckReceived = make(map[string]string)
	return ret
}
