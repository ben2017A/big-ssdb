package raft

type Member struct{
	Id string
	Role string
	Addr string

	// sliding window
	NextIndex uint64
	MatchIndex uint64

	KeepaliveTimeout int
	ReplicationTimeout int
}

func NewMember(id, addr string) *Member{
	ret := new(Member)
	ret.Id = id
	ret.Addr = addr
	return ret
}
