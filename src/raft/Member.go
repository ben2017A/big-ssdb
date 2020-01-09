package raft

type Member struct{
	Id string
	Role string
	Addr string

	NextIndex uint64
	MatchIndex uint64
}

func NewMember(id, addr string) *Member{
	ret := new(Member)
	ret.Id = id
	ret.Addr = addr
	return ret
}
