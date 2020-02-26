package raft

type Member struct{
	Id string
	Role string
	Addr string

	// sliding window
	NextIndex int64   // next_send
	SendWindow int64  // 
	MatchIndex int64  // last_ack

	HeartbeatTimeout int
	ReplicationTimeout int
}

func NewMember(id, addr string) *Member{
	ret := new(Member)
	ret.Role = "follower"
	ret.Id = id
	ret.Addr = addr
	ret.SendWindow = 3
	return ret
}
