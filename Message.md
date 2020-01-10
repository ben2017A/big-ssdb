	Message
		cmd
		src
		dst
		index
		term
		data

	ClientMessage(RPC)
		JoinGroup
		AddMember(for testing)
	RaftMessage(UDP/unreliable messaging)
		ErrorAck(not used)
			wrong srcId
			wrong dstId
		UpdateTerm
		RequestVote
		RequestVoteAck
		CommitEntry(leaderCommitLogIndex, leaderCommitLogTerm)
		AppendEntry
			AddMember
			HeartBeat
			OtherCommand
		AppendEntryAck
