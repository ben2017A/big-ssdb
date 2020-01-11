package raft

import (
	"log"
	"sort"
	"math/rand"
)

const ElectionTimeout = 5 * 1000
const HeartbeatTimeout   = 4 * 1000

const RequestVoteTimeout = 1 * 1000
const ReplicationTimeout = 1 * 1000

type Node struct{
	Id string
	Role string

	Term uint32

	Members map[string]*Member

	voteFor string
	votesReceived map[string]string

	electionTimeout int
	requestVoteTimeout int

	store *Store
	Transport Transport
}

func NewNode() *Node{
	node := new(Node)
	node.Term = 0
	node.Members = make(map[string]*Member)

	node.store = NewStore()

	node.electionTimeout = ElectionTimeout + rand.Intn(100)

	return node
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "candidate" {
		node.requestVoteTimeout -= timeElapse
		if node.requestVoteTimeout <= 0 {
			log.Println("RequestVote timeout")
			node.becomeFollower()
			return
		}
	}
	if node.Role == "follower" {
		node.electionTimeout -= timeElapse
		if node.electionTimeout <= 0 {
			log.Println("Election timeout")
			node.becomeCandidate()
		}
	}
	if node.Role == "leader" {
		for _, m := range node.Members {
			m.ReplicationTimeout -= timeElapse
			if m.ReplicationTimeout <= 0 {
				node.replicateMember(m)
			}

			m.HeartbeatTimeout -= timeElapse
			if m.HeartbeatTimeout <= 0 {
				// log.Println("Heartbeat timeout for node", m.Id)
				node.heartbeatMember(m)
			}
		}
	}
}

func (node *Node)becomeFollower(){
	log.Println("convert", node.Role, "=> follower")
	node.Role = "follower"
	node.voteFor = ""
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

	for _, m := range node.Members {
		// discover leader by receiving valid AppendEntry
		m.Role = "follower"
	}
}

func (node *Node)becomeCandidate(){
	log.Println("convert", node.Role, "=> candidate")
	node.Role = "candidate"
	node.Term += 1
	node.voteFor = node.Id
	node.votesReceived = make(map[string]string)

	for _, m := range node.Members {
		msg := new(Message)
		msg.Cmd = "RequestVote"
		msg.Dst = m.Id
		msg.Data = "please vote me"
		node.send(msg)
	}

	node.requestVoteTimeout = RequestVoteTimeout
}

func (node *Node)becomeLeader(){
	log.Println("convert", node.Role, "=> leader")
	node.Role = "leader"
	node.voteFor = ""

	for _, m := range node.Members {
		m.Role = "follower"
		m.NextIndex = node.store.LastIndex + 1
		m.MatchIndex = 0
		m.HeartbeatTimeout = 0
		m.ReplicationTimeout = ReplicationTimeout
		node.heartbeatMember(m)
	}
}

/* ############################################# */

func (node *Node)heartbeatMember(m *Member){
	m.HeartbeatTimeout = HeartbeatTimeout

	next := new(Entry)
	next.Type = "Heartbeat"
	next.CommitIndex = node.store.CommitIndex
	prev := node.store.GetEntry(m.NextIndex - 1)

	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Dst = m.Id
	if prev != nil {
		msg.PrevIndex = prev.Index
		msg.PrevTerm = prev.Term
	}
	msg.Data = next.Encode()
	node.send(msg)
}

func (node *Node)replicateMember(m *Member){
	m.ReplicationTimeout = ReplicationTimeout

	next := node.store.GetEntry(m.NextIndex)
	if next == nil {
		return;
	}
	next.CommitIndex = node.store.CommitIndex
	prev := node.store.GetEntry(m.NextIndex - 1)

	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Dst = m.Id
	if prev != nil {
		msg.PrevIndex = prev.Index
		msg.PrevTerm = prev.Term
	}
	msg.Data = next.Encode()
	node.send(msg)

	m.HeartbeatTimeout = HeartbeatTimeout
}

/* ############################################# */

func (node *Node)HandleRaftMessage(msg *Message){
	// smaller msg.Term is rejected
	if msg.Term < node.Term {
		// TODO: notify sender to update term by reply an Ack
		log.Println("ignore msg.Term =", msg.Term, " < currentTerm = ", node.Term)
		return
	}
	// node.Term is set to be larger msg.Term
	if msg.Term > node.Term {
		node.Term = msg.Term
		if node.Role != "follower" {
			node.becomeFollower()
		}
	}

	if node.Role == "leader" {
		if msg.Cmd == "AppendEntryAck" {
			node.handleAppendEntryAck(msg)
		}
	}
	if node.Role == "candidate" {
		if msg.Cmd == "RequestVoteAck" {
			node.handleRequestVoteAck(msg)
		}
	}
	if node.Role == "follower" {
		if msg.Cmd == "RequestVote" {
			node.handleRequestVote(msg)
		}
		if msg.Cmd == "AppendEntry" {
			node.handleAppendEntry(msg)
		}
	}
}

func (node *Node)handleRequestVote(msg *Message){
	// node.voteFor == msg.Src: retransimitted/duplicated RequestVote
	if node.voteFor != "" && node.voteFor != msg.Src {
		// TODO:
		log.Println(node.voteFor, msg.Src)
		return
	}
	if msg.PrevTerm > node.store.LastTerm || (msg.PrevTerm == node.store.LastTerm && msg.PrevIndex >= node.store.LastIndex) {
		node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
		log.Println("vote for", msg.Src)
		node.voteFor = msg.Src

		ack := new(Message)
		ack.Cmd = "RequestVoteAck"
		ack.Dst = msg.Src
		ack.Data = "true"
		node.send(ack)
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	// checkQuorum
	if msg.Term == node.Term && msg.PrevIndex <= node.store.LastIndex {
		log.Println("receive ack from", msg.Src)
		node.votesReceived[msg.Src] = "ok"

		if len(node.votesReceived) + 1 > (len(node.Members) + 1)/2 {
			log.Println("Got majority votes")
			node.becomeLeader()
		}
	}
}

func (node *Node)send(msg *Message){
	msg.Src = node.Id
	msg.Term = node.Term
	if msg.Cmd != "AppendEntry" {
		msg.PrevIndex = node.store.LastIndex
		msg.PrevTerm = node.store.LastTerm
	}
	node.Transport.Send(msg)
}

func (node *Node)sendAppendEntryAck(leaderId string, success bool){
	ack := new(Message)
	ack.Cmd = "AppendEntryAck"
	ack.Dst = leaderId
	if success {
		ack.Data = "true"
	}else{
		ack.Data = "false"
	}
	node.send(ack)
}

func (node *Node)handleAppendEntry(msg *Message){
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
	// TODO: 判断是否已经存在 leader
	node.Members[msg.Src].Role = "leader"

	if msg.PrevIndex > 0 && msg.PrevTerm > 0 {
		prev := node.store.GetEntry(msg.PrevIndex)
		if prev == nil || prev.Term != msg.PrevTerm {
			node.sendAppendEntryAck(msg.Src, false)
			return
		}
	}

	entry := DecodeEntry(msg.Data)

	if entry.Type == "Entry" {
		old := node.store.GetEntry(entry.Index)
		if old != nil && old.Term != entry.Term {
			log.Println("overwrite conflict entry")
		}
		node.store.AppendEntry(*entry)
		node.sendAppendEntryAck(msg.Src, true)	
	}

	// update commitIndex
	if node.store.CommitIndex < entry.CommitIndex{
		commitIndex := MinU64(entry.CommitIndex, node.store.LastIndex)
		node.store.CommitEntry(commitIndex)
	}
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]

	if msg.Data == "false" {
		m.NextIndex = MaxU64(1, m.NextIndex - 1)
		m.MatchIndex = 0
		log.Println("decrease NextIndex for node", m.Id, "to", m.NextIndex)
	}else{
		m.NextIndex = MaxU64(m.NextIndex, msg.PrevIndex + 1)
		m.MatchIndex = MaxU64(m.MatchIndex, msg.PrevIndex)
	
		// sort matchIndex[] in descend order
		matchIndex := make([]uint64, len(node.Members) + 1)
		matchIndex[0] = node.store.LastIndex
		for _, m := range node.Members {
			matchIndex = append(matchIndex, m.MatchIndex)
		}
		sort.Slice(matchIndex, func(i, j int) bool{
			return matchIndex[i] > matchIndex[j]
		})
		log.Println(matchIndex)
		commitIndex := matchIndex[len(matchIndex)/2]
		node.store.CommitEntry(commitIndex)
	}

	node.replicateMember(m)
}

/* ############################################# */

func (node *Node)Write(data string){
	entry := new(Entry)
	entry.Type = "Entry"
	entry.Index = node.store.LastIndex + 1
	entry.Term = node.Term
	entry.Data = data
	
	node.store.AppendEntry(*entry)

	for _, m := range node.Members {
		node.replicateMember(m)
	}
}

/* ############################################# */
