package raft

import (
	"fmt"
	"log"
	"sort"
	"math/rand"

	"myutil"
)

const ElectionTimeout = 5 * 1000
const HeartbeatTimeout   = 4 * 1000

const RequestVoteTimeout = 1 * 1000
const ReplicationTimeout = 1 * 1000

type Node struct{
	Id string
	Role string

	Term uint32
	lastApplied uint64

	Members map[string]*Member

	voteFor string
	votesReceived map[string]string

	electionTimeout int
	requestVoteTimeout int

	store *Storage
	transport Transport
}

func NewNode(store *Storage, transport Transport) *Node{
	node := new(Node)
	node.Role = "follower"
	node.Term = 0
	node.Members = make(map[string]*Member)
	node.electionTimeout = 0

	node.store = store
	node.transport = transport

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
	node.requestVoteTimeout = RequestVoteTimeout

	node.broadcast(NewRequestVoteMsg())
	if len(node.Members) == 0 {
		node.checkVoteResult()
	}
}

func (node *Node)checkVoteResult(){
	// checkQuorum
	if len(node.votesReceived) + 1 > (len(node.Members) + 1)/2 {
		log.Println("Got majority votes")
		node.becomeLeader()
	}
}

func (node *Node)becomeLeader(){
	log.Println("convert", node.Role, "=> leader")
	node.Role = "leader"
	node.voteFor = ""

	for _, m := range node.Members {
		m.Role = "follower"
		node.resetMemberState(m)
		node.heartbeatMember(m)
	}
}

/* ############################################# */

func (node *Node)resetMemberState(m *Member){
	m.NextIndex = node.store.LastIndex + 1
	m.MatchIndex = 0
	m.HeartbeatTimeout = 0
	m.ReplicationTimeout = ReplicationTimeout
}

func (node *Node)heartbeatMember(m *Member){
	m.HeartbeatTimeout = HeartbeatTimeout
	
	ent := NewHeartbeatEntry(node.store.CommitIndex)
	prev := node.store.GetEntry(m.NextIndex - 1)
	node.send(NewAppendEntryMsg(m.Id, ent, prev))
}

func (node *Node)replicateMember(m *Member){
	m.ReplicationTimeout = ReplicationTimeout

	ent := node.store.GetEntry(m.NextIndex)
	if ent == nil {
		return;
	}
	ent.CommitIndex = node.store.CommitIndex
	prev := node.store.GetEntry(m.NextIndex - 1)
	node.send(NewAppendEntryMsg(m.Id, ent, prev))

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
		node.send(NewRequestVoteAck(msg.Src, true))
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	if msg.Term == node.Term && msg.PrevIndex <= node.store.LastIndex {
		log.Println("receive vote from", msg.Src)
		node.votesReceived[msg.Src] = "ok"
		node.checkVoteResult()
	}
}

func (node *Node)handleAppendEntry(msg *Message){
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
	// TODO: 判断是否已经存在 leader
	node.Members[msg.Src].Role = "leader"

	if msg.PrevIndex > 0 && msg.PrevTerm > 0 {
		prev := node.store.GetEntry(msg.PrevIndex)
		if prev == nil || prev.Term != msg.PrevTerm {
			node.send(NewAppendEntryAck(msg.Src, false))
			return
		}
	}

	ent := DecodeEntry(msg.Data)

	if ent.Type != "Heartbeat" {
		old := node.store.GetEntry(ent.Index)
		if old != nil && old.Term != ent.Term {
			log.Println("delete conflict entry")
		}
		node.store.AppendEntry(*ent)
		node.send(NewAppendEntryAck(msg.Src, true))
	}

	node.store.CommitEntry(ent.CommitIndex)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]

	if msg.Data == "false" {
		m.NextIndex = myutil.MaxU64(1, m.NextIndex - 1)
		m.MatchIndex = 0
		log.Println("decrease NextIndex for node", m.Id, "to", m.NextIndex)
	}else{
		m.NextIndex = myutil.MaxU64(m.NextIndex, msg.PrevIndex + 1)
		m.MatchIndex = myutil.MaxU64(m.MatchIndex, msg.PrevIndex)
	
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

func (node *Node)newEntry(type_, data string) *Entry{
	ent := new(Entry)
	ent.Type = type_
	ent.Index = node.store.LastIndex + 1
	ent.Term = node.Term
	ent.Data = data
	return ent
}

func (node *Node)JoinGroup(leaderId string, leaderAddr string){
	if leaderId == node.Id {
		log.Println("could not join self:", leaderId)
		return
	}
	m := NewMember(leaderId, leaderAddr)
	node.Members[m.Id] = m
	node.resetMemberState(m)
	node.transport.Connect(m.Id, m.Addr)
	node.becomeFollower()
}

func (node *Node)AddMember(nodeId string, nodeAddr string){
	if node.Members[nodeId] != nil {
		log.Println("member already exists:", nodeId)
		return
	}
	if nodeId != node.Id {
		m := NewMember(nodeId, nodeAddr)
		node.Members[m.Id] = m
		node.resetMemberState(m)
		node.transport.Connect(m.Id, m.Addr)
	}

	data := fmt.Sprintf("%s %s", nodeId, nodeAddr)
	ent := node.newEntry("AddMember", data)
	node.store.AppendEntry(*ent)
	node.replicateAllMembers()	
}

// func (node *Node)DelMember(nodeId string){
// 	if nodeId == node.Id {
// 		log.Println("could not del self:", nodeId)
// 		return
// 	}
// 	if node.Members[nodeId] == nil {
// 		log.Println("member not exists:", nodeId)
// 		return
// 	}

// 	data := fmt.Sprintf("%s", nodeId)
// 	ent := node.newEntry("DelMember", data)
// 	node.store.AppendEntry(*ent)
// 	node.replicateAllMembers()	
// }

func (node *Node)Write(data string){
	ent := node.newEntry("Write", data)
	node.store.AppendEntry(*ent)
	node.replicateAllMembers()
}

/* ############################################# */

func (node *Node)send(msg *Message){
	msg.Src = node.Id
	msg.Term = node.Term
	if msg.Cmd != "AppendEntry" {
		msg.PrevIndex = node.store.LastIndex
		msg.PrevTerm = node.store.LastTerm
	}
	node.transport.Send(msg)
}

func (node *Node)broadcast(msg *Message){
	for _, m := range node.Members {
		msg.Dst = m.Id
		node.send(msg)
	}
}

func (node *Node)replicateAllMembers(){
	for _, m := range node.Members {
		node.replicateMember(m)
	}

	// allow single node mode, commit every entry immediately
	if node.Role == "leader" && len(node.Members) == 0 {
		node.store.CommitEntry(node.store.LastIndex)
	}
}
