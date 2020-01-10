package raft

import (
	"log"
	"sort"
	"math/rand"
)

const ElectionTimeout = 5 * 1000
const KeepaliveTimeout   = 3 * 1000

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

	lastEntry *Entry
	entries map[uint64]*Entry

	Transport Transport
}

func NewNode() *Node{
	node := new(Node)
	node.Term = 0
	node.Members = make(map[string]*Member)

	node.lastEntry = new(Entry)
	node.entries = make(map[uint64]*Entry)

	node.electionTimeout = ElectionTimeout + rand.Intn(100)

	return node
}

func (node *Node)becomeFollower(){
	log.Println("convert", node.Role, " => follower")
	node.Role = "follower"
	node.voteFor = ""
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

	for _, m := range node.Members {
		// discover leader by receiving valid AppendEntry
		m.Role = "follower"
	}
}

func (node *Node)becomeCandidate(){
	log.Println("convert", node.Role, " => candidate")
	node.Role = "candidate"
	node.Term += 1
	node.voteFor = node.Id
	node.votesReceived = make(map[string]string)

	for _, m := range node.Members {
		msg := new(Message)
		msg.Cmd = "RequestVote"
		msg.Src = node.Id
		msg.Dst = m.Id
		msg.Term = node.Term;
		msg.PrevIndex = node.lastEntry.Index
		msg.PrevTerm = node.lastEntry.Term
		msg.Data = "please vote me"
		node.Transport.Send(msg)
	}

	node.requestVoteTimeout = RequestVoteTimeout
}

func (node *Node)becomeLeader(){
	log.Println("convert", node.Role, " => leader")
	node.Role = "leader"
	node.voteFor = ""

	for _, m := range node.Members {
		m.NextIndex = node.lastEntry.Index + 1
		m.MatchIndex = 0
		m.KeepaliveTimeout = 0
		m.ReplicationTimeout = 0
	}
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "candidate" {
		node.requestVoteTimeout -= timeElapse
		if node.requestVoteTimeout <= 0 {
			log.Println("RequestVote timeout")
			node.becomeFollower()
			return
		}
		if len(node.votesReceived) + 1 > (len(node.Members) + 1)/2 {
			log.Println("Got majority votes")
			node.becomeLeader()
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

			m.KeepaliveTimeout -= timeElapse
			if m.KeepaliveTimeout <= 0 {
				log.Println("Keepalive timeout for node", m.Id)
				node.keepaliveMember(m)
			}
		}
	}
}

/* ############################################# */

func (node *Node)HandleRaftMessage(msg *Message){
	if msg.Term < node.Term {
		// TODO: false Ack
		log.Println("ignore msg.Term =", msg.Term, " < currentTerm = ", node.Term)
		return
	}

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
	if node.voteFor == "" {
		if msg.Term == node.Term && msg.PrevIndex >= node.lastEntry.Index {
			log.Println("vote for", msg.Src)
			node.voteFor = msg.Src
			node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

			ack := new(Message)
			ack.Cmd = "RequestVoteAck"
			ack.Src = node.Id
			ack.Dst = msg.Src
			ack.Term = node.Term;
			ack.PrevIndex = node.lastEntry.Index
			ack.PrevTerm = node.lastEntry.Term
			ack.Data = ""
			node.Transport.Send(ack)
		}
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	if msg.Term == node.Term && msg.PrevIndex <= node.lastEntry.Index {
		log.Println("receive ack from", msg.Src)
		node.votesReceived[msg.Src] = "ok"
	}
}

func (node *Node)sendAppendEntryAck(leaderId string, success bool){
	ack := new(Message)
	ack.Cmd = "AppendEntryAck"
	ack.Src = node.Id
	ack.Dst = leaderId
	ack.Term = node.Term;
	ack.PrevIndex = node.lastEntry.Index
	ack.PrevTerm = node.lastEntry.Term
	if success {
		ack.Data = "true"
	}else{
		ack.Data = "false"
	}
	node.Transport.Send(ack)
}

func (node *Node)handleAppendEntry(msg *Message){
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
	node.Members[msg.Src].Role = "leader"

	if msg.PrevIndex > 0 && msg.PrevTerm > 0 {
		prev := node.entries[msg.PrevIndex]
		if prev == nil || prev.Term != msg.PrevTerm {
			node.sendAppendEntryAck(msg.Src, false)
			return
		}
	}

	if msg.Data == "Keepalive" {
		return
	}

	entry := DecodeEntry(msg.Data)

	old := node.entries[entry.Index]
	if old != nil && old.Term != entry.Term {
		log.Println("delete conflict entry")
		delete(node.entries, entry.Index)
	}

	if node.entries[entry.Index] == nil {
		node.entries[entry.Index] = entry
	}

	for{
		// increase lastLogIndex by 1, in case there are gaps between entries received
		next := node.entries[node.lastEntry.Index + 1]
		if next == nil {
			break;
		}
		node.lastEntry = next
	}

	node.sendAppendEntryAck(msg.Src, true)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]

	if msg.Data == "false" {
		m.NextIndex -= 1
		if m.NextIndex < 1{
			m.NextIndex = 1
		}
		m.MatchIndex = 0
		log.Println("decrease NextIndex for node", m.Id, "to", m.NextIndex)
	}else{
		if m.NextIndex <= msg.PrevIndex {
			m.NextIndex = msg.PrevIndex + 1
		}
		if m.MatchIndex < msg.PrevIndex {
			m.MatchIndex = msg.PrevIndex
		}
	
		// commit log
		// sort matchIndex[] in descend order, commitIndex = matchIndex[(n+1)/2]
		matchIndex := make([]uint64, len(node.Members))
		for _, m := range node.Members {
			matchIndex = append(matchIndex, m.MatchIndex)
		}
		sort.Slice(matchIndex, func(i, j int) bool{
			return matchIndex[i] > matchIndex[j]
		})
		log.Println(matchIndex)
		// commitIndex := matchIndex[(len(matchIndex)+1)/2]
	}

	// node.replicateMember(m)
}

/* ############################################# */

func (node *Node)Write(data string){
	entry := new(Entry)
	entry.Index = node.lastEntry.Index + 1
	entry.Term = node.Term
	entry.Data = data

	log.Println("WAL.write", entry.Index, entry.Term, entry.Data)
	node.entries[entry.Index] = entry
	node.lastEntry = entry

	for _, m := range node.Members {
		node.replicateMember(m)
	}
}

func (node *Node)keepaliveMember(m *Member){
	m.KeepaliveTimeout = KeepaliveTimeout

	prev := node.entries[m.NextIndex - 1]

	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Src = node.Id
	msg.Dst = m.Id
	msg.Term = node.Term
	if prev != nil {
		msg.PrevIndex = prev.Index
		msg.PrevTerm = prev.Term
	}
	msg.Data = "Keepalive"
	node.Transport.Send(msg)
}

func (node *Node)replicateMember(m *Member){
	m.ReplicationTimeout = ReplicationTimeout

	next := node.entries[m.NextIndex]
	if next == nil {
		return;
	}
	prev := node.entries[m.NextIndex - 1]

	msg := new(Message)
	msg.Cmd = "AppendEntry"
	msg.Src = node.Id
	msg.Dst = m.Id
	msg.Term = node.Term
	if prev != nil {
		msg.PrevIndex = prev.Index
		msg.PrevTerm = prev.Term
	}
	msg.Data = next.Encode()
	node.Transport.Send(msg)

	m.KeepaliveTimeout = KeepaliveTimeout
}
