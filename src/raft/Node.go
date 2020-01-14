package raft

import (
	"fmt"
	"log"
	"sort"
	"math/rand"
	"strings"

	"myutil"
)

const ElectionTimeout = 5 * 1000
const HeartbeatTimeout   = 4 * 1000

const ReplicationTimeout = 1 * 1000

type Node struct{
	Id string
	Role string
	Addr string

	Term uint32
	lastApplied uint64

	Members map[string]*Member

	VoteFor string
	votesReceived map[string]string

	electionTimeout int

	store *Storage
	xport Transport
}

func NewNode(store *Storage, xport Transport) *Node{
	node := new(Node)
	node.Role = "follower"
	node.Term = 0
	node.Members = make(map[string]*Member)
	node.electionTimeout = ElectionTimeout

	node.store = store
	node.xport = xport

	store.SetNode(node)

	return node
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "follower" || node.Role == "candidate" {
		node.electionTimeout -= timeElapse
		if node.electionTimeout <= 0 {
			log.Println("Election timeout")
			node.startElection()
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
	if node.Role == "follower" {
		return
	}
	log.Println("convert", node.Role, "=> follower")
	node.Role = "follower"
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

	for _, m := range node.Members {
		// discover leader by receiving valid AppendEntry
		m.Role = "follower"
	}
}

func (node *Node)startElection(){
	if node.Role != "candidate" {
		log.Println("convert", node.Role, "=> candidate")
	}
	node.Role = "candidate"
	node.Term += 1
	node.VoteFor = node.Id
	node.store.SaveState()

	node.votesReceived = make(map[string]string)
	node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)

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

	// write noop entry with currentTerm
	if len(node.Members) > 0 {
		for _, m := range node.Members {
			node.resetMemberState(m)
		}
		ent := node.newEntry("Noop", "")
		node.store.AppendEntry(*ent)
		for _, m := range node.Members {
			node.replicateMember(m)
		}
	}
}

/* ############################################# */

func (node *Node)resetMemberState(m *Member){
	m.Role = "follower"
	m.NextIndex = node.store.LastIndex + 1
	m.MatchIndex = 0
	m.HeartbeatTimeout = HeartbeatTimeout
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

	if m.ResendIndex == ent.Index {
		m.ResendCount ++
		m.ReplicationTimeout = myutil.MinInt(m.ResendCount * ReplicationTimeout, HeartbeatTimeout * 2)
		// log.Println("retransmission", m.ResendCount)
	}else{
		m.ResendCount = 0
	}
	m.ResendIndex = ent.Index
	m.HeartbeatTimeout = HeartbeatTimeout
}

func (node *Node)ConnectMember(nodeId string, nodeAddr string){
	if nodeId == node.Id {
		node.Addr = nodeAddr
	} else {
		m := NewMember(nodeId, nodeAddr)
		node.Members[m.Id] = m
		node.resetMemberState(m)
		node.xport.Connect(m.Id, m.Addr)
	}
	log.Println("    connect member", nodeId, nodeAddr)
}

/* ############################################# */

func (node *Node)HandleRaftMessage(msg *Message){
	if msg.Dst != node.Id || node.Members[msg.Src] == nil {
		log.Println("drop message src", msg.Src, "dst", msg.Dst)
		return
	}

	// MUST: smaller msg.Term is rejected or ignored
	if msg.Term < node.Term {
		if msg.Cmd == "RequestVote" {
			log.Println("reject", msg.Cmd, "msg.Term =", msg.Term, " < currentTerm = ", node.Term)
			node.send(NewRequestVoteAck(msg.Src, false))
		} else if msg.Cmd == "AppendEntry" {
			log.Println("reject", msg.Cmd, "msg.Term =", msg.Term, " < currentTerm = ", node.Term)
			node.send(NewAppendEntryAck(msg.Src, false))
		} else {
			log.Println("ignore", msg.Cmd, "msg.Term =", msg.Term, " < currentTerm = ", node.Term)
		}
		// finish processing msg
		return
	}
	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term {
		node.Term = msg.Term
		node.VoteFor = ""
		node.store.SaveState()
		node.becomeFollower()
		// continue processing msg
	}

	if node.Role == "leader" {
		if msg.Cmd == "AppendEntryAck" {
			node.handleAppendEntryAck(msg)
		}
		return
	}
	if node.Role == "candidate" {
		if msg.Cmd == "RequestVoteAck" {
			node.handleRequestVoteAck(msg)
		}
		return
	}
	if node.Role == "follower" {
		if msg.Cmd == "RequestVote" {
			node.handleRequestVote(msg)
		} else if msg.Cmd == "AppendEntry" {
			node.handleAppendEntry(msg)
		}
		return
	}
}

func (node *Node)handleRequestVote(msg *Message){
	// node.VoteFor == msg.Src: retransimitted/duplicated RequestVote
	if node.VoteFor != "" && node.VoteFor != msg.Src {
		// just ignore
		log.Println("already vote for", node.VoteFor, "ignore", msg.Src)
		return
	}
	granted := false
	if msg.PrevTerm > node.store.LastTerm {
		granted = true
	} else if msg.PrevTerm == node.store.LastTerm && msg.PrevIndex >= node.store.LastIndex {
		granted = true
	} else {
		// we've got newer log, reject
	}

	if granted {
		node.electionTimeout = ElectionTimeout + rand.Intn(ElectionTimeout/2)
		log.Println("vote for", msg.Src)
		node.VoteFor = msg.Src
		node.store.SaveState()
		node.send(NewRequestVoteAck(msg.Src, true))
	} else {
		node.send(NewRequestVoteAck(msg.Src, false))
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	granted := (msg.Data == "true")
	if granted {
		log.Println("receive vote grant from", msg.Src)
		node.votesReceived[msg.Src] = "true"
		node.checkVoteResult()
	} else {
		log.Println("receive vote reject from", msg.Src)
		node.becomeFollower()
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

	if ent.Type == "Heartbeat" {
		node.send(NewAppendEntryAck(msg.Src, true))
	} else {
		old := node.store.GetEntry(ent.Index)
		if old != nil && old.Term != ent.Term {
			// TODO:
			log.Println("delete conflict entry, and entries that follow")
		}
		node.store.AppendEntry(*ent)
		node.send(NewAppendEntryAck(msg.Src, true))
	}

	if ent.CommitIndex > node.store.CommitIndex {
		commitIndex := myutil.MinU64(ent.CommitIndex, node.store.LastIndex)
		node.store.CommitEntry(commitIndex)
	}
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]

	if msg.Data == "false" {
		m.NextIndex = myutil.MaxU64(1, m.NextIndex - 1)
		m.MatchIndex = 0
		log.Println("decrease NextIndex for node", m.Id, "to", m.NextIndex)
	}else{
		oldMatchIndex := m.MatchIndex
		m.NextIndex = myutil.MaxU64(m.NextIndex, msg.PrevIndex + 1)
		m.MatchIndex = myutil.MaxU64(m.MatchIndex, msg.PrevIndex)
		if m.MatchIndex > oldMatchIndex {
			// sort matchIndex[] in descend order
			matchIndex := make([]uint64, 0, len(node.Members) + 1)
			matchIndex = append(matchIndex, node.store.LastIndex)
			for _, m := range node.Members {
				matchIndex = append(matchIndex, m.MatchIndex)
			}
			sort.Slice(matchIndex, func(i, j int) bool{
				return matchIndex[i] > matchIndex[j]
			})
			log.Println(matchIndex)
			commitIndex := matchIndex[len(matchIndex)/2]

			ent := node.store.GetEntry(commitIndex)
			// only commit currentTerm's log
			if ent.Term == node.Term && commitIndex > node.store.CommitIndex {
				node.store.CommitEntry(commitIndex)
				// TODO: notify followers to commit
			}
		}
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

func (node *Node)JoinGroup(nodeId string, nodeAddr string){
	log.Println("JoinGroup", nodeId, nodeAddr)
	if nodeId == node.Id {
		log.Println("could not join self:", nodeId)
		return
	}
	node.ConnectMember(nodeId, nodeAddr)
	node.store.SaveState()
	node.becomeFollower()
}

func (node *Node)AddMember(nodeId string, nodeAddr string){
	if node.Members[nodeId] != nil {
		log.Println("member already exists:", nodeId)
		return
	}
	data := fmt.Sprintf("%s %s", nodeId, nodeAddr)
	ent := node.newEntry("AddMember", data)
	node.store.AppendEntry(*ent)
	node.replicateEntries()	
}

func (node *Node)Write(data string){
	ent := node.newEntry("Write", data)
	node.store.AppendEntry(*ent)
	node.replicateEntries()
}

func (node *Node)replicateEntries(){
	for _, m := range node.Members {
		node.replicateMember(m)
	}
	// allow single node mode, commit every entry immediately
	if node.Role == "leader" && len(node.Members) == 0 {
		node.store.CommitEntry(node.store.LastIndex)
	}
}

/* #################### Subscriber interface ######################### */

func (node *Node)LastApplied() uint64{
	return node.lastApplied
}

func (node *Node)ApplyEntry(ent *Entry){
	node.lastApplied = ent.Index

	if ent.Type == "AddMember" {
		log.Println("[Apply]", ent.Encode())
		ps := strings.Split(ent.Data, " ")
		node.ConnectMember(ps[0], ps[1])
		node.store.SaveState()
	}else if ent.Type == "DelMember" {
		//
	}else if ent.Type == "Write"{
		log.Println("[Apply]", ent.Encode())
	}
}

/* ############################################# */

func (node *Node)send(msg *Message){
	msg.Src = node.Id
	msg.Term = node.Term
	if msg.Cmd != "AppendEntry" {
		msg.PrevIndex = node.store.LastIndex
		msg.PrevTerm = node.store.LastTerm
	}
	node.xport.Send(msg)
}

func (node *Node)broadcast(msg *Message){
	for _, m := range node.Members {
		msg.Dst = m.Id
		node.send(msg)
	}
}
