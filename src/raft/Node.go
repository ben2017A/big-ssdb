package raft

import (
	"fmt"
	"log"
	"sort"
	"math/rand"
	"strings"

	"util"
)

const ElectionTimeout = 5 * 1000
const HeartbeatTimeout   = 4 * 1000

const ReplicationTimeout = 1 * 1000

type Node struct{
	Id string
	Role string

	Term int32
	lastApplied int64

	Members map[string]*Member

	VoteFor string
	votesReceived map[string]string

	electionTimeout int

	store *Helper
	xport Transport
}

func NewNode(nodeId string, db Storage, xport Transport) *Node{
	node := new(Node)
	node.Id = nodeId
	node.Role = "follower"
	node.Term = 0
	node.Members = make(map[string]*Member)
	node.electionTimeout = 3 * 1000

	node.xport = xport
	node.store = NewHelper(node, db)
	node.lastApplied = node.store.CommitIndex

	if node.store.State().Id != "" {
		node.Term = node.store.State().Term
		node.VoteFor = node.store.State().VoteFor
	}

	return node
}

func (node *Node)Start(){
	for nodeId, nodeAddr := range node.store.State().Members {
		node.connectMember(nodeId, nodeAddr)
	}
	node.store.ApplyEntries()
}

func (node *Node)Stop(){
	node.store.Close()
	node.xport.Close()
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
	node.electionTimeout = ElectionTimeout // debug TODO:
	
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

	msg := NewRequestVoteMsg()
	for _, m := range node.Members {
		msg.Dst = m.Id
		node.send(msg)
	}

	// 单节点运行
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
		node.store.AddEntry(*ent)
		node.replicateAllMembers()
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
	if m.NextIndex - m.MatchIndex >= m.SendWindow {
		if m.MatchIndex != 0 {
			log.Println("stop and wait")
		}
		return
	}

	count := 0
	maxIndex := m.MatchIndex + m.SendWindow
	for m.NextIndex <= maxIndex {
		ent := node.store.GetEntry(m.NextIndex)
		if ent == nil {
			break
		}
		
		ent.CommitIndex = node.store.CommitIndex
		prev := node.store.GetEntry(m.NextIndex - 1)
		node.send(NewAppendEntryMsg(m.Id, ent, prev))

		count ++
		m.NextIndex += 1
	}
	if count > 0 {
		m.HeartbeatTimeout = HeartbeatTimeout
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

func (node *Node)connectMember(nodeId string, nodeAddr string){
	if nodeId == node.Id {
		return
	}
	if node.Members[nodeId] != nil {
		return
	}
	m := NewMember(nodeId, nodeAddr)
	node.Members[m.Id] = m
	node.resetMemberState(m)
	log.Println("    connect member", nodeId, nodeAddr)
	node.xport.Connect(m.Id, m.Addr)
}

/* ############################################# */

func (node *Node)HandleRaftMessage(msg *Message){
	if msg.Dst != node.Id || node.Members[msg.Src] == nil {
		log.Println(node.Id, "drop message src", msg.Src, "dst", msg.Dst, "members: ", node.Members)
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

	if ent.Type == "Heartbeat" || ent.Type == "Commit" {
		//
	} else {
		old := node.store.GetEntry(ent.Index)
		if old != nil && old.Term != ent.Term {
			// TODO:
			log.Println("delete conflict entry, and entries that follow")
		}
		node.store.AddEntry(*ent)
	}

	node.send(NewAppendEntryAck(msg.Src, true))
	node.store.CommitEntry(ent.CommitIndex)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]

	if msg.Data == "false" {
		if msg.PrevIndex < node.store.LastIndex {
			m.NextIndex = util.MaxInt64(1, msg.PrevIndex + 1)
			log.Println("decrease NextIndex for node", m.Id, "to", m.NextIndex)
			
			node.replicateMember(m)
		}
	}else{
		oldMatchIndex := m.MatchIndex
		m.NextIndex = util.MaxInt64(m.NextIndex, msg.PrevIndex + 1)
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		if m.MatchIndex > oldMatchIndex {
			// sort matchIndex[] in descend order
			matchIndex := make([]int64, 0, len(node.Members) + 1)
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
				
				// immediately notify followers to commit
				if m.NextIndex >= node.store.LastIndex {
					node.heartbeatMember(m)
				} else{
					node.replicateMember(m)
				}
			}
		}
	}
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
	node.connectMember(nodeId, nodeAddr)
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
	node.store.AddEntry(*ent)
	node.replicateAllMembers()	
}

func (node *Node)Write(data string) int64{
	ent := node.newEntry("Write", data)
	node.store.AddEntry(*ent)
	node.replicateAllMembers()
	return ent.Index
}

func (node *Node)AddService(svc Service){
	node.store.AddService(svc)
}

/* #################### Service interface ######################### */

func (node *Node)LastApplied() int64{
	return node.lastApplied
}

func (node *Node)ApplyEntry(ent *Entry){
	node.lastApplied = ent.Index

	// 注意, 不能在 ApplyEntry 里修改 CommitIndex
	if ent.Type == "AddMember" {
		log.Println("[Apply]", ent.Encode())
		ps := strings.Split(ent.Data, " ")
		if len(ps) == 2 {
			node.connectMember(ps[0], ps[1])
		}
	}else if ent.Type == "DelMember" {
		// TODO:
	}else{
		// log.Println("[Apply]", ent.Encode())
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
