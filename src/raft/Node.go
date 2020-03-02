package raft

import (
	"fmt"
	"log"
	"sort"
	"math/rand"
	"time"
	"strings"
	"sync"
	"encoding/json"

	"util"
)

const ElectionTimeout = 5 * 1000
const HeartbeatTimeout   = 4 * 1000 // TODO: ElectionTimeout/3
const ReplicationTimeout = 1 * 1000

type Node struct{
	Id string
	Addr string
	Role string

	// Raft persistent state
	Term int32
	VoteFor string
	Members map[string]*Member

	// volatile value
	lastApplied int64
	
	votesReceived map[string]string

	electionTimer int

	store *Helper
	xport Transport
	
	mux sync.Mutex
}

func NewNode(nodeId string, db Storage, xport Transport) *Node{
	node := new(Node)
	node.Id = nodeId
	node.Addr = xport.Addr()
	node.Role = "follower"
	node.Members = make(map[string]*Member)
	node.electionTimer = 3 * 1000

	node.xport = xport
	node.store = NewHelper(node, db)

	node.lastApplied = node.store.CommitIndex
	for nodeId, nodeAddr := range node.store.State().Members {
		node.connectMember(nodeId, nodeAddr)
	}

	return node
}

func (node *Node)AddService(svc Service){
	node.store.AddService(svc)
}

func (node *Node)Start(){
	go func() {
		const TimerInterval = 100
		ticker := time.NewTicker(TimerInterval * time.Millisecond)
		defer ticker.Stop()

		node.store.ApplyEntries()
	
		for{
			select{
			case <-ticker.C:
				node.mux.Lock()
				node.Tick(TimerInterval)
				node.mux.Unlock()
			case <-node.store.C:
				for len(node.store.C) > 0 {
					<-node.store.C
				}
				node.mux.Lock()
				node.replicateAllMembers()
				node.mux.Unlock()
			case msg := <-node.xport.C():
				node.mux.Lock()
				node.handleRaftMessage(msg)
				node.mux.Unlock()
			}
		}
	}()
}

// For testing
func (node *Node)Step(){
	node.mux.Lock()
	defer node.mux.Unlock()

	fmt.Printf("\n======= Testing: Step %s =======\n\n", node.Id)
	for {
		n := 0
		// receive
		for len(node.xport.C()) > 0 {
			msg := <-node.xport.C()
			log.Println("    receive < ", msg.Encode())
			node.handleRaftMessage(msg)
			n ++
		}
		// send
		if len(node.store.C) > 0 {
			for len(node.store.C) > 0 {
				<-node.store.C
			}
			node.replicateAllMembers()
			n ++
		}
		if n == 0 {
			break
		}
	}
	// timer
	node.Tick(900)
}

func (node *Node)Close(){
	node.store.Close()
	node.xport.Close()
}

func (node *Node)Tick(timeElapse int){
	if node.Role == "follower" || node.Role == "candidate" {
		node.electionTimer += timeElapse
		if node.electionTimer >= ElectionTimeout {
			log.Println("Election timeout")
			node.startElection()
		}
	} else if node.Role == "leader" {
		for _, m := range node.Members {
			m.ReceiveTimout += timeElapse
			m.ReplicationTimer += timeElapse
			m.HeartbeatTimer += timeElapse

			if m.ReplicationTimer >= ReplicationTimeout {
				// 如果 matchIndex == 0, 则只需要发送最新的一条 log(即 NextIndex=store.LastIndex)
				if m.MatchIndex != 0 && m.NextIndex != m.MatchIndex + 1 {
					log.Printf("resend member: %s, nextIndex: %d, matchIndex: %d", m.Id, m.NextIndex, m.MatchIndex)
					m.NextIndex = m.MatchIndex + 1
				}
				node.replicateMember(m)
			}
			if m.HeartbeatTimer >= HeartbeatTimeout {
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
	node.Role = "follower"
	node.electionTimer = 0	
	for _, m := range node.Members {
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
	node.electionTimer = rand.Intn(200)

	msg := NewRequestVoteMsg()
	for _, m := range node.Members {
		m.Role = "follower" // no one is leader
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
	}
	node.store.AddNewEntry("Noop", "")
}

/* ############################################# */

func (node *Node)resetMemberState(m *Member){
	m.Role = "follower"
	m.NextIndex = node.store.LastIndex + 1
	m.MatchIndex = 0
	m.HeartbeatTimer = 0
	m.ReplicationTimer = 0
}

func (node *Node)heartbeatMember(m *Member){
	m.HeartbeatTimer = 0
	
	ent := NewHeartbeatEntry(node.store.CommitIndex)
	prev := node.store.GetEntry(node.store.CommitIndex - 1)
	node.send(NewAppendEntryMsg(m.Id, ent, prev))
}

func (node *Node)replicateAllMembers(){
	for _, m := range node.Members {
		node.replicateMember(m)
	}
	// 单节点运行
	if len(node.Members) == 0 {
		node.store.CommitEntry(node.store.LastIndex)
	}
}

func (node *Node)replicateMember(m *Member){
	m.ReplicationTimer = 0
	if m.MatchIndex != 0 && m.NextIndex - m.MatchIndex >= m.SendWindow {
		log.Printf("stop and wait, next: %d, match: %d", m.NextIndex, m.MatchIndex)
		return
	}

	maxIndex := util.MaxInt64(m.NextIndex, m.MatchIndex + m.SendWindow)
	count := 0
	for /**/; m.NextIndex <= maxIndex; m.NextIndex++ {
		ent := node.store.GetEntry(m.NextIndex)
		if ent == nil {
			break
		}
		ent.CommitIndex = node.store.CommitIndex
		
		prev := node.store.GetEntry(m.NextIndex - 1)
		node.send(NewAppendEntryMsg(m.Id, ent, prev))

		count ++
	}
	if count > 0 {
		m.HeartbeatTimer = 0
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
	node.resetMemberState(m)
	node.Members[m.Id] = m
	node.xport.Connect(m.Id, m.Addr)
	log.Println("    connect member", m.Id, m.Addr)
}

func (node *Node)disconnectMember(nodeId string){
	if nodeId == node.Id {
		return
	}
	if node.Members[nodeId] == nil {
		return
	}
	m := node.Members[nodeId]
	delete(node.Members, nodeId)
	node.xport.Disconnect(m.Id)
	log.Println("    disconnect member", m.Id, m.Addr)
}

/* ############################################# */

func (node *Node)handleRaftMessage(msg *Message){
	if msg.Dst != node.Id || node.Members[msg.Src] == nil {
		log.Println(node.Id, "drop message src", msg.Src, "dst", msg.Dst, "members: ", node.Members)
		return
	}

	// MUST: smaller msg.Term is rejected or ignored
	if msg.Term < node.Term {
		if msg.Cmd == MessageCmdRequestVote {
			log.Println("reject", msg.Cmd, "msg.Term =", msg.Term, " < currentTerm = ", node.Term)
			node.send(NewRequestVoteAck(msg.Src, false))
		} else if msg.Cmd == MessageCmdAppendEntry {
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
		log.Printf("receive greater msg.term: %d, node.term: %d", msg.Term, node.Term)
		if node.Role == "leader" {
			arr := make([]int, 0, len(node.Members) + 1)
			arr = append(arr, 0) // self
			for _, m := range node.Members {
				arr = append(arr, m.ReceiveTimout)
			}
			sort.Ints(arr)
			log.Println("    receive timeouts =", arr)
			timer := arr[len(arr)/2]
			if timer < ElectionTimeout {
				log.Println("    major followers are still reachable, ignore")
				return
			}
		}
		
		node.Term = msg.Term
		node.VoteFor = ""
		node.store.SaveState()
		if node.Role != "follower" {
			log.Println("    became follower")
			node.becomeFollower()
		}
		// continue processing msg
	}

	if node.Role == "leader" {
		if msg.Cmd == MessageCmdAppendEntryAck {
			node.handleAppendEntryAck(msg)
		}
		return
	}
	if node.Role == "candidate" {
		if msg.Cmd == MessageCmdRequestVoteAck {
			node.handleRequestVoteAck(msg)
		}
		return
	}
	if node.Role == "follower" {
		if msg.Cmd == MessageCmdRequestVote {
			node.handleRequestVote(msg)
		} else if msg.Cmd == MessageCmdAppendEntry {
			node.handleAppendEntry(msg)
		} else if msg.Cmd == MessageCmdInstallSnapshot {
			node.handleInstallSnapshot(msg)
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
	if node.electionTimer < ElectionTimeout * 2/3 {
		for _, m := range node.Members {
			if m.Role == "leader" {
				log.Printf("leader %s is still active, ignore RequestVote from %s", m.Id, msg.Src)
				return
			}
		}
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
		node.electionTimer = 0
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
	node.electionTimer = 0
	for _, m := range node.Members {
		m.Role = "follower"
	}
	node.Members[msg.Src].Role = "leader"

	ent := DecodeEntry(msg.Data)

	if ent.Index != 1 { // if not very first entry
		prev := node.store.GetEntry(msg.PrevIndex)
		if prev == nil || prev.Term != msg.PrevTerm {
			log.Println("bad prev entry", msg.PrevIndex, msg.PrevTerm)
			node.send(NewAppendEntryAck(msg.Src, false))
			return
		}
	}

	if ent.Type == "Heartbeat" {
		//
	} else {
		old := node.store.GetEntry(ent.Index)
		if old != nil && old.Term != ent.Term {
			// TODO:
			log.Println("delete conflict entry, and entries that follow")
		}
		node.store.AppendEntry(ent)
	}

	node.send(NewAppendEntryAck(msg.Src, true))
	node.store.CommitEntry(ent.CommitIndex)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]
	m.ReceiveTimout = 0

	if msg.Data == "false" {
		if msg.PrevIndex < node.store.LastIndex {
			m.NextIndex = util.MaxInt64(1, msg.PrevIndex + 1)
			log.Println("decrease NextIndex for node", m.Id, "to", m.NextIndex)

			ent := node.store.GetEntry(m.NextIndex)
			if ent != nil {
				node.replicateMember(m)
			} else {
				node.sendInstallSnapshot(m)
			}
		}
	}else{
		oldMatchIndex := m.MatchIndex
		m.NextIndex = util.MaxInt64(m.NextIndex, msg.PrevIndex + 1)
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		if m.MatchIndex > oldMatchIndex {
			// sort matchIndex[] in descend order
			matchIndex := make([]int64, 0, len(node.Members) + 1)
			matchIndex = append(matchIndex, node.store.LastIndex) // self
			for _, m := range node.Members {
				matchIndex = append(matchIndex, m.MatchIndex)
			}
			sort.Slice(matchIndex, func(i, j int) bool{
				return matchIndex[i] > matchIndex[j]
			})
			log.Println("matchIndex[] =", matchIndex)
			commitIndex := matchIndex[len(matchIndex)/2]

			ent := node.store.GetEntry(commitIndex)
			// only commit currentTerm's log
			if ent.Term == node.Term && commitIndex > node.store.CommitIndex {
				node.store.CommitEntry(commitIndex)
			}
			
			if m.NextIndex <= node.store.LastIndex {
				node.replicateMember(m)
			} else {
				// 如果 follower 回复了最后一条 entry
				if m.MatchIndex  == node.store.LastIndex {
					// immediately notify followers to commit
					node.heartbeatMember(m)
				}
			}
		}
	}
}

// 也许 leader 只需要发送 InstallSnapshot 指令, 新节点收到后主动拉取
// Raft Snapshot 和 Service Snapshot, 而不是由 leader 推送.
// 未来可以从配置中心拉取.
func (node *Node)sendInstallSnapshot(m *Member){
	sn := node.store.MakeMemSnapshot()
	if sn == nil {
		log.Println("MakeMemSnapshot() error!")
		return
	}
	msg := NewInstallSnapshotMsg(m.Id, sn.Encode())
	node.send(msg)
}

func (node *Node)handleInstallSnapshot(msg *Message){
	sn := NewSnapshotFromString(msg.Data)
	if sn == nil {
		log.Println("NewSnapshotFromString() error!")
		return
	}
	
	log.Println("install Raft snapshot")
	for _, m := range node.Members {
		// it's ok to delete item while iterating
		node.disconnectMember(m.Id)
	}
	for nodeId, nodeAddr := range node.store.State().Members {
		node.connectMember(nodeId, nodeAddr)
	}
	node.lastApplied = sn.LastEntry().Index

	node.store.InstallSnapshot(sn)

	// TODO: copy service snapshot
	log.Println("TODO: install Service snapshot")
}

/* ###################### Quorum Methods ####################### */

func (node *Node)AddMember(nodeId string, nodeAddr string) int64 {
	node.mux.Lock()
	defer node.mux.Unlock()

	if node.Role != "leader" {
		log.Println("error: not leader")
		return -1
	}
	
	data := fmt.Sprintf("%s %s", nodeId, nodeAddr)
	ent := node.store.AddNewEntry("AddMember", data)
	return ent.Index
}

func (node *Node)DelMember(nodeId string) int64 {
	node.mux.Lock()
	defer node.mux.Unlock()

	if node.Role != "leader" {
		log.Println("error: not leader")
		return -1
	}
	
	data := nodeId
	ent := node.store.AddNewEntry("DelMember", data)
	return ent.Index
}

func (node *Node)Write(data string) int64 {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	if node.Role != "leader" {
		log.Println("error: not leader")
		return -1
	}
	
	ent := node.store.AddNewEntry("Write", data)
	return ent.Index
}

/* ###################### Service interface ####################### */

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
			node.store.SaveState()
		}
	}else if ent.Type == "DelMember" {
		log.Println("[Apply]", ent.Encode())
		nodeId := ent.Data
		// the deleted node would not receive a commit msg that it had been deleted
		node.disconnectMember(nodeId)
		node.store.SaveState()
	}
}

/* ###################### Operations ####################### */

func (node *Node)InfoMap() map[string]string {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	m := make(map[string]string)
	m["id"] = fmt.Sprintf("%d", node.Id)
	m["addr"] = node.Addr
	m["role"] = node.Role
	m["term"] = fmt.Sprintf("%d", node.Term)
	m["lastApplied"] = fmt.Sprintf("%d", node.lastApplied)
	m["commitIndex"] = fmt.Sprintf("%d", node.store.CommitIndex)
	m["lastTerm"] = fmt.Sprintf("%d", node.store.LastTerm)
	m["lastIndex"] = fmt.Sprintf("%d", node.store.LastIndex)
	b, _ := json.Marshal(node.Members)
	m["members"] = string(b)
	return m
}

func (node *Node)Info() string {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	var ret string
	ret += fmt.Sprintf("id: %s\n", node.Id)
	ret += fmt.Sprintf("addr: %s\n", node.Addr)
	ret += fmt.Sprintf("role: %s\n", node.Role)
	ret += fmt.Sprintf("term: %d\n", node.Term)
	ret += fmt.Sprintf("lastApplied: %d\n", node.lastApplied)
	ret += fmt.Sprintf("commitIndex: %d\n", node.store.CommitIndex)
	ret += fmt.Sprintf("lastTerm: %d\n", node.store.LastTerm)
	ret += fmt.Sprintf("lastIndex: %d\n", node.store.LastIndex)
	
	b, _ := json.Marshal(node.Members)
	ret += fmt.Sprintf("members: %s\n", string(b))

	return ret
}

func (node *Node)JoinGroup(nodeId string, nodeAddr string){
	node.mux.Lock()
	defer node.mux.Unlock()
	
	log.Println("JoinGroup", nodeId, nodeAddr)
	if nodeId == node.Id {
		log.Println("could not join self:", nodeId)
		return
	}
	// reset Raft state
	node.Term = 0
	node.VoteFor = ""
	node.Members = make(map[string]*Member)
	node.store.CommitIndex = 0
	node.store.SaveState()

	node.becomeFollower()
	node.connectMember(nodeId, nodeAddr)
	
	// TODO: delete raft log entries
}

func (node *Node)ChangeMode() {
}

func (node *Node)Helper() *Helper {
	return node.store
}

/* ############################################# */

func (node *Node)send(msg *Message){
	msg.Src = node.Id
	msg.Term = node.Term
	if msg.Cmd != MessageCmdAppendEntry {
		msg.PrevIndex = node.store.LastIndex
		msg.PrevTerm = node.store.LastTerm
	}
	node.xport.Send(msg)
}
