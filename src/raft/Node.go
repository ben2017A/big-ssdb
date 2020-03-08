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

type RoleType string

const(
	RoleLeader      = "leader"
	RoleFollower    = "follower"
	RoleCandidate   = "candidate"
)

const(
	ElectionTimeout    = 5 * 1000
	HeartbeatTimeout   = 4 * 1000 // TODO: ElectionTimeout/3
	ReplicationTimeout = 1 * 1000
	ReceiveTimeout     = HeartbeatTimeout * 3
)

type Node struct{
	Id string
	Addr string
	Role RoleType

	// Raft persistent state
	Term int32
	VoteFor string
	Members map[string]*Member

	// volatile value
	lastApplied int64
	
	votesReceived map[string]string

	electionTimer int

	store *Storage
	xport Transport
	
	mux sync.Mutex
}

func NewNode(nodeId string, store *Storage, xport Transport) *Node{
	node := new(Node)
	node.Id = nodeId
	node.Addr = xport.Addr()
	node.Role = RoleFollower
	node.Members = make(map[string]*Member)
	node.electionTimer = 3 * 1000

	node.xport = xport
	node.store = store
	store.SetNode(node)

	// init Raft state from persistent storage
	node.lastApplied = store.CommitIndex
	node.Term = store.State().Term
	node.VoteFor = store.State().VoteFor
	for nodeId, nodeAddr := range store.State().Members {
		node.connectMember(nodeId, nodeAddr)
	}

	log.Printf("init raft node[%s]:", node.Id)
	log.Println("    CommitIndex:", store.CommitIndex, "LastTerm:", store.LastTerm, "LastIndex:", store.LastIndex)
	log.Println("    " + store.State().Encode())

	return node
}

func (node *Node)SetService(svc Service){
	node.store.Service = svc
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
	if node.Role == RoleFollower || node.Role == RoleCandidate {
		node.electionTimer += timeElapse
		if node.electionTimer >= ElectionTimeout {
			log.Println("start PreVote")
			node.startPreVote()
		}
	} else if node.Role == RoleLeader {
		for _, m := range node.Members {
			m.ReceiveTimeout += timeElapse
			m.ReplicateTimer += timeElapse
			m.HeartbeatTimer += timeElapse

			if m.ReplicateTimer >= ReplicationTimeout {
				if m.NextIndex != m.MatchIndex + 1 {
					log.Printf("resend member: %s, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
					m.NextIndex = m.MatchIndex + 1
				}
				node.replicateMember(m)
			}
			if m.HeartbeatTimer >= HeartbeatTimeout {
				// log.Println("Heartbeat timeout for node", m.Id)
				node.pingMember(m)
			}
		}
	}
}

func (node *Node)startPreVote(){
	node.electionTimer = 0
	node.votesReceived = make(map[string]string)
	node.broadcast(NewPreVoteMsg())
	
	// 单节点运行
	if len(node.Members) == 0 {
		node.startElection()
	}
}

func (node *Node)startElection(){
	node.electionTimer = rand.Intn(200)
	node.votesReceived = make(map[string]string)

	node.Role = RoleCandidate
	node.Term += 1
	node.VoteFor = node.Id
	node.store.SaveState()

	node.resetAllMember()
	node.broadcast(NewRequestVoteMsg())
	
	// 单节点运行
	if len(node.Members) == 0 {
		node.checkVoteResult()
	}
}

func (node *Node)checkVoteResult(){
	count := 1
	for _, res := range node.votesReceived {
		if res == "grant" {
			count ++
		}
	}
	if count > (len(node.Members) + 1)/2 {
		log.Printf("Node %s became leader", node.Id)
		node.becomeLeader()
	}
}

func (node *Node)becomeFollower(){
	if node.Role == RoleFollower {
		return
	}
	node.Role = RoleFollower
	node.electionTimer = 0	
	node.resetAllMember()
}

func (node *Node)becomeLeader(){
	node.Role = RoleLeader
	node.resetAllMember()
	// write noop entry with currentTerm to implictly commit previous term's log
	if node.store.LastIndex == 0 || node.store.LastIndex != node.store.CommitIndex {
		node.store.AddNewEntry(EntryTypeNoop, "")
	} else {
		for _, m := range node.Members {
			node.pingMember(m)
		}
	}
}

/* ############################################# */

func (node *Node)resetAllMember(){
	for _, m := range node.Members {
		node.resetMember(m)
	}
}

func (node *Node)resetMember(m *Member){
	m.Reset()
	m.Role = RoleFollower
	m.NextIndex = node.store.LastIndex + 1
}

func (node *Node)pingMember(m *Member){
	m.HeartbeatTimer = 0
	
	ent := NewPingEntry(node.store.CommitIndex)
	prev := node.store.GetEntry(node.store.LastIndex)
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
	if m.MatchIndex != 0 && m.NextIndex - m.MatchIndex >= m.SendWindow {
		log.Printf("stop and wait %s, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
		return
	}
	if m.ReceiveTimeout >= ReceiveTimeout && m.NextIndex != m.MatchIndex + 1 {
		log.Printf("replicate %s timeout, %d", m.Id, m.ReceiveTimeout)
		return
	}

	m.ReplicateTimer = 0
	maxIndex := util.MaxInt64(m.NextIndex, m.MatchIndex + m.SendWindow)
	count := 0
	for m.NextIndex <= maxIndex {
		ent := node.store.GetEntry(m.NextIndex)
		if ent == nil {
			break
		}
		ent.CommitIndex = node.store.CommitIndex
		
		prev := node.store.GetEntry(m.NextIndex - 1)
		node.send(NewAppendEntryMsg(m.Id, ent, prev))
		
		m.NextIndex ++
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
	node.resetMember(m)
	node.Members[m.Id] = m
	node.xport.Connect(m.Id, m.Addr)
	log.Println("    connect member", m.Id, m.Addr)
}

func (node *Node)disconnectAllMember(){
	for _, m := range node.Members {
		// it's ok to delete item while iterating
		node.disconnectMember(m.Id)
	}
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
		if msg.Type == MessageTypeRequestVote {
			log.Println("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term)
			node.send(NewRequestVoteAck(msg.Src, false))
		} else if msg.Type == MessageTypeAppendEntry {
			log.Println("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term)
			node.send(NewAppendEntryAck(msg.Src, false))
		} else {
			log.Println("ignore", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term)
		}
		// finish processing msg
		return
	}
	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term {
		log.Printf("receive greater msg.term: %d, node.term: %d", msg.Term, node.Term)
		node.Term = msg.Term
		node.VoteFor = ""
		if node.Role != RoleFollower {
			log.Printf("Node %s became follower", node.Id)
			node.becomeFollower()
		}
		node.store.SaveState()
		// continue processing msg
	}

	if node.Role == RoleLeader {
		if msg.Type == MessageTypeAppendEntryAck {
			node.handleAppendEntryAck(msg)
		} else if msg.Type == MessageTypePreVote {
			node.handlePreVote(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}
	if node.Role == RoleCandidate {
		if msg.Type == MessageTypeRequestVoteAck {
			node.handleRequestVoteAck(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}
	if node.Role == RoleFollower {
		if msg.Type == MessageTypeRequestVote {
			node.handleRequestVote(msg)
		} else if msg.Type == MessageTypeAppendEntry {
			node.handleAppendEntry(msg)
		} else if msg.Type == MessageTypeInstallSnapshot {
			node.handleInstallSnapshot(msg)
		} else if msg.Type == MessageTypePreVote {
			node.handlePreVote(msg)
		} else if msg.Type == MessageTypePreVoteAck {
			node.handlePreVoteAck(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}
}

func (node *Node)handlePreVote(msg *Message){
	if node.Role == RoleLeader {
		arr := make([]int, 0, len(node.Members) + 1)
		arr = append(arr, 0) // self
		for _, m := range node.Members {
			arr = append(arr, m.ReceiveTimeout)
		}
		sort.Ints(arr)
		log.Println("    receive timeouts =", arr)
		timer := arr[len(arr)/2]
		if timer < ReceiveTimeout {
			log.Println("    major followers are still reachable, ignore")
			return
		}
	}
	for _, m := range node.Members {
		if m.Role == RoleLeader && m.ReceiveTimeout < ReceiveTimeout {
			log.Printf("leader %s is still active, ignore PreVote from %s", m.Id, msg.Src)
			return
		}
	}
	node.send(NewPreVoteAck(msg.Src))
}

func (node *Node)handlePreVoteAck(msg *Message){
	log.Printf("receive PreVoteAck from %s", msg.Src)
	node.votesReceived[msg.Src] = msg.Data
	if len(node.votesReceived) + 1 > (len(node.Members) + 1)/2 {
		node.startElection()
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
	log.Printf("receive vote %s from %s", msg.Data, msg.Src)
	node.votesReceived[msg.Src] = msg.Data
	node.checkVoteResult()
}

func (node *Node)handleAppendEntry(msg *Message){
	node.electionTimer = 0
	m := node.Members[msg.Src]
	m.ReceiveTimeout = 0
	for _, m := range node.Members {
		m.Role = RoleFollower
	}
	m.Role = RoleLeader

	ent := DecodeEntry(msg.Data)

	if ent.Index != 1 { // if not very first entry
		prev := node.store.GetEntry(msg.PrevIndex)
		if prev == nil || prev.Term != msg.PrevTerm {
			if prev == nil {
				log.Println("prev entry not found", msg.PrevTerm, msg.PrevIndex)
			} else {
				log.Printf("prev.Term %d != msg.PrevTerm %d", prev.Term, msg.PrevTerm)
			}
			node.send(NewAppendEntryAck(msg.Src, false))
			return
		}
	}

	if ent.Type == EntryTypePing {
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
	m.ReceiveTimeout = 0

	if msg.Data == "false" {
		if msg.PrevIndex == 0 {
			node.sendInstallSnapshot(m)
			return
		}
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
					node.pingMember(m)
				}
			}
		}
	}
}

func (node *Node)sendInstallSnapshot(m *Member){
	sn := node.store.CreateSnapshot()
	if sn == nil {
		log.Println("CreateSnapshot() error!")
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
	node._installSnapshot(sn)
	node.send(NewAppendEntryAck(msg.Src, true))
	
	// TODO: copy service snapshot
	log.Println("TODO: install Service snapshot")
}

func (node *Node)_installSnapshot(sn *Snapshot) bool {
	log.Println("install Raft snapshot")
	node.disconnectAllMember()
	for nodeId, nodeAddr := range sn.State().Members {
		node.connectMember(nodeId, nodeAddr)
	}
	node.lastApplied = sn.LastEntry().Index

	return node.store.InstallSnapshot(sn)
}

/* ###################### Service interface ####################### */

func (node *Node)LastApplied() int64{
	return node.lastApplied
}

func (node *Node)ApplyEntry(ent *Entry){
	node.lastApplied = ent.Index

	// 注意, 不能在 ApplyEntry 里修改 CommitIndex
	if ent.Type == EntryTypeAddMember {
		log.Println("[Apply]", ent.Encode())
		ps := strings.Split(ent.Data, " ")
		if len(ps) == 2 {
			node.connectMember(ps[0], ps[1])
			node.store.SaveState()
		}
	}else if ent.Type == EntryTypeDelMember {
		log.Println("[Apply]", ent.Encode())
		nodeId := ent.Data
		// the deleted node would not receive a commit msg that it had been deleted
		node.disconnectMember(nodeId)
		node.store.SaveState()
	}
}

/* ###################### Quorum Methods ####################### */

func (node *Node)AddMember(nodeId string, nodeAddr string) int64 {
	node.mux.Lock()
	defer node.mux.Unlock()

	if node.Role != RoleLeader {
		log.Println("error: not leader")
		return -1
	}
	
	data := fmt.Sprintf("%s %s", nodeId, nodeAddr)
	ent := node.store.AddNewEntry(EntryTypeAddMember, data)
	return ent.Index
}

func (node *Node)DelMember(nodeId string) int64 {
	node.mux.Lock()
	defer node.mux.Unlock()

	if node.Role != RoleLeader {
		log.Println("error: not leader")
		return -1
	}
	
	data := nodeId
	ent := node.store.AddNewEntry(EntryTypeDelMember, data)
	return ent.Index
}

func (node *Node)Write(data string) int64 {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	if node.Role != RoleLeader {
		log.Println("error: not leader")
		return -1
	}
	
	ent := node.store.AddNewEntry(EntryTypeData, data)
	return ent.Index
}

/* ###################### Operations ####################### */

func (node *Node)InfoMap() map[string]string {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	m := make(map[string]string)
	m["id"] = fmt.Sprintf("%d", node.Id)
	m["addr"] = node.Addr
	m["role"] = string(node.Role)
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
	ret += fmt.Sprintf("electionTimer: %d\n", node.electionTimer)
	
	b, _ := json.Marshal(node.Members)
	ret += fmt.Sprintf("members: %s\n", string(b))

	return ret
}

func (node *Node)CreateSnapshot() *Snapshot {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	return node.store.CreateSnapshot()
}

func (node *Node)InstallSnapshot(sn *Snapshot) bool {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	return node._installSnapshot(sn)
}

func (node *Node)JoinGroup(leaderId string, leaderAddr string) {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	if leaderId == node.Id {
		log.Println("could not join self:", leaderId)
		return
	}
	if len(node.Members) > 0 {
		log.Println("already in group")
		return
	}
	log.Println("JoinGroup", leaderId, leaderAddr)

	node.Term = 0
	node.VoteFor = ""
	node.lastApplied = 0
	node.connectMember(leaderId, leaderAddr)
	node.becomeFollower()
	
	log.Println("clean Raft database")
	node.store.CleanAll()
}

func (node *Node)QuitGroup() {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	log.Println("QuitGroup")
	node.disconnectAllMember()
	node.store.SaveState()
}

func (node *Node)UpdateStatus() {
}


/* ############################################# */

func (node *Node)send(msg *Message){
	msg.Src = node.Id
	msg.Term = node.Term
	if msg.Type != MessageTypeAppendEntry {
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
