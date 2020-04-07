package raft

import (
	"fmt"
	"log"
	"sort"
	"math/rand"
	"time"
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
	TickerInterval     = 100
	ElectionTimeout    = 5 * 1000
	HeartbeatTimeout   = 4 * 1000 // TODO: ElectionTimeout/3
	ReplicateTimeout   = 1 * 1000
	ReceiveTimeout     = ReplicateTimeout * 3

	SendWindowSize  = 3
	SendChannelSize = 10
)

type Node struct{
	role RoleType
	members map[string]*Member // 由 Config 维护
	votesReceived map[string]string
	electionTimer int
	commitIndex int64

	conf *Config
	logs *Binlog

	// messages to be processed by raft
	recv_c chan *Message
	// messages to be sent to other node
	send_c chan *Message
	
	mux sync.Mutex
}

func NewNode(conf *Config) *Node{
	node := new(Node)
	node.role = RoleFollower
	node.conf = conf
	node.logs = NewBinlog(node)
	node.recv_c = make(chan *Message, 1)
	node.send_c = make(chan *Message, SendChannelSize)
	node.electionTimer = 3 * 1000

	node.conf.Init(node)
	return node
}

func (node *Node)Id() string {
	return node.conf.id
}

func (node *Node)Term() int32 {
	return node.conf.term
}

func (node *Node)VoteFor() string {
	return node.conf.voteFor
}

func (node *Node)RecvC() chan<- *Message {
	return node.recv_c
}

func (node *Node)SendC() <-chan *Message {
	return node.send_c
}

func (node *Node)Start(){
	node.startTicker()
	node.startNetwork()
	node.Tick(0)
}

func (node *Node)Close(){
	node.mux.Lock()
	defer node.mux.Unlock()
	node.logs.Close()
}

func (node *Node)startTicker(){
	log.Println("start ticker, interval:", TickerInterval)
	go func() {
		ticker := time.NewTicker(TickerInterval * time.Millisecond)
		defer ticker.Stop()

		for {
			<- ticker.C
			node.Tick(TickerInterval)
		}
	}()
}

func (node *Node)startNetwork(){
	log.Println("start networking")
	go func() {
		for{
			node.handleEvent()
		}
	}()
}

func (node *Node)handleEvent() {
	select{
	case <-node.logs.fsyncNotify:
		node.mux.Lock()
		if node.role == RoleLeader {
			node.replicateAllMembers()
		}
		node.mux.Unlock()
	case msg := <-node.recv_c:
		node.mux.Lock()
		node.handleRaftMessage(msg)
		node.mux.Unlock()
	}
}

// for testing
func (node *Node)Step() {
	// fmt.Printf("\n------------------ %s Step ------------------\n", node.Id())
	for {
		node.handleEvent()
	}
}

func (node *Node)Tick(timeElapseMs int){
	node.mux.Lock()
	defer node.mux.Unlock()

	if node.role == RoleFollower || node.role == RoleCandidate {
		// 单节点运行
		if len(node.members) == 0 {
			log.Println("start Election")
			node.startElection()
			node.checkVoteResult()
		} else {
			node.electionTimer += timeElapseMs
			if node.electionTimer >= ElectionTimeout {
				log.Println("start PreVote")
				node.startPreVote()
			}
		}
	} else if node.role == RoleLeader {
		for _, m := range node.members {
			m.ReceiveTimeout += timeElapseMs
			m.ReplicateTimer += timeElapseMs
			m.HeartbeatTimer += timeElapseMs

			if m.ReceiveTimeout < ReceiveTimeout {
				if m.ReplicateTimer >= ReplicateTimeout {
					if m.MatchIndex != 0 && m.NextIndex != m.MatchIndex + 1 {
						log.Printf("resend member: %s, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
						m.NextIndex = m.MatchIndex + 1
					}
					node.replicateMember(m)
				}
			}
			if m.HeartbeatTimer >= HeartbeatTimeout {
				// log.Println("Heartbeat timeout for node", m.Id)
				node.pingMember(m)
			}
		}
	}
}

// prepare ping all members, will actually ping in Tick()
func (node *Node)preparePingAllMembers(){
	for _, m := range node.members {
		m.HeartbeatTimer = HeartbeatTimeout
	}
}

func (node *Node)startPreVote(){
	node.role = RoleFollower
	node.electionTimer = 0
	node.votesReceived = make(map[string]string)
	node.broadcast(NewPreVoteMsg())
}

func (node *Node)startElection(){
	node.role = RoleCandidate
	node.electionTimer = rand.Intn(200)
	node.votesReceived = make(map[string]string)
	node.resetMembers()
	node.conf.NewTerm()
	node.broadcast(NewRequestVoteMsg())
}

func (node *Node)becomeFollower(){
	node.role = RoleFollower
	node.electionTimer = 0	
	node.resetMembers()
}

func (node *Node)becomeLeader(){
	node.role = RoleLeader
	node.electionTimer = 0
	node.resetMembers()

	// write noop entry with currentTerm to implictly commit previous term's log
	node.logs.AppendEntry(EntryTypeNoop, "")
	log.Printf("Node %s became leader", node.Id())
}

/* ############################################# */

func (node *Node)resetMembers() {
	for _, m := range node.members {
		m.Reset()
		m.NextIndex = node.logs.lastIndex + 1
	}
}

func (node *Node)pingAllMember(){
	for _, m := range node.members {
		node.pingMember(m)
	}
}

func (node *Node)pingMember(m *Member){
	m.HeartbeatTimer = 0
	
	prev := node.logs.GetEntry(node.logs.lastIndex)
	ent := NewPingEntry(node.commitIndex)
	node.send(NewAppendEntryMsg(m.Id, ent, prev))
}

func (node *Node)replicateAllMembers(){
	for _, m := range node.members {
		node.replicateMember(m)
	}
}

func (node *Node)replicateMember(m *Member){
	if m.MatchIndex != 0 && m.NextIndex - m.MatchIndex > SendWindowSize {
		log.Printf("stop and wait %s, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
		return
	}

	m.ReplicateTimer = 0
	maxIndex := util.MaxInt64(m.NextIndex, m.MatchIndex + SendWindowSize)
	for m.NextIndex <= maxIndex {
		ent := node.logs.GetEntry(m.NextIndex)
		// log.Println(node.Id(), m.Id, m.NextIndex, ent)
		if ent == nil {
			break
		}
		ent.Commit = node.commitIndex
		
		prev := node.logs.GetEntry(m.NextIndex - 1)
		node.send(NewAppendEntryMsg(m.Id, ent, prev))
		
		m.NextIndex ++
		m.HeartbeatTimer = 0
	}
}


/* ############################################# */

func (node *Node)handleRaftMessage(msg *Message){
	m := node.members[msg.Src]
	if msg.Dst != node.Id() || m == nil {
		log.Println(node.Id(), "drop message from unknown src", msg.Src, "dst", msg.Dst, "members: ", node.members)
		return
	}
	m.ReceiveTimeout = 0

	// MUST: smaller msg.Term is rejected or ignored
	if msg.Term < node.Term() {
		log.Println("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term())
		node.send(NewGossipMsg(msg.Src))
		// finish processing msg
		return
	}
	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term() {
		log.Printf("receive greater msg.term: %d, node.term: %d", msg.Term, node.Term())
		node.conf.SaveState(msg.Term, "")
		if node.role != RoleFollower {
			log.Printf("Node %s became follower", node.Id())
			node.becomeFollower()
		}
		// continue processing msg
	}
	if msg.Type == MessageTypeGossip {
		return
	}

	if node.role == RoleLeader {
		if msg.Type == MessageTypeAppendEntryAck {
			node.handleAppendEntryAck(msg)
		} else if msg.Type == MessageTypePreVote {
			node.handlePreVote(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}
	if node.role == RoleCandidate {
		if msg.Type == MessageTypeRequestVoteAck {
			node.handleRequestVoteAck(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}
	if node.role == RoleFollower {
		if msg.Type == MessageTypeRequestVote {
			node.handleRequestVote(msg)
		} else if msg.Type == MessageTypeAppendEntry {
			node.handleAppendEntry(msg)
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
	if node.role == RoleLeader {
		arr := make([]int, 0, len(node.members) + 1)
		arr = append(arr, 0) // self
		for _, m := range node.members {
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
	for _, m := range node.members {
		if m.Role == RoleLeader && m.ReceiveTimeout < ReceiveTimeout {
			log.Printf("leader %s is still active, ignore PreVote from %s", m.Id, msg.Src)
			return
		}
	}
	node.send(NewPreVoteAck(msg.Src))
}

func (node *Node)handlePreVoteAck(msg *Message){
	log.Printf("receive %s from %s", msg.Type, msg.Src)
	node.votesReceived[msg.Src] = msg.Data
	if len(node.votesReceived) + 1 > (len(node.members) + 1)/2 {
		node.startElection()
	}
}

func (node *Node)handleRequestVote(msg *Message){
	if node.VoteFor() != "" && node.VoteFor() != msg.Src {
		// just ignore
		log.Println("already vote for", node.VoteFor(), "ignore", msg.Src)
		return
	}
	
	granted := false
	if msg.PrevTerm > node.logs.lastTerm {
		granted = true
	} else if msg.PrevTerm == node.logs.lastTerm && msg.PrevIndex >= node.logs.lastIndex {
		granted = true
	} else {
		// we've got newer log, reject
	}

	if granted {
		node.electionTimer = 0
		log.Println("vote for", msg.Src)
		node.conf.SetVoteFor(msg.Src)
		node.send(NewRequestVoteAck(msg.Src, true))
	} else {
		node.send(NewRequestVoteAck(msg.Src, false))
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	log.Printf("receive %s from %s", msg.Type, msg.Src)
	node.votesReceived[msg.Src] = msg.Data
	node.checkVoteResult()
}

func (node *Node)checkVoteResult(){
	grant := 1
	reject := 0
	for _, res := range node.votesReceived {
		if res == "grant" {
			grant ++
		} else {
			reject ++
		}
	}
	if grant > (len(node.members) + 1)/2 {
		node.becomeLeader()
	} else if reject > len(node.members)/2 {
		log.Printf("grant: %d, reject: %d, total: %d", grant, reject, len(node.members)+1)
		node.becomeFollower()
	}
}

func (node *Node)sendDuplicatedAckToMessage(msg *Message){
	var prev *Entry
	if msg.PrevIndex < node.logs.lastIndex {
		prev = node.logs.GetEntry(msg.PrevIndex - 1)
	} else {
		prev = node.logs.GetEntry(node.logs.lastIndex)
	}
	
	ack := NewAppendEntryAck(msg.Src, false)
	if prev != nil {
		ack.PrevTerm = prev.Term
		ack.PrevIndex = prev.Index
	}

	node.send(ack)
}

func (node *Node)handleAppendEntry(msg *Message){
	for _, m := range node.members {
		if m.Id == msg.Src {
			m.Role = RoleLeader
		} else {
			m.Role = RoleFollower
		}
	}
	node.electionTimer = 0

	if msg.PrevIndex > node.commitIndex {
		if msg.PrevIndex != node.logs.lastIndex {
			log.Printf("gap between entry, msg.prevIndex: %d, lastIndex: %d", msg.PrevIndex, node.logs.lastIndex)
			node.sendDuplicatedAckToMessage(msg)
			return
		}
		prev := node.logs.GetEntry(msg.PrevIndex)
		if prev == nil {
			log.Println("prev entry not found", msg.PrevTerm, msg.PrevIndex)
			node.sendDuplicatedAckToMessage(msg)
			return
		}
		if prev.Term != msg.PrevTerm {
			log.Printf("entry index: %d, prev.Term %d != msg.PrevTerm %d", msg.PrevIndex, prev.Term, msg.PrevTerm)
			node.sendDuplicatedAckToMessage(msg)
			return
		}
	}

	ent := DecodeEntry(msg.Data)

	if ent.Type == EntryTypePing {
		node.send(NewAppendEntryAck(msg.Src, true))
	} else {
		if ent.Index < node.commitIndex {
			log.Printf("entry: %d before committed: %d", ent.Index, node.commitIndex)
			node.sendDuplicatedAckToMessage(msg)
			return
		}

		old := node.logs.GetEntry(ent.Index)
		if old != nil {
			if old.Term != ent.Term {
				// TODO:
				log.Println("TODO: delete conflict entry, and entries that follow")
			} else {
				log.Println("duplicated entry ", ent.Term, ent.Index)
			}
		}
		node.logs.WriteEntry(*ent)
		// TODO: delay/batch ack
		node.send(NewAppendEntryAck(msg.Src, true))
	}

	commitIndex := util.MinInt64(ent.Commit, node.logs.lastIndex)
	node.commitEntry(commitIndex)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.members[msg.Src]

	if msg.Data == "false" {
		log.Printf("node %s, reset nextIndex: %d -> %d", m.Id, m.NextIndex, msg.PrevIndex + 1)
		m.MatchIndex = msg.PrevIndex
		m.NextIndex  = m.MatchIndex + 1
	} else {
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		m.NextIndex  = util.MaxInt64(m.NextIndex, m.MatchIndex + 1)
		if m.MatchIndex > node.commitIndex {
			oldI := node.commitIndex
			node.proceedCommitIndex()
			newI := node.commitIndex
			if oldI != newI {
				node.preparePingAllMembers()
			}
		}
	}
	node.replicateMember(m)
}

func (node *Node)proceedCommitIndex() {
	// sort matchIndex[] in descend order
	matchIndex := make([]int64, 0, len(node.members) + 1)
	matchIndex = append(matchIndex, node.logs.lastIndex) // self
	for _, m := range node.members {
		matchIndex = append(matchIndex, m.MatchIndex)
	}
	sort.Slice(matchIndex, func(i, j int) bool{
		return matchIndex[i] > matchIndex[j]
	})
	commitIndex := matchIndex[len(matchIndex)/2]
	log.Println("match[] =", matchIndex, " major matchIndex", commitIndex)

	commitIndex = util.MinInt64(commitIndex, node.logs.lastIndex)
	if commitIndex > node.commitIndex {
		ent := node.logs.GetEntry(commitIndex)
		// only commit currentTerm's log
		if ent.Term == node.Term() {
			node.commitEntry(commitIndex)
		}
	}
}

func (node *Node)commitEntry(commitIndex int64) {
	if commitIndex <= node.commitIndex {
		return
	}
	log.Printf("commit %d => %d", node.commitIndex, commitIndex)
	node.commitIndex = commitIndex

	// synchronously apply to Config
	for index := node.conf.LastApplied() + 1; index <= node.commitIndex; index ++ {
		ent := node.logs.GetEntry(index)
		if ent == nil {
			log.Fatalf("Lost entry@%d", index)
		}
		node.conf.ApplyEntry(ent)
	}

	// TODO: asynchronously apply to Service
}

/* ###################### Methods ####################### */

func (node *Node)Propose(data string) (int32, int64) {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	if node.role != RoleLeader {
		log.Println("error: not leader")
		return -1, -1
	}
	
	ent := node.logs.AppendEntry(EntryTypeData, data)
	return ent.Term, ent.Index
}

func (node *Node)Info() string {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	var ret string
	ret += fmt.Sprintf("id: %s\n", node.Id())
	ret += fmt.Sprintf("addr: %s\n", node.conf.addr)
	ret += fmt.Sprintf("role: %s\n", node.role)
	ret += fmt.Sprintf("term: %d\n", node.Term())
	ret += fmt.Sprintf("voteFor: %s\n", node.VoteFor())
	ret += fmt.Sprintf("lastApplied: %d\n", node.conf.LastApplied())
	ret += fmt.Sprintf("commitIndex: %d\n", node.commitIndex)
	ret += fmt.Sprintf("lastTerm: %d\n", node.logs.lastTerm)
	ret += fmt.Sprintf("lastIndex: %d\n", node.logs.lastIndex)
	ret += fmt.Sprintf("electionTimer: %d\n", node.electionTimer)
	b, _ := json.Marshal(node.members)
	ret += fmt.Sprintf("members: %s\n", string(b))

	return ret
}

/* ############################################# */

func (node *Node)send(msg *Message){
	new_msg := *msg // copy
	new_msg.Src = node.Id()
	new_msg.Term = node.Term()
	if new_msg.PrevTerm == 0 && new_msg.Type != MessageTypeAppendEntry {
		new_msg.PrevTerm = node.logs.lastTerm
		new_msg.PrevIndex = node.logs.lastIndex
	}
	node.send_c <- &new_msg
}

func (node *Node)broadcast(msg *Message){
	for _, m := range node.members {
		msg.Dst = m.Id
		node.send(msg)
	}
}
