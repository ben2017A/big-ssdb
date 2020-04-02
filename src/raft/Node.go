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
	ElectionTimeout    = 5 * 1000
	HeartbeatTimeout   = 4 * 1000 // TODO: ElectionTimeout/3
	ReplicationTimeout = 1 * 1000
	ReceiveTimeout     = HeartbeatTimeout * 3
)

type Node struct{
	Role RoleType
	Members map[string]*Member // 由 Config 维护
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
	node.Role = RoleFollower
	node.conf = conf
	node.logs = NewBinlog(node)
	node.recv_c = make(chan *Message, 3)
	node.send_c = make(chan *Message, 3)
	node.electionTimer = 2 * 1000

	node.conf.Init(node)
	return node
}

func (node *Node)Id() string {
	return node.conf.Id()
}

func (node *Node)Term() int32 {
	return node.conf.Term()
}

func (node *Node)VoteFor() string {
	return node.conf.VoteFor()
}

func (node *Node)RecvC() chan<- *Message {
	return node.recv_c
}

func (node *Node)SendC() <-chan *Message {
	return node.send_c
}

func (node *Node)Start(){
	node.StartTicker()
	node.StartNetwork()
}

func (node *Node)StartTicker(){
	go func() {
		const TimerInterval = 100
		ticker := time.NewTicker(TimerInterval * time.Millisecond)
		defer ticker.Stop()

		log.Println("setup ticker, interval:", TimerInterval)
		for {
			<- ticker.C
			node.mux.Lock()
			node.Tick(TimerInterval)
			node.mux.Unlock()
		}
	}()
}

func (node *Node)StartNetwork(){
	go func() {
		log.Println("setup networking")
		for{
			select{
			// case <-node.store.C:
			// 	node.mux.Lock()
			// 	node.replicateAllMembers()
			// 	node.mux.Unlock()
			case msg := <-node.recv_c:
				node.mux.Lock()
				node.handleRaftMessage(msg)
				node.mux.Unlock()
			}
		}
	}()
}

func (node *Node)Close(){
	node.logs.Close()
}

func (node *Node)Tick(timeElapse int){
	if node.Role == RoleFollower || node.Role == RoleCandidate {
		if len(node.Members) > 0 {
			node.electionTimer += timeElapse
			if node.electionTimer >= ElectionTimeout {
				log.Println("start PreVote")
				node.startPreVote()
			}
		}
	} else if node.Role == RoleLeader {
		for _, m := range node.Members {
			m.ReceiveTimeout += timeElapse
			m.ReplicateTimer += timeElapse
			m.HeartbeatTimer += timeElapse

			if m.ReceiveTimeout < ReceiveTimeout {
				if m.ReplicateTimer >= ReplicationTimeout {
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

func (node *Node)startPreVote(){
	node.Role = RoleFollower
	node.electionTimer = 0
	node.votesReceived = make(map[string]string)

	node.broadcast(NewPreVoteMsg())
	
	// 单节点运行
	if len(node.Members) == 0 {
		node.startElection()
	}
}

func (node *Node)startElection(){
	node.Role = RoleCandidate
	node.electionTimer = rand.Intn(200)
	node.votesReceived = make(map[string]string)
	node.resetMembers()

	node.conf.NewTerm()
	node.broadcast(NewRequestVoteMsg())
	
	// 单节点运行
	if len(node.Members) == 0 {
		node.checkVoteResult()
	}
}

func (node *Node)becomeFollower(){
	node.Role = RoleFollower
	node.electionTimer = 0	
	node.resetMembers()
}

func (node *Node)becomeLeader(){
	node.Role = RoleLeader
	node.electionTimer = 0
	node.resetMembers()

	// write noop entry with currentTerm to implictly commit previous term's log
	node.logs.AppendEntry(EntryTypeNoop, "")
	log.Printf("Node %s became leader", node.Id())
}

/* ############################################# */

func (node *Node)resetMembers() {
	for _, m := range node.Members {
		m.Reset()
		m.NextIndex = node.logs.LastIndex + 1
	}
}

func (node *Node)pingAllMember(){
	for _, m := range node.Members {
		node.pingMember(m)
	}
}

func (node *Node)pingMember(m *Member){
	m.HeartbeatTimer = 0
	
	prev := node.logs.GetEntry(node.logs.LastIndex)
	ent := NewPingEntry(node.commitIndex)
	node.send(NewAppendEntryMsg(m.Id, ent, prev))
}

func (node *Node)replicateAllMembers(){
	for _, m := range node.Members {
		node.replicateMember(m)
	}
}

func (node *Node)replicateMember(m *Member){
	if m.MatchIndex != 0 && m.NextIndex - m.MatchIndex > m.SendWindow {
		log.Printf("stop and wait %s, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
		return
	}

	m.ReplicateTimer = 0
	maxIndex := util.MaxInt64(m.NextIndex, m.MatchIndex + m.SendWindow)
	for m.NextIndex <= maxIndex {
		ent := node.logs.GetEntry(m.NextIndex)
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
	if grant > (len(node.Members) + 1)/2 {
		node.becomeLeader()
	} else if reject > len(node.Members)/2 {
		log.Printf("grant: %d, reject: %d, total: %d", grant, reject, len(node.Members)+1)
		node.becomeFollower()
	}
}

func (node *Node)handleRaftMessage(msg *Message){
	m := node.Members[msg.Src]
	if msg.Dst != node.Id() || m == nil {
		log.Println(node.Id(), "drop message from unknown src", msg.Src, "dst", msg.Dst, "members: ", node.Members)
		return
	}
	m.ReceiveTimeout = 0

	// MUST: smaller msg.Term is rejected or ignored
	if msg.Term < node.Term() {
		log.Println("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term())
		node.send(NewNoneMsg(msg.Src))
		// finish processing msg
		return
	}
	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term() {
		log.Printf("receive greater msg.term: %d, node.term: %d", msg.Term, node.Term())
		node.conf.SaveState(msg.Term, "")
		if node.Role != RoleFollower {
			log.Printf("Node %s became follower", node.Id())
			node.becomeFollower()
		}
		// continue processing msg
	}
	if msg.Type == MessageTypeNone {
		return
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
	if node.VoteFor() != "" && node.VoteFor() != msg.Src {
		// just ignore
		log.Println("already vote for", node.VoteFor(), "ignore", msg.Src)
		return
	}
	
	granted := false
	if msg.PrevTerm > node.logs.LastTerm {
		granted = true
	} else if msg.PrevTerm == node.logs.LastTerm && msg.PrevIndex >= node.logs.LastIndex {
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
	log.Printf("receive vote %s from %s", msg.Data, msg.Src)
	node.votesReceived[msg.Src] = msg.Data
	node.checkVoteResult()
}

func (node *Node)sendDuplicatedAckToMessage(msg *Message){
	var prev *Entry
	if msg.PrevIndex < node.logs.LastIndex {
		prev = node.logs.GetEntry(msg.PrevIndex - 1)
	} else {
		prev = node.logs.GetEntry(node.logs.LastIndex)
	}
	
	ack := NewAppendEntryAck(msg.Src, false)
	if prev != nil {
		ack.PrevTerm = prev.Term
		ack.PrevIndex = prev.Index
	}

	node.send(ack)
}

func (node *Node)handleAppendEntry(msg *Message){
	for _, m := range node.Members {
		if m.Id == msg.Src {
			m.Role = RoleLeader
		} else {
			m.Role = RoleFollower
		}
	}
	node.electionTimer = 0

	if msg.PrevIndex > node.commitIndex {
		if msg.PrevIndex != node.logs.LastIndex {
			log.Printf("gap between entry, msg.prevIndex: %d, lastIndex: %d", msg.PrevIndex, node.logs.LastIndex)
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

	node.CommitEntry(ent.Commit)
}

func (node *Node)handleAppendEntryAck(msg *Message){
	m := node.Members[msg.Src]

	if msg.Data == "false" {
		log.Printf("node %s, reset nextIndex: %d -> %d", m.Id, m.NextIndex, msg.PrevIndex + 1)
		m.NextIndex = msg.PrevIndex + 1
	} else {
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		m.NextIndex  = util.MaxInt64(m.NextIndex, m.MatchIndex + 1)
		if m.MatchIndex > node.commitIndex {
			commitIndex := node.checkCommitIndex()
			if commitIndex > node.commitIndex {
				// only commit currentTerm's log
				ent := node.logs.GetEntry(commitIndex)
				if ent.Term == node.Term() {
					node.CommitEntry(commitIndex)

					// follower acked last entry, heartbeat it to commit
					if m.MatchIndex == node.logs.LastIndex {
						// TODO: ping all?
						node.pingMember(m)
						return
					}
				}
			}
		}
	}

	node.replicateMember(m)
}

func (node *Node)checkCommitIndex() int64 {
	// sort matchIndex[] in descend order
	matchIndex := make([]int64, 0, len(node.Members) + 1)
	matchIndex = append(matchIndex, node.logs.LastIndex) // self
	for _, m := range node.Members {
		matchIndex = append(matchIndex, m.MatchIndex)
	}
	sort.Slice(matchIndex, func(i, j int) bool{
		return matchIndex[i] > matchIndex[j]
	})
	commitIndex := matchIndex[len(matchIndex)/2]
	log.Println("match[] =", matchIndex, "commit", commitIndex)
	return commitIndex
}

func (node *Node)CommitEntry(index int64) {
	node.commitIndex = util.MinInt64(node.logs.LastIndex, index)
}

/* ###################### Quorum Methods ####################### */

func (node *Node)Propose(data string) (int32, int64) {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	if node.Role != RoleLeader {
		log.Println("error: not leader")
		return -1, -1
	}
	
	ent := node.logs.AppendEntry(EntryTypeData, data)
	return ent.Term, ent.Index
}

/* ###################### Operations ####################### */

func (node *Node)InfoMap() map[string]string {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	m := make(map[string]string)
	m["id"] = fmt.Sprintf("%s", node.Id())
	m["addr"] = node.conf.Addr()
	m["role"] = string(node.Role)
	m["term"] = fmt.Sprintf("%d", node.Term())
	m["voteFor"] = fmt.Sprintf("%s", node.VoteFor())
	m["lastApplied"] = fmt.Sprintf("%d", node.conf.LastApplied())
	m["commitIndex"] = fmt.Sprintf("%d", node.commitIndex)
	m["lastTerm"] = fmt.Sprintf("%d", node.logs.LastTerm)
	m["lastIndex"] = fmt.Sprintf("%d", node.logs.LastIndex)
	b, _ := json.Marshal(node.Members)
	m["members"] = string(b)
	return m
}

func (node *Node)Info() string {
	node.mux.Lock()
	defer node.mux.Unlock()
	
	var ret string
	ret += fmt.Sprintf("id: %s\n", node.Id())
	ret += fmt.Sprintf("addr: %s\n", node.conf.Addr())
	ret += fmt.Sprintf("role: %s\n", node.Role)
	ret += fmt.Sprintf("term: %d\n", node.Term())
	ret += fmt.Sprintf("voteFor: %s\n", node.VoteFor())
	ret += fmt.Sprintf("lastApplied: %d\n", node.conf.LastApplied())
	ret += fmt.Sprintf("commitIndex: %d\n", node.commitIndex)
	ret += fmt.Sprintf("lastTerm: %d\n", node.logs.LastTerm)
	ret += fmt.Sprintf("lastIndex: %d\n", node.logs.LastIndex)
	ret += fmt.Sprintf("electionTimer: %d\n", node.electionTimer)
	b, _ := json.Marshal(node.Members)
	ret += fmt.Sprintf("members: %s\n", string(b))

	return ret
}

/* ############################################# */

func (node *Node)send(msg *Message){
	msg.Src = node.Id()
	msg.Term = node.Term()
	if msg.PrevTerm == 0 && msg.Type != MessageTypeAppendEntry {
		msg.PrevTerm = node.logs.LastTerm
		msg.PrevIndex = node.logs.LastIndex
	}
	node.send_c <- msg
}

func (node *Node)broadcast(msg *Message){
	for _, m := range node.Members {
		new_msg := *msg // copy
		new_msg.Dst = m.Id
		node.send(&new_msg)
	}
}
