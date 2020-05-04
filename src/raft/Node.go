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
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
)

// NodeOption
const(
	TickerInterval   = 100
	ElectionTimeout  = 5 * 1000
	HeartbeatTimeout = 4 * 1000 // TODO: ElectionTimeout/3
	ReplicateTimeout = 1 * 1000
	ReceiveTimeout   = ReplicateTimeout * 3

	SendWindowSize  = 3

	MaxBinlogGapToInstallSnapshot = 5
	MaxUncommittedSize = MaxBinlogGapToInstallSnapshot
)

// Node 是轻量级的, 可以快速的创建和销毁
type Node struct{
	sync.Mutex

	role RoleType
	leader *Member // currently discovered leader
	votesReceived map[string]string // nodeId => vote result
	electionTimer int

	conf *Config
	logs *Binlog

	// messages to be processed by self
	recv_c chan *Message
	// messages to be sent to other nodes
	send_c chan *Message
	stop_c chan bool
	stop_end_c chan bool
}

func NewNode(conf *Config, logs *Binlog) *Node {
	node := new(Node)
	node.role = RoleFollower
	node.recv_c = make(chan *Message, 1/*TODO*/)
	node.send_c = make(chan *Message, 1/*TODO*/)

	node.conf = conf
	node.conf.node = node
	node.logs = logs
	node.logs.node = node
	node.logs.commitIndex = node.conf.applied

	// validate persitent state
	if node.logs.UncommittedSize() > MaxUncommittedSize {
		log.Fatalf("Data corruption, too many uncommitted logs, append: %d, commit: %d",
			node.logs.AppendIndex(), node.logs.CommitIndex())
	}

	return node
}

func (node *Node)Id() string {
	return node.conf.id
}

func (node *Node)Term() int32 {
	return node.conf.term
}

func (node *Node)Vote() string {
	return node.conf.vote
}

func (node *Node)CommitIndex() int64 {
	return node.logs.CommitIndex()
}

func (node *Node)RecvC() chan<- *Message {
	return node.recv_c
}

func (node *Node)SendC() <-chan *Message {
	return node.send_c
}

func (node *Node)Start(){
	log.Printf("Start %s, peers: %s, term: %d, commit: %d, accept: %d",
			node.conf.id, node.conf.peers, node.conf.term,
			node.logs.CommitIndex(), node.logs.AcceptIndex())
	node.startWorders()
	// 单节点运行
	if node.conf.IsSingleton() {
		node.startElection()
		node.checkVoteResult()
	}
}

func (node *Node)Close(){
	log.Printf("Stopping %s...", node.Id())
	node.logs.accept_c <- false
	node.logs.commit_c <- false
	node.recv_c <- nil
	node.stop_c <- true // signal ticker to stop
	for i := 0; i < 4; i++ {
		<- node.stop_end_c
	}

	node.Lock()
	node.conf.Close()
	node.logs.Close()
	node.Unlock()
	log.Printf("Stopped %s", node.Id())
}

func (node *Node)startWorders(){
	node.stop_c = make(chan bool)
	node.stop_end_c = make(chan bool)

	go func() {
		for {
			msg := <- node.recv_c
			if msg == nil {
				break
			}
			node.Lock()
			node.handleRaftMessage(msg)
			node.Unlock()
		}
		node.stop_end_c <- true
	}()

	go func() {
		ticker := time.NewTicker(TickerInterval * time.Millisecond)
		defer ticker.Stop()
		stop := false
		for !stop {
			select{
			case <- node.stop_c:
				stop = true
			case <- ticker.C:
				node.Tick(TickerInterval)
			}
		}
		node.stop_end_c <- true
	}()

	go func() {
		defer func(){
			node.stop_end_c <- true
		}()
		for {
			if b := <- node.logs.accept_c; b == false {
				return
			}
			// clear channel, multi signals are treated as one
			for len(node.logs.accept_c) > 0 {
				b := <- node.logs.accept_c
				if !b {
					return
				}
			}
			node.Lock()
			// 单节点运行
			if node.conf.IsSingleton() && node.role == RoleLeader {
				node.advanceCommitIndex()
			} else {
				if node.role == RoleLeader {
					node.replicateAllMembers()
				} else {
					node.sendAppendEntryAck()
				}
			}
			node.Unlock()
		}
	}()

	go func() {
		defer func(){
			node.stop_end_c <- true
		}()
		for {
			if b := <- node.logs.commit_c; b == false {
				return
			}
			// clear channel, multi signals are treated as one
			for len(node.logs.commit_c) > 0 {
				b := <- node.logs.commit_c
				if !b {
					return
				}
			}
			node.Lock()
			if node.role == RoleLeader {
				node.heartbeatAllMembers()
			}
			node.Unlock()
		}
	}()
}

func (node *Node)Tick(timeElapseMs int){
	node.Lock()
	defer node.Unlock()

	if !node.conf.joined {
		return
	}

	if node.role == RoleFollower || node.role == RoleCandidate {
		node.electionTimer += timeElapseMs
		if node.electionTimer >= ElectionTimeout {
			node.startPreVote()
		}
	} else if node.role == RoleLeader {
		for _, m := range node.conf.members {
			m.ReceiveTimeout += timeElapseMs
			m.ReplicateTimer += timeElapseMs
			m.HeartbeatTimer += timeElapseMs

			if node.logs.CommitIndex() - m.MatchIndex > MaxBinlogGapToInstallSnapshot {
				if m.HeartbeatTimer >= HeartbeatTimeout {
					m.HeartbeatTimer = 0
					log.Printf("Member %s, send snapshot", m.Id)
					node.sendSnapshot(m.Id)
				}
				continue
			}

			if m.ReceiveTimeout < ReceiveTimeout {
				if m.ReplicateTimer >= ReplicateTimeout {
					if m.MatchIndex != -1 && m.NextIndex != m.MatchIndex + 1 {
						log.Printf("Member: %s retransmit, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
						m.NextIndex = m.MatchIndex + 1
					}
					node.replicateMember(m)
				}
			}
			if m.HeartbeatTimer >= HeartbeatTimeout {
				node.heartbeatMember(m)
			}
		}
	}
}

func (node *Node)reset(){
	node.leader = nil
	node.electionTimer = rand.Intn(200)
	node.votesReceived = make(map[string]string)
	node.resetMembers()
}

func (node *Node)startPreVote() {
	node.reset()
	node.role = RoleFollower
	node.broadcast(NewPreVoteMsg())
	log.Printf("Node %s start prevote at term %d", node.Id(), node.Term())
}

func (node *Node)startElection() {
	node.reset()
	node.role = RoleCandidate
	node.conf.SetRound(node.Term() + 1, node.Id())
	node.broadcast(NewRequestVoteMsg())
	log.Printf("Node %s start election at term %d", node.Id(), node.Term())
}

func (node *Node)becomeFollower() {
	node.reset()
	node.role = RoleFollower
	log.Printf("Node %s became follower at term %d", node.Id(), node.Term())
}

func (node *Node)becomeLeader() {
	node.reset()
	node.role = RoleLeader

	if node.Term() == 1 { 
		// 初始化集群成员列表
		for _, id := range node.conf.peers {
			node.logs.Append(node.Term(), EntryTypeConf, fmt.Sprintf("AddMember %s", id))
		}
	} else {
		node.logs.Append(node.Term(), EntryTypeNoop, "")
	}
	log.Printf("Node %s became leader at term %d", node.Id(), node.Term())
}

/* ############################################# */

func (node *Node)resetMembers() {
	nextIndex := node.logs.AcceptIndex() + 1
	for _, m := range node.conf.members {
		m.Reset()
		m.NextIndex = nextIndex
	}
}

func (node *Node)heartbeatAllMembers() {
	for _, m := range node.conf.members {
		if m.NextIndex > node.logs.AcceptIndex() {
			node.heartbeatMember(m)
		}
	}
}

func (node *Node)heartbeatMember(m *Member){
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	prev := node.logs.LastEntry()
	ent := NewHearteatEntry(node.logs.CommitIndex())
	node.send(NewAppendEntryMsg(m.Id, ent, prev))
}

func (node *Node)replicateAllMembers() {
	for _, m := range node.conf.members {
		node.replicateMember(m)
	}
}

func (node *Node)replicateMember(m *Member) {
	if m.NextIndex == 0 {
		return
	}
	if m.MatchIndex != -1 && m.NextIndex - m.MatchIndex > SendWindowSize {
		log.Printf("member %s send window full, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
		return
	}

	m.ReplicateTimer = 0
	maxIndex := util.MaxInt64(m.NextIndex, m.MatchIndex + SendWindowSize)
	for m.NextIndex <= maxIndex {
		ent := node.logs.GetEntry(m.NextIndex)
		if ent == nil {
			break
		}
		ent.Commit = node.logs.CommitIndex()
		
		prev := node.logs.GetEntry(m.NextIndex - 1)
		node.send(NewAppendEntryMsg(m.Id, ent, prev))
		
		m.NextIndex ++
		m.HeartbeatTimer = 0
	}
}

/* ############################################# */

func (node *Node)handleRaftMessage(msg *Message){
	m := node.conf.members[msg.Src]
	if msg.Dst != node.Id() || m == nil {
		log.Printf("%s drop message %s => %s, peers: %s", node.Id(), msg.Src, msg.Dst, node.conf.peers)
		return
	}
	m.ReceiveTimeout = 0

	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term() {
		log.Printf("Node %s receive greater msg.term: %d, node.term: %d", node.Id(), msg.Term, node.Term())
		node.conf.SetRound(msg.Term, "")
		if node.role != RoleFollower {
			node.becomeFollower()
		}
		// continue processing msg
	}
	if msg.Type == MessageTypeGossip {
		return
	}

	if node.role == RoleLeader {
		if msg.Type == MessageTypeAppendEntryAck {
			node.handleAppendEntryAck(m, msg)
		} else if msg.Type == MessageTypePreVote {
			node.handlePreVote(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}

	// MUST: smaller msg.Term is rejected or ignored
	if msg.Term < node.Term() {
		log.Println("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term())
		node.send(NewGossipMsg(msg.Src))
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
		} else if msg.Type == MessageTypeInstallSnapshot {
			node.handleInstallSnapshot(msg)
		} else {
			log.Println("drop message", msg.Encode())
		}
		return
	}
}

func (node *Node)handlePreVote(msg *Message){
	if node.role == RoleLeader {
		arr := make([]int, 0, len(node.conf.members) + 1)
		arr = append(arr, 0) // self
		for _, m := range node.conf.members {
			arr = append(arr, m.ReceiveTimeout)
		}
		sort.Ints(arr)
		log.Println("    receive timeouts[] =", arr)
		timer := arr[len(arr)/2]
		if timer < ReceiveTimeout {
			log.Println("    major followers are still reachable, ignore")
			return
		}
	} else {
		if node.leader != nil && node.leader.ReceiveTimeout < ReceiveTimeout {
			log.Printf("leader %s is still reachable, ignore PreVote from %s", node.leader.Id, msg.Src)
			return
		}
	}
	log.Printf("Node %s grant prevote to %s", node.Id(), msg.Src)
	node.send(NewPreVoteAck(msg.Src))
}

func (node *Node)handlePreVoteAck(msg *Message){
	log.Printf("Node %s receive prevote ack from %s", node.Id(), msg.Src)
	node.votesReceived[msg.Src] = "grant"
	if len(node.votesReceived) + 1 > (len(node.conf.members) + 1)/2 {
		node.startElection()
	}
}

func (node *Node)handleRequestVote(msg *Message){
	// only vote once at one term, if the candidate does not receive ack,
	// it should start a new election
	if node.Vote() != "" /*&& node.Vote() != msg.Src*/ {
		// just ignore
		log.Println("already vote for", node.Vote(), "ignore", msg.Src)
		return
	}
	
	last := node.logs.LastEntry()
	granted := false
	if msg.PrevTerm > last.Term {
		granted = true
	} else if msg.PrevTerm == last.Term && msg.PrevIndex >= last.Index {
		granted = true
	} else {
		// we've got newer log, reject
	}

	if granted {
		node.electionTimer = 0
		node.conf.SetRound(node.Term(), msg.Src)
		node.send(NewRequestVoteAck(msg.Src, true))
		log.Printf("Node %s vote for %s at term %d", node.Id(), msg.Src, node.Term())
	} else {
		node.send(NewRequestVoteAck(msg.Src, false))
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	log.Printf("Node %s receive %s vote ack from %s", node.Id(), msg.Data, msg.Src)
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
	if grant > (len(node.conf.members) + 1)/2 {
		node.becomeLeader()
	} else if reject > len(node.conf.members)/2 {
		log.Printf("grant: %d, reject: %d, total: %d", grant, reject, len(node.conf.members)+1)
		node.becomeFollower()
	}
}

func (node *Node)sendDuplicatedAckToMessage(msg *Message){
	var prev *Entry
	if msg.PrevIndex < node.logs.AcceptIndex() {
		prev = node.logs.GetEntry(msg.PrevIndex - 1)
	} else {
		prev = node.logs.LastEntry()
	}
	
	ack := NewAppendEntryAck(msg.Src, false)
	if prev != nil {
		ack.PrevTerm = prev.Term
		ack.PrevIndex = prev.Index
	}
	node.send(ack)
}

func (node *Node)setLeaderId(leaderId string) {
	if node.leader != nil {
		if node.leader.Id == leaderId {
			return
		}
		node.leader.Role = RoleFollower
	}
	node.leader = node.conf.members[leaderId]
	node.leader.Role = RoleLeader
}

func (node *Node)handleAppendEntry(msg *Message){
	node.electionTimer = 0
	node.setLeaderId(msg.Src)

	if msg.PrevIndex > 0 {
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

	if ent.Type == EntryTypeBeat {
		// TODO: remember last ack index and time
		node.sendAppendEntryAck()
	} else {
		if ent.Index <= node.logs.CommitIndex() {
			log.Printf("invalid entry: %d before commit: %d", ent.Index, node.logs.CommitIndex())
			node.sendDuplicatedAckToMessage(msg)
			return
		}
		node.logs.Write(ent)
	}
	node.logs.Commit(ent.Commit)
}

func (node *Node)handleAppendEntryAck(m *Member, msg *Message) {
	if node.logs.CommitIndex() - msg.PrevIndex > MaxBinlogGapToInstallSnapshot {
		log.Printf("Member %s sync broken, prevIndex %d, commit %d, maxGap: %d",
			msg.Src, msg.PrevIndex, node.logs.CommitIndex(), MaxBinlogGapToInstallSnapshot)
		m.MatchIndex = msg.PrevIndex
		return
	}
	if msg.PrevIndex > node.logs.AcceptIndex() {
		log.Printf("Member %s bad msg, prevIndex %d > lastIndex %d",
			msg.Src, msg.PrevIndex, node.logs.AcceptIndex())
		return
	}

	if msg.Data == "false" {
		if m.NextIndex != msg.PrevIndex + 1 {
			log.Printf("Member %s, reset nextIndex: %d -> %d", m.Id, m.NextIndex, msg.PrevIndex + 1)
			m.MatchIndex = msg.PrevIndex
			m.NextIndex  = msg.PrevIndex + 1
		}
		return
	} else {
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		m.NextIndex  = util.MaxInt64(m.NextIndex,  msg.PrevIndex + 1)
		if m.MatchIndex > node.logs.CommitIndex() {
			node.advanceCommitIndex()
		}
	}
	node.replicateMember(m)
}

func (node *Node)advanceCommitIndex() {
	// sort matchIndex[] in descend order
	matchIndex := make([]int64, 0, len(node.conf.members) + 1)
	matchIndex = append(matchIndex, node.logs.AcceptIndex()) // self
	for _, m := range node.conf.members {
		matchIndex = append(matchIndex, m.MatchIndex)
	}
	sort.Slice(matchIndex, func(i, j int) bool{
		return matchIndex[i] > matchIndex[j]
	})
	commitIndex := matchIndex[len(matchIndex)/2]
	log.Println("match[] =", matchIndex, " major matchIndex", commitIndex)

	commitIndex = util.MinInt64(commitIndex, node.logs.AcceptIndex())
	if commitIndex > node.logs.CommitIndex() {
		ent := node.logs.GetEntry(commitIndex)
		// only commit currentTerm's log
		if ent.Term == node.Term() {
			node.logs.Commit(commitIndex)
		}
	}
}

func (node *Node)handleInstallSnapshot(msg *Message) {
	node.loadSnapshot(msg.Data)
	node.send(NewAppendEntryAck(msg.Src, true))
}

/* ###################### Methods ####################### */

func (node *Node)Propose(data string) (int32, int64) {
	return node._propose(EntryTypeData, data)
}

func (node *Node)_propose(etype EntryType, data string) (int32, int64) {
	var term int32
	node.Lock()
	if node.role != RoleLeader {
		node.Unlock()
		log.Println("error: not leader")
		return -1, -1
	}
	// assign term to a new entry while holding lock
	term = node.Term()
	node.Unlock()

	// check MaxUncommittedSize
	for node.logs.UncommittedSize() >= 3 {
		log.Println("sleep")
		log.Printf("commit: %d, accept: %d append: %d", node.logs.CommitIndex(), node.logs.AcceptIndex(), node.logs.AppendIndex())
		util.Sleep(1)
	}

	ent := node.logs.Append(term, etype, data)
	return ent.Term, ent.Index
}

func (node *Node)ProposeAddMember(nodeId string) (int32, int64) {
	data := fmt.Sprintf("AddMember %s", nodeId)
	return node._propose(EntryTypeConf, data)
}

func (node *Node)ProposeDelMember(nodeId string) (int32, int64) {
	data := fmt.Sprintf("DelMember %s", nodeId)
	return node._propose(EntryTypeConf, data)
}

// // 获取集群的 ReadIndex, 这个函数将发起集群内的网络交互, 阻塞直到收到结果.
// func (node *Node)RaftReadIndex() int64 {
// }

func (node *Node)Info() string {
	node.Lock()
	defer node.Unlock()
	
	var ret string
	ret += fmt.Sprintf("id: %s\n", node.Id())
	ret += fmt.Sprintf("role: %s\n", node.role)
	ret += fmt.Sprintf("term: %d\n", node.conf.term)
	ret += fmt.Sprintf("vote: %s\n", node.conf.vote)
	ret += fmt.Sprintf("append: %d\n", node.logs.AppendIndex())
	ret += fmt.Sprintf("accept: %d\n", node.logs.AcceptIndex())
	ret += fmt.Sprintf("commit: %d\n", node.logs.CommitIndex())
	bs, _ := json.Marshal(node.conf.members)
	ret += fmt.Sprintf("members: %s\n", string(bs))

	return ret
}

/* ############################################# */

func (node *Node)send(msg *Message){
	new_msg := *msg // copy
	new_msg.Src = node.Id()
	new_msg.Term = node.Term()
	if new_msg.PrevTerm == 0 && new_msg.Type != MessageTypeAppendEntry {
		last := node.logs.LastEntry()
		new_msg.PrevTerm = last.Term
		new_msg.PrevIndex = last.Index
	}
	node.send_c <- &new_msg
}

func (node *Node)broadcast(msg *Message){
	for _, m := range node.conf.members {
		msg.Dst = m.Id
		node.send(msg)
	}
}

func (node *Node)sendAppendEntryAck() {
	if node.leader != nil {
		node.send(NewAppendEntryAck(node.leader.Id, true))
	}
}

func (node *Node)sendSnapshot(dst string){
	data := node.makeSnapshot()
	resp := NewInstallSnapshotMsg(dst, data)
	node.send(resp)
}

/* ###################### Snapshot ####################### */

func (node *Node)makeSnapshot() string {
	sn := MakeSnapshot(node)
	return sn.Encode()
}

func (node *Node)loadSnapshot(data string) {
	log.Printf("Node %s installing snapshot", node.Id())
	sn := new(Snapshot)
	if sn.Decode(data) == false {
		log.Println("decode snapshot error")
		return
	}

	node.reset()
	node.role = RoleFollower

	node.conf.RecoverFromSnapshot(sn)
	node.logs.RecoverFromSnapshot(sn)

	log.Printf("Reset %s, peers: %s, term: %d, commit: %d, accept: %d",
			node.conf.id, node.conf.peers, node.conf.term,
			node.logs.CommitIndex(), node.logs.AcceptIndex())

	log.Printf("Done")
}
