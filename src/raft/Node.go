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
	MaxPendingLogs = MaxBinlogGapToInstallSnapshot
)

// Node 是轻量级的, 可以快速的创建和销毁
type Node struct{
	role RoleType
	leader string // currently discovered leader
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
	append_c chan bool
	commit_c chan bool
	
	mux sync.Mutex
}

func NewNode(conf *Config, logs *Binlog) *Node {
	node := new(Node)
	node.role = RoleFollower
	node.recv_c = make(chan *Message, 1) // TODO:
	node.send_c = make(chan *Message, 1) // TODO:
	node.append_c = make(chan bool, 3) // log been persisted
	node.commit_c = make(chan bool, 3) // log been committed

	node.conf = conf
	node.conf.node = node

	node.logs = logs
	node.logs.node = node

	// validate persitent state
	if node.conf.commit > node.logs.LastIndex() {
		log.Fatalf("Data corruption, commit: %d > lastIndex: %d", node.conf.commit, node.logs.LastIndex())
	}
	if node.conf.commit < node.logs.LastIndex() - MaxPendingLogs {
		log.Fatalf("Data corruption, too much pending logs, commit: %d, lastIndex: %d",
			node.conf.commit, node.logs.LastIndex())
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
	return node.conf.commit
}

func (node *Node)RecvC() chan<- *Message {
	return node.recv_c
}

func (node *Node)SendC() <-chan *Message {
	return node.send_c
}

func (node *Node)Start(){
	last := node.logs.LastEntry()
	log.Printf("Start %s, peers: %s, term: %d, commit: %d, lastTerm: %d, lastIndex: %d",
			node.conf.id, node.conf.peers, node.conf.term, node.conf.commit,
			last.Term, last.Index)
	node.startWorders()
	// 单节点运行
	if len(node.conf.members) == 0 {
		node.startElection()
		node.checkVoteResult()
	}
}

func (node *Node)Close(){
	log.Printf("Stopping %s...", node.Id())
	for i := 0; i < 4; i++ {
		// may or may not wake up all readers, so send n signals
		node.stop_c <- true // signal to stop
		<- node.stop_end_c
	}

	node.mux.Lock()
	node.conf.Close()
	node.logs.Close()
	node.mux.Unlock()
	log.Printf("Stopped %s", node.Id())
}

func (node *Node)startWorders(){
	node.stop_c = make(chan bool)
	node.stop_end_c = make(chan bool)

	go func() {
		stop := false
		for !stop {
			select{
			case <- node.stop_c:
				stop = true
			case msg := <-node.recv_c:
				node.mux.Lock()
				node.handleRaftMessage(msg)
				node.mux.Unlock()
			}
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
		stop := false
		for !stop {
			select{
			case <- node.stop_c:
				stop = true
			case <-node.append_c:
				// clear channel, multi signals are treated as one
				for len(node.append_c) > 0 {
					<- node.append_c
				}
				// 单节点运行
				if len(node.conf.members) == 0 && node.role == RoleLeader {
					node.advanceCommitIndex()
					break
				}
				if node.role == RoleLeader {
					node.mux.Lock()
					node.replicateAllMembers()
					node.mux.Unlock()
				} else {
					node.sendAppendEntryAck()
				}
			}
		}
		node.stop_end_c <- true
	}()

	go func() {
		stop := false
		for !stop {
			select{
			case <- node.stop_c:
				stop = true
			case <- node.commit_c:
				// clear channel, multi signals are treated as one
				for len(node.commit_c) > 0 {
					<- node.commit_c
				}
				if node.role == RoleLeader {
					node.mux.Lock()
					node.heartbeatAllMembers()
					node.mux.Unlock()
				}
			}
		}
		node.stop_end_c <- true
	}()
}

func (node *Node)Tick(timeElapseMs int){
	node.mux.Lock()
	defer node.mux.Unlock()

	if node.role == RoleFollower || node.role == RoleCandidate {
		if node.conf.joined {
			node.electionTimer += timeElapseMs
			if node.electionTimer >= ElectionTimeout {
				node.startPreVote()
			}
		}
	} else if node.role == RoleLeader {
		for _, m := range node.conf.members {
			m.ReceiveTimeout += timeElapseMs
			m.ReplicateTimer += timeElapseMs
			m.HeartbeatTimer += timeElapseMs

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
				// send snapshot?
				if node.logs.LastIndex() - m.MatchIndex > MaxBinlogGapToInstallSnapshot {
					log.Printf("Member %s, send snapshot", m.Id)
					node.sendSnapshot(m.Id)
				} else {
					node.heartbeatMember(m)
				}
			}
		}
	}
}

func (node *Node)startPreVote(){
	node.role = RoleFollower
	node.electionTimer = 0
	node.votesReceived = make(map[string]string)
	node.broadcast(NewPreVoteMsg())
	log.Printf("Node %s start prevote at term %d", node.Id(), node.Term())
}

func (node *Node)startElection(){
	node.role = RoleCandidate
	node.electionTimer = rand.Intn(200)
	node.votesReceived = make(map[string]string)
	node.resetMembers()
	node.conf.SetRound(node.Term() + 1, node.Id())
	node.broadcast(NewRequestVoteMsg())
	log.Printf("Node %s start election at term %d", node.Id(), node.Term())
}

func (node *Node)becomeFollower(){
	log.Printf("Node %s became follower at term %d", node.Id(), node.Term())
	node.role = RoleFollower
	node.electionTimer = 0	
	node.resetMembers()
}

func (node *Node)becomeLeader(){
	log.Printf("Node %s became leader at term %d", node.Id(), node.Term())
	node.role = RoleLeader
	node.electionTimer = 0
	node.resetMembers()

	if node.Term() == 1 { 
		// 初始化集群成员列表
		for _, id := range node.conf.peers {
			node.logs.AppendEntry(EntryTypeConf, fmt.Sprintf("AddMember %s", id))
		}
	} else {
		node.logs.AppendEntry(EntryTypeNoop, "")
	}
}

/* ############################################# */

func (node *Node)resetMembers() {
	nextIndex := node.logs.LastIndex() + 1
	for _, m := range node.conf.members {
		m.Reset()
		m.NextIndex = nextIndex
	}
}

func (node *Node)heartbeatAllMembers() {
	for _, m := range node.conf.members {
		if m.NextIndex > node.logs.LastIndex() {
			node.heartbeatMember(m)
		}
	}
}

func (node *Node)heartbeatMember(m *Member){
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	prev := node.logs.LastEntry()
	ent := NewBeatEntry(node.CommitIndex())
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
		ent.Commit = node.CommitIndex()
		
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

	// MUST: smaller msg.Term is rejected or ignored
	if msg.Term < node.Term() {
		log.Println("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term())
		node.send(NewGossipMsg(msg.Src))
		return
	}
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
	}
	for _, m := range node.conf.members {
		if m.Role == RoleLeader && m.ReceiveTimeout < ReceiveTimeout {
			log.Printf("leader %s is still reachable, ignore PreVote from %s", m.Id, msg.Src)
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
	if msg.PrevIndex < node.logs.LastIndex() {
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

func (node *Node)setLeader(leaderId string) {
	if node.leader == leaderId {
		return
	}
	if node.leader != "" {
		node.conf.members[node.leader].Role = RoleFollower
	}
	node.leader = leaderId
	node.conf.members[node.leader].Role = RoleLeader
}

func (node *Node)handleAppendEntry(msg *Message){
	node.electionTimer = 0
	node.setLeader(msg.Src)

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
		if ent.Index <= node.CommitIndex() {
			log.Printf("entry: %d not after commit: %d", ent.Index, node.CommitIndex())
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
		node.logs.WriteEntry(ent)
	}

	commitIndex := util.MinInt64(ent.Commit, node.logs.LastIndex())
	node.commitEntry(commitIndex)
}

func (node *Node)handleAppendEntryAck(msg *Message) {
	if node.logs.LastIndex() - msg.PrevIndex > MaxBinlogGapToInstallSnapshot {
		log.Printf("Member %s sync broken, prevIndex %d, lastIndex %d, maxGap: %d",
			msg.Src, msg.PrevIndex, node.logs.LastIndex(), MaxBinlogGapToInstallSnapshot)
		return
	}
	if msg.PrevIndex > node.logs.LastIndex() {
		log.Printf("Member %s bad msg, prevIndex %d > lastIndex %d",
			msg.Src, msg.PrevIndex, node.logs.LastIndex())
		return
	}

	m := node.conf.members[msg.Src]
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
		if m.MatchIndex > node.CommitIndex() {
			node.advanceCommitIndex()
		}
	}
	node.replicateMember(m)
}

func (node *Node)advanceCommitIndex() {
	// sort matchIndex[] in descend order
	matchIndex := make([]int64, 0, len(node.conf.members) + 1)
	matchIndex = append(matchIndex, node.logs.LastIndex()) // self
	for _, m := range node.conf.members {
		matchIndex = append(matchIndex, m.MatchIndex)
	}
	sort.Slice(matchIndex, func(i, j int) bool{
		return matchIndex[i] > matchIndex[j]
	})
	commitIndex := matchIndex[len(matchIndex)/2]
	log.Println("match[] =", matchIndex, " major matchIndex", commitIndex)

	commitIndex = util.MinInt64(commitIndex, node.logs.LastIndex())
	if commitIndex > node.CommitIndex() {
		ent := node.logs.GetEntry(commitIndex)
		// only commit currentTerm's log
		if ent.Term == node.Term() {
			node.commitEntry(commitIndex)
			node.commit_c <- true
		}
	}
}

func (node *Node)commitEntry(commitIndex int64) {
	if commitIndex <= node.CommitIndex() {
		return
	}
	log.Printf("%s commit %d => %d", node.Id(), node.CommitIndex(), commitIndex)
	node.conf.commit = commitIndex

	// synchronously apply to Config
	for index := node.conf.applied + 1; index <= node.CommitIndex(); index ++ {
		ent := node.logs.GetEntry(index)
		if ent == nil {
			log.Fatalf("Lost entry@%d", index)
		}
		node.conf.ApplyEntry(ent)
	}

	// TODO: asynchronously apply to Service
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
	node.mux.Lock()
	defer node.mux.Unlock()
	
	if node.role != RoleLeader {
		log.Println("error: not leader")
		return -1, -1
	}
	// TODO: use pending channel
	for node.conf.commit <= node.logs.LastIndex() - MaxPendingLogs {
		node.mux.Unlock()
		util.Sleep(0.001)
		node.mux.Lock()
	}
	
	ent := node.logs.AppendEntry(etype, data)
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
	node.mux.Lock()
	defer node.mux.Unlock()

	last := node.logs.LastEntry()
	
	var ret string
	ret += fmt.Sprintf("id: %s\n", node.Id())
	ret += fmt.Sprintf("role: %s\n", node.role)
	ret += fmt.Sprintf("term: %d\n", node.conf.term)
	ret += fmt.Sprintf("commit: %d\n", node.conf.commit)
	ret += fmt.Sprintf("lastTerm: %d\n", last.Term)
	ret += fmt.Sprintf("lastIndex: %d\n", last.Index)
	ret += fmt.Sprintf("vote: %s\n", node.conf.vote)
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
	if node.leader != "" {
		node.send(NewAppendEntryAck(node.leader, true))
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

	node.role = RoleFollower
	node.leader = ""
	node.electionTimer = 0	
	node.votesReceived = make(map[string]string)

	node.conf.RecoverFromSnapshot(sn)
	node.logs.RecoverFromSnapshot(sn)

	last := node.logs.LastEntry()
	log.Printf("Reset %s, peers: %s, term: %d, commit: %d, lastTerm: %d, lastIndex: %d",
		node.conf.id, node.conf.peers, node.conf.term, node.conf.commit,
		last.Term, last.Index)

	log.Printf("Done")
}
