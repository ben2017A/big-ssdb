package raft

import (
	"fmt"
	"sort"
	"math/rand"
	"time"
	"sync"
	"encoding/json"
	"glog"
	"util"
)

type RoleType string

const(
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
)

// TODO: NodeOption
const(
	TickerInterval   = 100
	ElectionTimeout  = 5 * 1000
	ReplicateTimeout = 1 * 1000
	HeartbeatTimeout = ReplicateTimeout * 3

	SendWindowSize     = 3
	MaxUncommittedSize = SendWindowSize
	MaxFallBehindSize  = 5
)

// Node is lightweighted
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
	// Binlog stores commitIndex in every log entry, which may not be up to date
	node.logs.commitIndex = node.conf.applied

	node.reset()

	return node
}

func (node *Node)Id() string {
	return node.conf.id
}

func (node *Node)Term() int32 {
	return node.conf.term
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
	glog.Info("Start %s, peers: %s, term: %d, commit: %d, accept: %d",
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
	glog.Info("Stopping %s...", node.Id())
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
	glog.Info("Stopped %s", node.Id())
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

	signal_consume := func(ch chan bool, consume func()) {
		defer func() {
			node.stop_end_c <- true
		}()
		for {
			if b := <- ch; b == false {
				return
			}
			// clear channel, multi signals are treated as one
			for len(ch) > 0 {
				b := <- ch
				if !b {
					return
				}
			}
			consume()
		}
	}

	go signal_consume(node.logs.accept_c, node.onAccept)
	go signal_consume(node.logs.commit_c, node.onCommit)
}

func (node *Node)Tick(timeElapseMs int) {
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
			m.IdleTimer += timeElapseMs
			m.ReplicateTimer += timeElapseMs
			m.HeartbeatTimer += timeElapseMs

			if m.HeartbeatTimer >= HeartbeatTimeout {
				// only send snapshot when follower responses within HeartbeatTimeout * 2
				if m.Connected() && m.IdleTimer <= HeartbeatTimeout * 2 {
					if node.logs.CommitIndex() - m.MatchIndex > MaxFallBehindSize {
						m.HeartbeatTimer = 0
						glog.Info("Member %s, send snapshot", m.Id)
						node.sendSnapshot(m.Id)
					}
				}
			}

			if m.ReplicateTimer >= ReplicateTimeout {
				// only send data to follower when
				// 1. we ever received an ack(may be to a heartbeat)
				// 2. it is not idle
				// 3. it is not fall behind
				if m.Connected() && m.IdleTimer < HeartbeatTimeout {
					if node.logs.CommitIndex() - m.MatchIndex <= MaxFallBehindSize {
						if m.UnackedSize() != 0 {
							glog.Info("Member: %s retransmission timeout, reset next: %d => %d",
								m.Id, m.NextIndex, m.MatchIndex + 1)
							m.NextIndex = m.MatchIndex + 1
						}
						node.replicateMember(m)
					}
				}
			}

			if m.HeartbeatTimer >= HeartbeatTimeout {
				node.heartbeatMember(m)
			}
		}
	}
}

func (node *Node)onAccept() {
	node.Lock()
	defer node.Unlock()
	// 单节点运行
	if node.conf.IsSingleton() && node.role == RoleLeader {
		node.advanceCommitIndex()
	} else {
		if node.role == RoleLeader {
			for _, m := range node.conf.members {
				node.replicateMember(m)
			}
		} else {
			node.sendAppendEntryAck()
		}
	}
}

func (node *Node)onCommit() {
	node.Lock()
	defer node.Unlock()
	if node.role == RoleLeader {
		for _, m := range node.conf.members {
			// will not heartbeat when there is pending entry to be sent
			if m.NextIndex > node.logs.AcceptIndex() {
				node.heartbeatMember(m)
			}
		}
	}
}

func (node *Node)reset() {
	node.leader = nil
	node.electionTimer = rand.Intn(200)
	node.votesReceived = make(map[string]string)
	node.resetMembers()
}

func (node *Node)startPreVote() {
	node.reset()
	node.role = RoleFollower
	glog.Info("Node %s start prevote at term %d", node.Id(), node.Term())
	node.broadcast(NewPreVoteMsg())
}

func (node *Node)startElection() {
	node.reset()
	node.role = RoleCandidate
	node.conf.SetRound(node.Term() + 1, node.Id())
	glog.Info("Node %s start election at term %d", node.Id(), node.Term())
	node.broadcast(NewRequestVoteMsg())
}

func (node *Node)becomeFollower() {
	node.reset()
	node.role = RoleFollower
	glog.Info("Node %s became follower at term %d", node.Id(), node.Term())
}

func (node *Node)becomeLeader() {
	node.reset()
	node.role = RoleLeader
	glog.Info("Node %s became leader at term %d", node.Id(), node.Term())

	if node.Term() == 1 { 
		// store cluster members in binlog, as very first entry(s)
		for _, id := range node.conf.peers {
			node.logs.Append(node.Term(), EntryTypeConf, fmt.Sprintf("AddMember %s", id))
		}
	} else {
		node.logs.Append(node.Term(), EntryTypeNoop, "")
	}
	// immediately check if followers are alive, then send data after acked
	node.heartbeatAllMembers()
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
		m.HeartbeatTimer = 0
		m.ReplicateTimer = 0
	}
	pre := node.logs.LastEntry()
	ent := NewHearteatEntry(node.logs.CommitIndex())
	node.broadcast(NewAppendEntryMsg("", ent, pre))
}

func (node *Node)heartbeatMember(m *Member){
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	pre := node.logs.LastEntry()
	ent := NewHearteatEntry(node.logs.CommitIndex())
	node.send(NewAppendEntryMsg(m.Id, ent, pre))
}

func (node *Node)replicateMember(m *Member) {
	if !m.Connected() {
		return
	}
	if m.UnackedSize() >= SendWindowSize {
		glog.Info("member %s sending window full, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
		return
	}
	m.ReplicateTimer = 0

	maxIndex := m.MatchIndex + SendWindowSize
	for m.NextIndex <= maxIndex {
		ent := node.logs.GetEntry(m.NextIndex)
		if ent == nil {
			break
		}
		ent.Commit = node.logs.CommitIndex()
		
		pre := node.logs.GetEntry(m.NextIndex - 1)
		node.send(NewAppendEntryMsg(m.Id, ent, pre))
		
		m.NextIndex ++
		m.HeartbeatTimer = 0
	}
}

/* ############################################# */

func (node *Node)handleRaftMessage(msg *Message){
	m := node.conf.members[msg.Src]
	if msg.Dst != node.Id() || m == nil {
		glog.Info("%s drop message %s => %s, peers: %s", node.Id(), msg.Src, msg.Dst, node.conf.peers)
		return
	}
	m.IdleTimer = 0

	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term() {
		glog.Info("Node %s receive greater msg.term: %d, node.term: %d", node.Id(), msg.Term, node.Term())
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
			glog.Infoln("drop message", msg.Encode())
		}
		return
	}

	// MUST: smaller msg.Term is rejected
	if msg.Term < node.Term() {
		glog.Infoln("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term())
		node.send(NewGossipMsg(msg.Src))
		return
	}

	if node.role == RoleCandidate {
		if msg.Type == MessageTypeRequestVoteAck {
			node.handleRequestVoteAck(msg)
		} else {
			glog.Infoln("drop message", msg.Encode())
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
			glog.Infoln("drop message", msg.Encode())
		}
		return
	}
}

func (node *Node)handlePreVote(msg *Message){
	if node.role == RoleLeader {
		arr := make([]int, 0, len(node.conf.members) + 1)
		arr = append(arr, 0) // self
		for _, m := range node.conf.members {
			arr = append(arr, m.IdleTimer)
		}
		sort.Ints(arr)
		glog.Debugln("    receive timeouts[] =", arr)
		timer := arr[len(arr)/2]
		if timer < HeartbeatTimeout {
			glog.Debug("    major followers are still reachable, ignore")
			return
		}
	} else {
		if node.leader != nil && node.leader.IdleTimer < HeartbeatTimeout {
			glog.Info("leader %s is still reachable, ignore PreVote from %s", node.leader.Id, msg.Src)
			return
		}
	}
	glog.Info("Node %s grant prevote to %s", node.Id(), msg.Src)
	node.send(NewPreVoteAck(msg.Src))
}

func (node *Node)handlePreVoteAck(msg *Message){
	glog.Info("Node %s receive prevote ack from %s", node.Id(), msg.Src)
	node.votesReceived[msg.Src] = "grant"
	if len(node.votesReceived) + 1 > (len(node.conf.members) + 1)/2 {
		node.startElection()
	}
}

func (node *Node)handleRequestVote(msg *Message){
	// only vote once at one term, if the candidate does not receive ack,
	// it should start a new election
	if node.conf.vote != "" /*&& node.conf.vote != msg.Src*/ {
		// just ignore
		glog.Infoln("already vote for", node.conf.vote, "ignore", msg.Src)
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
		glog.Info("Node %s vote for %s at term %d", node.Id(), msg.Src, node.Term())
	} else {
		node.send(NewRequestVoteAck(msg.Src, false))
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	glog.Info("Node %s receive %s vote ack from %s", node.Id(), msg.Data, msg.Src)
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
		glog.Info("grant: %d, reject: %d, total: %d", grant, reject, len(node.conf.members)+1)
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
			glog.Infoln("prev entry not found", msg.PrevTerm, msg.PrevIndex)
			node.sendDuplicatedAckToMessage(msg)
			return
		}
		if prev.Term != msg.PrevTerm {
			glog.Infoln("entry index: %d, prev.Term %d != msg.PrevTerm %d", msg.PrevIndex, prev.Term, msg.PrevTerm)
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
			glog.Info("invalid entry: %d before commit: %d", ent.Index, node.logs.CommitIndex())
			node.sendDuplicatedAckToMessage(msg)
			return
		}
		node.logs.Write(ent)
	}
	if ent.Commit > node.logs.CommitIndex() {
		node.tryCommit(ent.Commit)
	}
}

func (node *Node)handleAppendEntryAck(m *Member, msg *Message) {
	if node.logs.CommitIndex() - msg.PrevIndex > MaxFallBehindSize {
		glog.Info("Member %s sync broken, prevIndex %d, commit %d, MaxFallBehindSize: %d",
			msg.Src, msg.PrevIndex, node.logs.CommitIndex(), MaxFallBehindSize)
		m.MatchIndex = msg.PrevIndex
		return
	}
	if msg.PrevIndex > node.logs.AcceptIndex() {
		glog.Info("Member %s bad msg, prevIndex %d > lastIndex %d",
			msg.Src, msg.PrevIndex, node.logs.AcceptIndex())
		return
	}

	if msg.Data == "false" {
		if m.NextIndex != msg.PrevIndex + 1 {
			glog.Info("Member %s, reset nextIndex: %d -> %d", m.Id, m.NextIndex, msg.PrevIndex + 1)
			m.MatchIndex = msg.PrevIndex
			m.NextIndex  = msg.PrevIndex + 1
		}
		return // do not send immediately, wait for replicate timer
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
	glog.Debugln("match[] =", matchIndex, " major matchIndex", commitIndex)

	commitIndex = util.MinInt64(commitIndex, node.logs.AcceptIndex())
	if commitIndex > node.logs.CommitIndex() {
		ent := node.logs.GetEntry(commitIndex)
		// only commit currentTerm's log
		if ent.Term == node.Term() {
			node.tryCommit(commitIndex)
		}
	}
}

func (node *Node)handleInstallSnapshot(msg *Message) {
	node.loadSnapshot(msg.Data)
	node.send(NewAppendEntryAck(msg.Src, true))
}

func (node *Node)tryCommit(commitIndex int64) {
	oldIndex := node.logs.CommitIndex()

	// logs.Commit() may write to commit_c, which may block until
	// Node consumed commit_c, consuming commit_c requires holding the lock
	node.Unlock() // already locked by caller
	node.logs.Commit(commitIndex)
	node.Lock()   // will be unlocked by caller

	newIndex := node.logs.CommitIndex()
	if oldIndex == newIndex {
		return
	}
	glog.Info("%s commit %d => %d", node.Id(), oldIndex, newIndex)

	// synchronously apply to Config
	for index := node.conf.applied + 1; index <= node.logs.CommitIndex(); index ++ {
		ent := node.logs.GetEntry(index)
		if ent == nil {
			glog.Fatal("Lost entry@%d", index)
		}
		node.conf.ApplyEntry(ent)
	}
	// TODO: asynchronously apply to Service
}

/* ###################### Methods ####################### */

// 应该由调用者确保不会 propose 太快, 否则直接丢弃
func (node *Node)Propose(data string) (int32, int64) {
	return node._propose(EntryTypeData, data)
}

func (node *Node)_propose(etype EntryType, data string) (int32, int64) {
	// use a propose_lock?
	var term int32
	node.Lock()
	{
		if node.role != RoleLeader {
			node.Unlock()
			glog.Infoln("error: not leader")
			return -1, -1
		}
		// assign term to a proposing entry while holding lock, then assign index
		// inside logs.Append(). When a new entry with save index (sure with newer term)
		// received, the newer one will replace the old one.
		term = node.Term()

		// TODO: 直接丢弃?
		for node.logs.UncommittedSize() >= MaxUncommittedSize {
			glog.Info("sleep, append: %d, accept: %d commit: %d",
				node.logs.AppendIndex(), node.logs.AcceptIndex(), node.logs.CommitIndex())
			node.Unlock()
			util.Sleep(0.01)
			node.Lock()
		}
	}
	node.Unlock()

	// call logs.Append() outside of node.Lock(), because it may
	// need node.Lock() to proceed
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
// func (node *Node)ReadIndex() int64 {
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
	glog.Info("Node %s installing snapshot", node.Id())
	sn := new(Snapshot)
	if sn.Decode(data) == false {
		glog.Error("decode snapshot error")
		return
	}

	node.reset()
	node.role = RoleFollower

	node.conf.RecoverFromSnapshot(sn)
	node.logs.RecoverFromSnapshot(sn)

	glog.Info("Reset %s, peers: %s, term: %d, commit: %d, accept: %d",
			node.conf.id, node.conf.peers, node.conf.term,
			node.logs.CommitIndex(), node.logs.AcceptIndex())

	glog.Info("Done")
}
