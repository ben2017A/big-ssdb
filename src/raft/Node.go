package raft

import (
	"fmt"
	"sort"
	"math/rand"
	// "time"
	"sync"
	"encoding/json"
	log "glog"
	"util"
)

// TODO: NodeOption
const(
	TickerInterval   = 100
	ElectionTimeout  = 5 * 1000
	ReplicateTimeout = 1 * 1000
	HeartbeatTimeout = ReplicateTimeout * 3

	MaxWindowSize      = 3
	MaxUncommittedSize = MaxWindowSize
	MaxFallBehindSize  = 5
)

// Node is lightweighted
type Node struct{
	sync.Mutex

	role PeerRole
	leader *Member // currently discovered leader
	votesReceived map[string]string // nodeId => vote result
	electionTimer int

	xport Transport
	conf *Config
	logs *Binlog

	stop_c chan bool
	done_c chan bool
	// messages to be processed by self
	recv_c chan *Message
	commit_c chan bool

	commitIndex int64
}

func NewNode(xport Transport, conf *Config, logs *Binlog) *Node {
	node := new(Node)
	node.stop_c = make(chan bool)
	node.done_c = make(chan bool)
	node.recv_c = make(chan *Message, 0/*TODO*/)
	node.commit_c = make(chan bool)

	node.xport = xport
	node.conf = conf
	node.conf.node = node
	node.logs = logs
	node.logs.node = node

	node.reset()
	node.role = RoleFollower
	node.commitIndex = node.conf.applied

	// validate persitent state
	if node.CommitIndex() > node.logs.AcceptIndex() {
		log.Fatal("Data corruption, commit: %d > accept: %d", node.CommitIndex(), node.logs.AcceptIndex())
	}
	
	return node
}

func (node *Node)Id() string {
	return node.conf.id
}

func (node *Node)Term() int32 {
	return node.conf.term
}

func (node *Node)CommitIndex() int64 {
	return node.commitIndex
}

func (node *Node)RecvC() chan<- *Message {
	return node.recv_c
}

func (node *Node)Start(){
	log.Info("Start %s, peers: %s, term: %d, commit: %d, accept: %d",
			node.conf.id, node.conf.peers, node.conf.term,
			node.CommitIndex(), node.logs.AcceptIndex())

	util.StartSignalConsumerThread(node.done_c, node.logs.accept_c, node.onAccept)
	util.StartSignalConsumerThread(node.done_c, node.commit_c, node.onCommit)
	util.StartTickerConsumerThread(node.stop_c, node.done_c, TickerInterval, node.Tick)
	util.StartThread(node.done_c, func(){
		for {
			msg := <- node.recv_c
			if msg == nil {
				break
			}
			node.onReceive(msg)
		}
	})
		
	// 单节点运行
	if node.conf.IsSingleton() {
		node.startElection()
		node.checkVoteResult()
	}
}

func (node *Node)Close(){
	log.Info("Stopping %s...", node.Id())

	// WARNING: 停止先后顺序有要求

	// 先停外界输入
	node.recv_c <- nil
	<- node.done_c

	// 停持久化 
	node.logs.Close()
	<- node.done_c

	// 停 commit worker
	node.commit_c <- false
	<- node.done_c

	// 停定时器
	node.stop_c <- true
	<- node.done_c

	node.conf.Close()

	log.Info("Stopped %s", node.Id())
}

func (node *Node)startWorkers(){
	util.StartSignalConsumerThread(node.done_c, node.logs.accept_c, node.onAccept)
	util.StartSignalConsumerThread(node.done_c, node.commit_c, node.onCommit)
	util.StartTickerConsumerThread(node.stop_c, node.done_c, TickerInterval, node.Tick)
	util.StartThread(node.done_c, func(){
		for {
			msg := <- node.recv_c
			if msg == nil {
				break
			}
			node.onReceive(msg)
		}
	})
}

func (node *Node)onAccept() {
	node.Lock()
	defer node.Unlock()
	if node.role == RoleLeader {
		// 单节点运行
		if node.conf.IsSingleton() {
			node.advanceCommitIndex()
		}
		for _, m := range node.conf.members {
			node.maybeReplicate(m)
		}
	} else {
		node.sendAppendEntryAck()
	}
}

func (node *Node)onCommit() {
	node.Lock()
	defer node.Unlock()
	if node.role == RoleLeader {
		for _, m := range node.conf.members {
			// will not heartbeat when there is pending entry to be sent
			if m.NextIndex > node.logs.AcceptIndex() {
				node.sendHeartbeat(m)
			}
		}
	}
}

func (node *Node)onReceive(msg *Message) {
	node.Lock()
	defer node.Unlock()
	m := node.conf.members[msg.Src]
	if msg.Dst != node.Id() || m == nil {
		log.Info("%s drop message %s => %s, peers: %s", node.Id(), msg.Src, msg.Dst, node.conf.peers)
		return
	}
	m.IdleTimer = 0
	m.HeartbeatTimer = 0
	node.handleRaftMessage(m, msg)
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
				m.HeartbeatTimer = 0
				if m.State == StateFallBehind {
					node.sendSnapshot(m)
				} else {
					node.sendHeartbeat(m)
				}
				m.State = StateHeartbeat
			}
			if m.ReplicateTimer >= ReplicateTimeout {
				m.ReplicateTimer = 0
				if m.UnackedSize() != 0 {
					log.Info("Member: %s retransmission timeout, reset next: %d => %d",
						m.Id, m.NextIndex, m.MatchIndex + 1)
					m.NextIndex = m.MatchIndex + 1
				}
				node.maybeReplicate(m)
			}
		}
	}
}

func (node *Node)reset() {
	node.leader = nil
	node.electionTimer = rand.Intn(ElectionTimeout/10)
	node.votesReceived = make(map[string]string)
	for _, m := range node.conf.members {
		m.Reset()
		m.NextIndex = node.logs.AcceptIndex() + 1
	}
}

func (node *Node)startPreVote() {
	node.reset()
	node.role = RoleFollower
	log.Info("Node %s start prevote at term %d", node.Id(), node.Term())
	node.broadcast(NewPreVoteMsg())
}

func (node *Node)startElection() {
	node.reset()
	node.role = RoleCandidate
	node.conf.SetRound(node.Term() + 1, node.Id())
	log.Info("Node %s start election at term %d", node.Id(), node.Term())
	node.broadcast(NewRequestVoteMsg())
}

func (node *Node)becomeFollower() {
	node.reset()
	node.role = RoleFollower
	log.Info("Node %s became follower at term %d", node.Id(), node.Term())
}

func (node *Node)becomeLeader() {
	node.reset()
	node.role = RoleLeader
	log.Info("Node %s became leader at term %d", node.Id(), node.Term())

	if node.Term() == 1 { 
		// store cluster members in binlog, as very first entry(s)
		for _, id := range node.conf.peers {
			node.logs.Append(node.Term(), EntryTypeConf, fmt.Sprintf("add_peer %s", id))
		}
	} else {
		node.logs.Append(node.Term(), EntryTypeNoop, "")
	}

	// immediately broadcast heartbeat
	pre := node.logs.LastEntry()
	ent := NewHearteatEntry(node.CommitIndex())
	node.broadcast(NewAppendEntryMsg("", ent, pre))
}

/* ############################################# */

func (node *Node)sendHeartbeat(m *Member) {
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	var pre *Entry = nil
	if m.MatchIndex > 0 {
		pre = node.logs.GetEntry(m.MatchIndex)
	}
	ent := NewHearteatEntry(node.CommitIndex())
	node.send(NewAppendEntryMsg(m.Id, ent, pre))
}

func (node *Node)maybeReplicate(m *Member) {
	if m.State != StateReplicate {
		return
	}
	// flow control
	if m.UnackedSize() >= m.WindowSize {
		log.Info("member %s sending window full, next: %d, match: %d", m.Id, m.NextIndex, m.MatchIndex)
		return
	}

	maxIndex := util.MinInt64(m.MatchIndex + m.WindowSize, node.logs.AcceptIndex())
	for m.NextIndex <= maxIndex {
		ent := node.logs.GetEntry(m.NextIndex)
		if ent == nil {
			break
		}
		ent.Commit = util.MinInt64(ent.Index, node.CommitIndex())
		
		prev := node.logs.GetEntry(m.NextIndex - 1)
		ok := node.send(NewAppendEntryMsg(m.Id, ent, prev))
		if !ok {
			// maybe it is because of flow control
			break
		}
		
		m.NextIndex ++
		m.ReplicateTimer = 0
		m.HeartbeatTimer = 0
	}
}

func (node *Node)sendSnapshot(m *Member){
	m.HeartbeatTimer = 0
	m.ReplicateTimer = 0
	data := node.makeSnapshot()
	resp := NewInstallSnapshotMsg(m.Id, data)
	node.send(resp)
	log.Info("Member %s, send snapshot", m.Id)
}

/* ############################################# */

func (node *Node)handleRaftMessage(m *Member, msg *Message){
	// MUST: node.Term is set to be larger msg.Term
	if msg.Term > node.Term() {
		log.Info("Node %s receive greater msg.term: %d, node.term: %d", node.Id(), msg.Term, node.Term())
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
			log.Infoln("drop message", msg.Encode())
		}
		return
	}

	// MUST: smaller msg.Term is rejected
	if msg.Term < node.Term() {
		log.Infoln("reject", msg.Type, "msg.Term =", msg.Term, " < node.term = ", node.Term())
		node.send(NewGossipMsg(msg.Src))
		return
	}

	if node.role == RoleCandidate {
		if msg.Type == MessageTypeRequestVoteAck {
			node.handleRequestVoteAck(msg)
		} else {
			log.Infoln("drop message", msg.Encode())
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
			log.Infoln("drop message", msg.Encode())
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
		log.Debugln("    receive timeouts[] =", arr)
		timer := arr[len(arr)/2]
		if timer < HeartbeatTimeout * 2 {
			log.Debug("    major followers are still reachable, ignore")
			return
		}
	} else {
		if node.leader != nil && node.leader.IdleTimer < HeartbeatTimeout {
			log.Info("leader %s is still reachable, ignore PreVote from %s", node.leader.Id, msg.Src)
			return
		}
	}
	log.Info("Node %s grant prevote to %s", node.Id(), msg.Src)
	node.send(NewPreVoteAck(msg.Src))
}

func (node *Node)handlePreVoteAck(msg *Message){
	log.Info("Node %s receive prevote ack from %s", node.Id(), msg.Src)
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
		log.Infoln("already vote for", node.conf.vote, "ignore", msg.Src)
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
		log.Info("Node %s vote for %s at term %d", node.Id(), msg.Src, node.Term())
	} else {
		node.send(NewRequestVoteAck(msg.Src, false))
	}
}

func (node *Node)handleRequestVoteAck(msg *Message){
	log.Info("Node %s receive %s vote ack from %s", node.Id(), msg.Data, msg.Src)
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
		log.Info("grant: %d, reject: %d, total: %d", grant, reject, len(node.conf.members)+1)
		node.becomeFollower()
	}
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
			log.Infoln("prev entry not found", msg.PrevTerm, msg.PrevIndex)
			node.sendDupAck()
			return
		}
		if prev.Term != msg.PrevTerm {
			log.Info("entry index: %d, prev.Term %d != msg.PrevTerm %d", msg.PrevIndex, prev.Term, msg.PrevTerm)
			node.sendDupAck()
			return
		}
	}

	ent := DecodeEntry(msg.Data)

	if ent.Type == EntryTypeBeat {
		node.sendAppendEntryAck()
	} else {
		if ent.Index <= node.CommitIndex() {
			log.Info("invalid entry: %d before commit: %d", ent.Index, node.CommitIndex())
			node.sendDupAck()
			return
		}
		// 注意, 任何涉及到 channel 操作的地方, 都要释放锁, 除非 channel 是无限大的
		node.Unlock()
		node.logs.Write(ent)
		node.Lock()
	}
	if ent.Commit > node.CommitIndex() {
		node.maybeCommit(ent.Commit)
	}
}

func (node *Node)handleAppendEntryAck(m *Member, msg *Message) {
	if node.CommitIndex() - msg.PrevIndex > MaxFallBehindSize {
		log.Info("Member %s fall behind, prevIndex %d, commit %d",
			msg.Src, msg.PrevIndex, node.CommitIndex())
		m.State = StateFallBehind
		m.MatchIndex = util.MaxInt64(m.MatchIndex, 0) // 可能初始化
		return
	}
	if msg.PrevIndex > node.logs.AcceptIndex() {
		log.Info("Member %s bad msg, prevIndex %d > lastIndex %d",
			msg.Src, msg.PrevIndex, node.logs.AcceptIndex())
		return
	}

	if msg.Data == "false" {
		m.State = StateHeartbeat
		// TODO: 记录 dupAckIndex, dupAckRepeats, 3 次再重传
		old := m.NextIndex
		m.WindowSize = 1 // congestion control
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		m.NextIndex  = util.MaxInt64(m.MatchIndex + 1,  msg.PrevIndex + 1)
		log.Info("Member %s, reset nextIndex: %d -> %d", m.Id, old, m.NextIndex)
	} else {
		m.State = StateReplicate
		m.WindowSize = util.MinInt64(m.WindowSize + 1, MaxWindowSize) // slow start
		m.MatchIndex = util.MaxInt64(m.MatchIndex, msg.PrevIndex)
		m.NextIndex  = util.MaxInt64(m.NextIndex,  msg.PrevIndex + 1)
		if m.MatchIndex > node.CommitIndex() {
			node.advanceCommitIndex()
		}
		node.maybeReplicate(m)
	}
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
	log.Debugln("match", matchIndex, " major matchIndex", commitIndex)

	if commitIndex > node.CommitIndex() {
		ent := node.logs.GetEntry(commitIndex)
		// only commit currentTerm's log
		if ent != nil && ent.Term == node.Term() {
			node.maybeCommit(commitIndex)
		}
	}
}

func (node *Node)maybeCommit(commitIndex int64) {
	commitIndex = util.MinInt64(commitIndex, node.logs.AcceptIndex())
	if commitIndex <= node.commitIndex {
		return
	}
	log.Info("%s commit %d => %d", node.Id(), node.commitIndex, commitIndex)
	node.commitIndex = commitIndex

	// synchronously apply to Config
	for index := node.conf.applied + 1; index <= node.CommitIndex(); index ++ {
		ent := node.logs.GetEntry(index)
		if ent == nil {
			log.Fatal("Lost entry@%d", index)
		}
		node.conf.ApplyEntry(ent)
	}

	// TODO: asynchronously apply to Service, in onCommit()?

	// 这里需要先释放锁, 因为 commit_c 的消费者需要获取锁
	// 除非 commit_c 是无限大的, 否则 commit_c 最终会阻塞
	node.Unlock() // already locked by caller
	node.commit_c <- true
	node.Lock()
}

func (node *Node)handleInstallSnapshot(msg *Message) {
	node.loadSnapshot(msg.Data)
	node.send(NewAppendEntryAck(msg.Src, true))
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
			log.Infoln("error: not leader")
			return -1, -1
		}
		// assign term to a proposing entry while holding lock, then assign index
		// inside logs.Append(). When a new entry with save index (sure with newer term)
		// received, the newer one will replace the old one.
		term = node.Term()

		// TODO: 直接丢弃?
		for node.logs.AppendIndex() - node.CommitIndex() >= MaxUncommittedSize {
			log.Info("sleep, append: %d, accept: %d commit: %d",
				node.logs.AppendIndex(), node.logs.AcceptIndex(), node.CommitIndex())
			node.Unlock()
			util.Sleep(0.001)
			node.Lock()
		}
	}
	node.Unlock()

	// call logs.Append() outside of node.Lock(), because it may
	// need node.Lock() to proceed
	ent := node.logs.Append(term, etype, data)
	return ent.Term, ent.Index
}

func (node *Node)ProposeAddPeer(nodeId string) (int32, int64) {
	data := fmt.Sprintf("add_peer %s", nodeId)
	return node._propose(EntryTypeConf, data)
}

func (node *Node)ProposeDelPeer(nodeId string) (int32, int64) {
	data := fmt.Sprintf("del_peer %s", nodeId)
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
	ret += fmt.Sprintf("commit: %d\n", node.CommitIndex())
	bs, _ := json.Marshal(node.conf.members)
	ret += fmt.Sprintf("members: %s\n", string(bs))

	return ret
}

/* ############################################# */

func (node *Node)send(msg *Message) bool {
	msg.Src = node.Id()
	msg.Term = node.Term()
	if msg.PrevTerm == 0 && msg.Type != MessageTypeAppendEntry {
		last := node.logs.LastEntry()
		msg.PrevTerm = last.Term
		msg.PrevIndex = last.Index
	}
	return node.xport.Send(msg)
}

func (node *Node)broadcast(msg *Message) {
	for _, m := range node.conf.members {
		msg.Dst = m.Id
		node.send(msg)
	}
}

func (node *Node)sendAppendEntryAck() {
	if node.leader == nil {
		return
	}
	node.send(NewAppendEntryAck(node.leader.Id, true))
}

func (node *Node)sendDupAck() {
	node.send(NewAppendEntryAck(node.leader.Id, false))
}

/* ###################### Snapshot ####################### */

func (node *Node)makeSnapshot() string {
	sn := MakeSnapshot(node)
	return sn.Encode()
}

func (node *Node)loadSnapshot(data string) {
	log.Info("Node %s installing snapshot", node.Id())
	sn := new(Snapshot)
	if sn.Decode(data) == false {
		log.Error("decode snapshot error")
		return
	}

	node.reset()
	node.role = RoleFollower
	node.commitIndex = sn.LastIndex()

	node.conf.RecoverFromSnapshot(sn)
	node.logs.RecoverFromSnapshot(sn)

	log.Info("Reset %s, peers: %s, term: %d, commit: %d, accept: %d",
			node.conf.id, node.conf.peers, node.conf.term,
			node.CommitIndex(), node.logs.AcceptIndex())

	log.Info("Done")
}
