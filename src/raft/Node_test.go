package raft

import (
	"testing"
	"fmt"
	"time"
	"os"
	log "glog"
	// "flag"
)

// go test -cover -run Node

var xport *MemTransport
var n1 *Node
var n2 *Node

func newNode(id string, peers []string) *Node {
	dir := "./tmp/" + id
	os.MkdirAll(dir, 0755)

	c := OpenConfig(dir)
	c.Clean()
	c.Init(id, peers)

	b := OpenBinlog(dir)
	b.Clean()

	n := NewNode(c, b)
	n.SetTransport(xport)
	xport.AddNode(n)
	n.Start()
	return n
}

func clean_nodes(){
	if xport != nil {
		xport.Close()
	}
	xport = NewMemTransport()
	xport.Listen("")
}

/*
测试思路:
等待集群各节点同步后, 观察各节点的 commitIndex 是否相同, 若相同则认为各状态机是一致的.
*/

func TestNode(t *testing.T){
	// defer log.Flush()

	clean_nodes()

	log.Debug("\n=========================================================\n")
	testOrphanNode()
	clean_nodes()

	log.Debug("\n=========================================================\n")
	testOneNode()
	fmt.Printf("\n")
	testJoin()
	fmt.Printf("\n")
	testQuit()
	clean_nodes()

	log.Debug("\n===========================test==============================\n")
	testTwoNodes()
	fmt.Printf("\n")
	testQuit()
	clean_nodes()

	log.Debug("\n=========================================================\n")
	testSnapshot()
	clean_nodes()

	log.Debug("\n=========================================================\n")
	testRestart()
	clean_nodes()

	log.Debug("end")
	fmt.Println("")
}

func testOrphanNode() {
	// 启动 leader
	testOneNode()

	// 启动孤儿节点
	n2 = newNode("n2", []string{})

	// 集群接受 n2
	n1.ProposeAddPeer("n2")
	wait()

	n1.Tick(HeartbeatTimeout)
	wait()

	// 集群的消息会被 n2 丢弃
	if len(n2.conf.peers) != 0 {
		log.Fatal("error")
	}
	if n2.CommitIndex() != 0 {
		log.Fatal("error")
	}
}

// 单节点集群
func testOneNode() {
	n1 = newNode("n1", []string{"n1"})

	if n1.role != RoleLeader {
		log.Fatal("error")
	}

	wait() // wait commit
	if n1.CommitIndex() != 1 {
		log.Fatal("error")
	}
}

// 以相同的配置启动双节点集群
func testTwoNodes() {
	members := []string{"n1", "n2"}
	n1 = newNode("n1", members)
	n2 = newNode("n2", members)

	n1.Tick(ElectionTimeout) // n1 start election
	wait()

	for i := 0; i < 10; i++ {
		n1.Propose(fmt.Sprintf("%d", i))
	}
	wait()

	if n1.role != RoleLeader {
		log.Fatal("error")
	}
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	if n1.CommitIndex() != n2.CommitIndex() {
		log.Fatal("error %d %d", n1.CommitIndex(), n2.CommitIndex())	
	}
	log.Debug("-----")
	// os.Exit(1)
}

// 新节点加入集群
func testJoin() {
	t, i := n1.ProposeAddPeer("n2")
	if t < 0 {
		log.Fatal("%d %d", t, i)
	}
	wait() // wait apply
	if n1.conf.members["n2"] == nil {
		log.Fatal("error")
	}

	n2 = newNode("n2", []string{"n1"}/*leader=n1*/)

	n1.Tick(HeartbeatTimeout) // heartbeat and logs
	wait()
	n1.Tick(HeartbeatTimeout) // heartbeat commit
	wait()

	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	if n1.CommitIndex() != n2.CommitIndex() {
		log.Fatalln("error", n1.CommitIndex(), n2.CommitIndex())	
	}
	log.Debug("-----")
}

// 退出集群
func testQuit() {
	t, i := n1.ProposeDelPeer("n2")
	log.Debug("%d %d", t, i)
	wait()
	if n1.conf.members["n2"] != nil {
		log.Fatal("error")
	}

	n2.Tick(ElectionTimeout) // start election
	wait()

	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	log.Debug("-----")
}

// 落后太多时, 同步 Raft 快照
func testSnapshot() {
	testOneNode()
	for i := 0; i < MaxFallBehindSize; i++ {
		n1.Propose(fmt.Sprintf("%d", i))
	}
	wait() // wait all propose done
	testJoin()

	n1.Tick(HeartbeatTimeout*1) // send snapshot
	wait() // wait replication

	idx := n2.CommitIndex()
	n1.Propose("c")
	wait() // wait replication
	log.Debug(n1.Info())

	if n2.CommitIndex() != idx + 1 {
		log.Fatal("error %d %d", idx, n2.CommitIndex())	
	}
	log.Debug("-----")
}

func testRestart() {
	n1 = newNode("n1", []string{"n1"})
	xport.DelNode(n1)

	n1 = NewNode(OpenConfig("./tmp/n1"), OpenBinlog("./tmp/n1"))
	n1.SetTransport(xport)
	n1.Start()
	xport.AddNode(n1)

	n1.Propose("set a 1")

	wait()
	if n1.CommitIndex() != 3 {
		log.Fatal("error")	
	}
	log.Debug("-----")
}

//////////////////////////////////////////////////////////////////

func wait(){
	time.Sleep(10 * time.Millisecond)
}

func sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}
