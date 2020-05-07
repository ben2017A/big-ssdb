package raft

import (
	"testing"
	"fmt"
	"time"
	"sync"
	"os"
	log "glog"
	// "flag"
)

// go test -cover -run Node

// 新配置, 如果指定目录已存在旧配置, 则先删除旧配置
func newConfig(id string, peers []string, dir string) *Config {
	c := OpenConfig(dir)
	c.Clean()
	c.Init(id, peers)
	return c
}

func newBinlog(dir string) *Binlog {
	b := OpenBinlog(dir)
	b.Clean()
	return b
}

/*
测试思路:
等待集群各节点同步后, 观察各节点的 commitIndex 是否相同, 若相同则认为各状态机是一致的.
*/

var n1 *Node
var n2 *Node
var mutex sync.Mutex
var nodes map[string]*Node
var dir1 string = "./tmp/n1"
var dir2 string = "./tmp/n2"

func TestNode(t *testing.T){
	// defer log.Flush()
	os.MkdirAll(dir1, 0755)
	os.MkdirAll(dir2, 0755)

	nodes = make(map[string]*Node)

	// fmt.Printf("\n=========================================================\n")
	// testOrphanNode()
	// clean_nodes()

	// fmt.Printf("\n=========================================================\n")
	// testOneNode()
	// fmt.Printf("\n")
	// testJoin()
	// fmt.Printf("\n")
	// testQuit()
	// clean_nodes()

	// fmt.Printf("\n=========================================================\n")
	// testTwoNodes()
	// fmt.Printf("\n")
	// testQuit()
	// clean_nodes()

	// fmt.Printf("\n=========================================================\n")
	// testSnapshot()
	// clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testRestart()
	clean_nodes()

	log.Info("end")
	fmt.Println("")
}

func testOrphanNode() {
	// 启动 leader
	testOneNode()

	// 启动孤儿节点
	n2 = NewNode(newConfig("n2", []string{}, dir2), newBinlog(dir2))
	add_node(n2)

	// 集群接受 n2
	n1.ProposeAddPeer("n2")
	wait()

	log.Info("")
	n1.Tick(HeartbeatTimeout)
	log.Info("")
	wait()
	log.Info("")

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
	n1 = NewNode(newConfig("n1", []string{"n1"}, dir1), newBinlog(dir1))
	add_node(n1)

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
	n1 = NewNode(newConfig("n1", members, dir1), newBinlog(dir1))
	n2 = NewNode(newConfig("n2", members, dir2), newBinlog(dir2))
	add_node(n1)
	add_node(n2)

	n1.Tick(ElectionTimeout) // n1 start election
	
	wait() // wait log replication

	if n1.role != RoleLeader {
		log.Fatal("error")
	}
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	if n1.CommitIndex() != 2 {
		log.Fatal("error")
	}
	if n1.CommitIndex() != n2.CommitIndex() {
		log.Fatal("error ", n1.CommitIndex(), n2.CommitIndex())	
	}
	log.Info("-----")
}

// 新节点加入集群
func testJoin() {
	n1.ProposeAddPeer("n2")
	wait()
	if n1.conf.members["n2"] == nil {
		log.Info("error")
	}

	n2 = NewNode(newConfig("n2", []string{"n1"}, dir2), newBinlog(dir2))
	add_node(n2)

	n1.Tick(HeartbeatTimeout) // leader send ping
	
	wait() // wait repication

	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	// if n1.CommitIndex() != n2.CommitIndex() {
	// 	log.Fatal("error")	
	// }
	log.Info("-----")
}

// 退出集群
func testQuit() {
	n1.ProposeDelPeer("n2")
	wait()
	if n1.conf.members["n2"] != nil {
		log.Fatal("error")
	}

	n2.Tick(ElectionTimeout) // start election
	wait()
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	log.Info("-----")
}

// 落后太多时, 同步 Raft 快照
func testSnapshot() {
	testOneNode()
	for i := 0; i < MaxFallBehindSize; i++ {
		// log.Info(i)
		n1.Propose(fmt.Sprintf("%d", i))
	}
	testJoin()

	n1.Tick(HeartbeatTimeout*1) // send snapshot
	wait() // wait replication

	idx := n2.CommitIndex()
	n1.Propose("c")
	wait() // wait replication
	log.Info(n1.Info())

	if n2.CommitIndex() != idx + 1 {
		log.Fatal("error ", idx, n2.CommitIndex())	
	}
	log.Info("-----")
}

func testRestart() {
	n1 = NewNode(newConfig("n1", []string{"n1"}, dir1), newBinlog(dir1))
	add_node(n1)

	mutex.Lock()
	{
		n1.Close()
		delete(nodes, "n1")
	}
	mutex.Unlock()

	n1 = NewNode(OpenConfig(dir1), OpenBinlog(dir1))
	add_node(n1)

	n1.Propose("set a 1")

	wait()
	if n1.CommitIndex() != 3 {
		log.Fatal("error")	
	}
	log.Info("-----")
}

//////////////////////////////////////////////////////////////////

func add_node(n *Node){
	mutex.Lock()
	defer mutex.Unlock()
	
	n.Start()
	nodes[n.Id()] = n

	go func(){
		for {
			msg := <- n.SendC()
			if msg == nil {
				return
			}
			log.Info("    send > " + msg.Encode())
		
			go func(){
				mutex.Lock()
				defer mutex.Unlock()
				// log.Info("    recv < " + msg.Encode())
				if nodes[msg.Dst] != nil {
					nodes[msg.Dst].RecvC() <- msg
				}
			}()
		}
	}()
}

func clean_nodes(){
	mutex.Lock()
	defer mutex.Unlock()

	for id, n := range nodes {
		n.Close()
		delete(nodes, id)
		// log.Printf("%s stopped", id)
	}
}

func wait(){
	time.Sleep(10 * time.Millisecond)
}

func sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}
