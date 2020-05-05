package raft

import (
	"testing"
	"log"
	"fmt"
	"time"
	"sync"
	"os"
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
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	os.MkdirAll(dir1, 0755)
	os.MkdirAll(dir2, 0755)

	nodes = make(map[string]*Node)

	go networking()

	fmt.Printf("\n=========================================================\n")
	testOrphanNode()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testOneNode()
	fmt.Printf("\n")
	testJoin()
	fmt.Printf("\n")
	testQuit()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testTwoNodes()
	fmt.Printf("\n")
	testQuit()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testSnapshot()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testRestart()
	clean_nodes()

	log.Println("end")
	fmt.Println("")
}

func testOrphanNode() {
	// 启动 leader
	testOneNode()

	// 启动孤儿节点
	mutex.Lock()
	{
		n2 = NewNode(newConfig("n2", []string{}, dir2), newBinlog(dir2))
		nodes[n2.Id()] = n2
		n2.Start()
	}
	mutex.Unlock()

	// 集群接受 n2
	n1.ProposeAddMember("n2")
	sleep()

	n1.Tick(HeartbeatTimeout)
	sleep()

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
	mutex.Lock()
	{
		n1 = NewNode(newConfig("n1", []string{"n1"}, dir1), newBinlog(dir1))
		nodes[n1.Id()] = n1
		n1.Start()
	}
	mutex.Unlock()

	if n1.role != RoleLeader {
		log.Fatal("error")
	}

	sleep() // wait commit
	if n1.CommitIndex() != 1 {
		log.Fatal("error")
	}
}

// 以相同的配置启动双节点集群
func testTwoNodes() {
	mutex.Lock()
	{
		members := []string{"n1", "n2"}
		n1 = NewNode(newConfig("n1", members, dir1), newBinlog(dir1))
		n2 = NewNode(newConfig("n2", members, dir2), newBinlog(dir2))
		nodes[n1.Id()] = n1
		nodes[n2.Id()] = n2
		n1.Start()
		n2.Start()
	}
	mutex.Unlock()

	n1.Tick(ElectionTimeout) // n1 start election
	
	sleep() // wait log replication

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
	log.Println("-----")
}

// 新节点加入集群
func testJoin() {
	n1.ProposeAddMember("n2")
	sleep()
	if n1.conf.members["n2"] == nil {
		log.Println("error")
	}

	mutex.Lock()
	{
		n2 = NewNode(newConfig("n2", []string{"n1"}, dir2), newBinlog(dir2))
		nodes[n2.Id()] = n2
		n2.Start()
	}
	mutex.Unlock()

	n1.Tick(HeartbeatTimeout) // leader send ping
	
	sleep() // wait repication

	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	// if n1.CommitIndex() != n2.CommitIndex() {
	// 	log.Fatal("error")	
	// }
	log.Println("-----")
}

// 退出集群
func testQuit() {
	n1.ProposeDelMember("n2")
	sleep()
	if n1.conf.members["n2"] != nil {
		log.Fatal("error")
	}

	n2.Tick(ElectionTimeout) // start election
	sleep()
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	log.Println("-----")
}

// 落后太多时, 同步 Raft 快照
func testSnapshot() {
	testOneNode()
	for i := 0; i < MaxUncommittedSize*10; i++ {
		// log.Println(i)
		n1.Propose(fmt.Sprintf("%d", i))
	}
	testJoin()

	n1.Tick(HeartbeatTimeout*1) // send snapshot
	sleep() // wait replication

	idx := n2.CommitIndex()
	n1.Propose("c")
	sleep() // wait replication

	if n2.CommitIndex() != idx + 1 {
		log.Fatal("error ", idx, n2.CommitIndex())	
	}
	log.Println("-----")
}

func testRestart() {
	mutex.Lock()
	{
		n1 = NewNode(newConfig("n1", []string{"n1"}, dir1), newBinlog(dir1))
		nodes[n1.Id()] = n1
		n1.Start()
		sleep()

		n1.Close()
		delete(nodes, "n1")

		n1 = NewNode(OpenConfig(dir1), OpenBinlog(dir1))
		nodes[n1.Id()] = n1
		n1.Start()
	}
	mutex.Unlock()

	n1.Propose("set a 1")

	sleep()
	if n1.CommitIndex() != 3 {
		log.Fatal("error")	
	}
	log.Println("-----")
}

//////////////////////////////////////////////////////////////////

func clean_nodes(){
	sleep() // wait proceed commit

	mutex.Lock()
	defer mutex.Unlock()

	for id, n := range nodes {
		n.Close()
		delete(nodes, id)
		// log.Printf("%s stopped", id)
	}
}

func dispatch(id string){
	mutex.Lock()
	defer mutex.Unlock()

	n := nodes[id]
	if n == nil {
		return
	}
	if len(n.SendC()) == 0 {
		return
	}

	msg := <- n.SendC()
	log.Println("    send > " + msg.Encode())

	if nodes[msg.Dst] != nil {
		// log.Println("    recv < " + msg.Encode())
		nodes[msg.Dst].RecvC() <- msg
	}
}

func networking() {
	for {
		mutex.Lock()
		for id, _ := range nodes {
			go dispatch(id)
		}
		mutex.Unlock()

		time.Sleep(100 * time.Microsecond)
	}
}

func sleep(){
	time.Sleep(10 * time.Millisecond)
}
