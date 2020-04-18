package raft

import (
	"testing"
	"log"
	"fmt"
	"time"
	"sync"
)

/*
测试思路:
等待集群各节点同步后, 观察各节点的 commitIndex 是否相同, 若相同则认为各状态机是一致的.
*/

var n1 *Node
var n2 *Node
var mutex sync.Mutex
var nodes map[string]*Node

func TestNode(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	nodes = make(map[string]*Node)

	go networking()

	fmt.Printf("\n=========================================================\n")
	testOneNode()
	testJoin()
	testQuit()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testTwoNodes()
	testQuit()
	clean_nodes()

	fmt.Printf("\n=========================================================\n")
	testSnapshot()
	clean_nodes()

	log.Println("end")
}

// 单节点集群
func testOneNode() {
	mutex.Lock()
	{
		n1 = NewNode(NewConfig("n1", []string{"n1"}))
		nodes[n1.Id()] = n1
	}
	mutex.Unlock()

	n1.Start()
	if n1.role != RoleLeader {
		log.Fatal("error")
	}

	sleep(0.01) // wait commit
	if n1.commitIndex != 1 {
		log.Fatal("error")
	}
}

// 以相同的配置启动双节点集群
func testTwoNodes() {
	mutex.Lock()
	{
		members := []string{"n1", "n2"}
		n1 = NewNode(NewConfig("n1", members))
		n2 = NewNode(NewConfig("n2", members))
		nodes[n1.Id()] = n1
		nodes[n2.Id()] = n2
	}
	mutex.Unlock()

	n1.Start()
	n2.Start()
	n1.Tick(ElectionTimeout) // n1 start election
	
	sleep(0.02) // wait log replication
	if n1.role != RoleLeader {
		log.Fatal("error")
	}
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	if n1.commitIndex != 2 {
		log.Fatal("error")
	}
	if n1.commitIndex != n2.commitIndex {
		log.Fatal("error")	
	}
}

// 新节点加入集群
func testJoin() {
	n1.ProposeAddMember("n2")
	sleep(0.01)
	if n1.conf.members["n2"] == nil {
		log.Println("error")
	}

	mutex.Lock()
	{
		n2 = NewNode(NewConfig("n2", []string{"n1"}))
		nodes[n2.Id()] = n2
	}
	mutex.Unlock()

	n2.Start()
	n1.Tick(HeartbeatTimeout) // leader send ping
	
	sleep(0.01) // wait repication

	if n2.role != RoleFollower {
		log.Fatal("error")
	}
	if n1.commitIndex != n2.commitIndex {
		log.Fatal("error")	
	}
}

// 退出集群
func testQuit() {
	n1.ProposeDelMember("n2")
	sleep(0.01)
	if n1.conf.members["n2"] != nil {
		log.Fatal("error")
	}

	n2.Tick(ElectionTimeout) // start election
	sleep(0.01)
	if n2.role != RoleFollower {
		log.Fatal("error")
	}
}

// 落后太多时, 同步 Raft 快照
func testSnapshot() {
	testOneNode()
	n1.Propose("a")
	n1.Propose("b")

	testJoin()

	idx := n2.commitIndex
	n1.Propose("c")

	sleep(0.01) // wait replication

	if n2.commitIndex != idx + 1 {
		log.Fatal("error")	
	}
}

//////////////////////////////////////////////////////////////////

func sleep(second float32){
	time.Sleep((time.Duration)(second * 1000) * time.Millisecond)
}

func clean_nodes(){
	sleep(0.01) // wait proceed commit

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
		for id, n := range nodes {
			n.Tick(1)
			go dispatch(id)
		}
		mutex.Unlock()

		sleep(0.001)
	}
}
