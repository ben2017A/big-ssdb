package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	// "path/filepath"

	"raft"
	"link"
)

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}
	nodeId := fmt.Sprintf("%d", port)

	// /////////////////////////////////////

	// log.Println("Raft server started at", port)
	// db := store.OpenKVStore(base_dir + "/raft")
	// node := raft.NewNode(nodeId, raft_xport.Addr(), db)

	log.Println("Service server started at", port+1000)
	svc_xport := link.NewServer("127.0.0.1", port+1000)
	// svc := server.NewService(base_dir, node, svc_xport)
	// defer svc.Close()

	members := make(map[string]string)
	members["8001"] = "127.0.0.1:8001"
	members["8002"] = "127.0.0.1:8002"

	conf := raft.NewConfig(nodeId, "addr", members)
	node := raft.NewNode(conf)
	node.Start()

	// raft_xport.Connect("8001", "127.0.0.1:8001")
	// raft_xport.Connect("8002", "127.0.0.1:8002")
	raft_xport := raft.NewUdpTransport("127.0.0.1", port)
	for k, v := range members {
		raft_xport.Connect(k, v)
	}

	for{
		select{
		case req := <-svc_xport.C:
			t, i := node.Propose(req.Cmd())
			log.Println("Propose", t, i)
		case msg := <-raft_xport.C():
			node.RecvC() <- msg
		case msg := <-node.SendC():
			raft_xport.Send(msg)
		}
	}
}
