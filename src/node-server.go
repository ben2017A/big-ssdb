package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"path/filepath"

	"raft"
	"store"
	"link"
	"server"
)

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}
	nodeId := fmt.Sprintf("%d", port)

	base_dir, _ := filepath.Abs(fmt.Sprintf("./tmp/%s", nodeId))

	/////////////////////////////////////

	log.Println("Raft server started at", port)
	db := store.OpenKVStore(base_dir + "/raft")
	raft_xport := raft.NewUdpTransport("127.0.0.1", port)
	node := raft.NewNode(nodeId, raft_xport.Addr(), db)

	log.Println("Service server started at", port+1000)
	svc_xport := link.NewTcpServer("127.0.0.1", port+1000)
	svc := server.NewService(base_dir, node, svc_xport)
	defer svc.Close()

	// testing
	raft_xport.Connect("8001", "127.0.0.1:8001")
	raft_xport.Connect("8002", "127.0.0.1:8002")

	for{
		select{
		case msg := <-svc_xport.C:
			svc.HandleClientMessage(msg)
		case msg := <-raft_xport.C():
			node.RecvC() <- msg
		case msg := <-node.SendC():
			raft_xport.Send(msg)
		}
	}
}
