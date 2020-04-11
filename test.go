package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	// "path/filepath"
	// "bytes"
	// "encoding/binary"

	"link"
	"raft"
	"server"
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

	id_addr := make(map[string]string)
	id_addr["8001"] = "127.0.0.1:8001"
	id_addr["8002"] = "127.0.0.1:8002"

	members := make([]string, 0)
	for k, _ := range id_addr {
		members = append(members, k)
	}

	conf := raft.NewConfig(nodeId, members)
	node := raft.NewNode(conf)
	node.Start()

	raft_xport := server.NewUdpTransport("127.0.0.1", port)
	for k, v := range id_addr {
		raft_xport.Connect(k, v)
	}

	for{
		select{
		case req := <-svc_xport.C:
			t, i := node.Propose(req.Encode())
			log.Println("Propose", t, i)
		case msg := <-raft_xport.C():
			node.RecvC() <- msg
		case msg := <-node.SendC():
			raft_xport.Send(msg)
		}
	}
}
