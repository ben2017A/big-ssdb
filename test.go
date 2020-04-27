package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	// "path/filepath"
	// "encoding/binary"

	"raft"
	"redis"
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
	base_dir := "./tmp/" + nodeId
	conf_dir := base_dir + "/config.wal"
	logs_dir := base_dir + "/config.wal"

	if err := os.MkdirAll(conf_dir, 0755); err != nil {
		log.Printf("Failed to make dir %s, %s", conf_dir, err)
		return nil
	}
	if err := os.MkdirAll(logs_dir, 0755); err != nil {
		log.Printf("Failed to make dir %s, %s", logs_dir, err)
		return nil
	}

	svc_xport := redis.NewTransport("127.0.0.1", port+1000)
	if svc_xport == nil {
		return
	}
	defer svc_xport.Close()
	log.Println("Redis server started at", port+1000)

	id_addr := make(map[string]string)
	id_addr["8001"] = "127.0.0.1:8001"
	id_addr["8002"] = "127.0.0.1:8002"

	members := make([]string, 0)
	for k, _ := range id_addr {
		members = append(members, k)
	}

	conf := raft.OpenConfig("./tmp/" + nodeId)
	if conf.IsNew() {
		conf.Init(nodeId, members)
	}
	// conf := raft.NewConfig(nodeId, members, "./tmp/" + nodeId)
	node := raft.NewNode(conf)
	node.Start()
	defer node.Close()

	raft_xport := server.NewUdpTransport("127.0.0.1", port)
	for k, v := range id_addr {
		raft_xport.Connect(k, v)
	}
	defer raft_xport.Close()
	log.Println("Raft server started at", port+1000)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	
	quit := false
	for !quit {
		select{
		case <- c:
			quit = true
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
