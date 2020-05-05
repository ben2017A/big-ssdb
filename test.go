package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"strings"

	"raft"
	"raft/server"
	"redis"
)

var node *raft.Node
var xport *redis.Transport

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}
	nodeId := fmt.Sprintf("%d", port)

	// /////////////////////////////////////
	base_dir := "./tmp/" + nodeId

	if err := os.MkdirAll(base_dir, 0755); err != nil {
		log.Fatalf("Failed to make dir %s, %s", base_dir, err)
	}

	xport = redis.NewTransport("127.0.0.1", port+1000)
	if xport == nil {
		log.Fatalf("Failed to start redis port")
		return
	}
	defer xport.Close()
	log.Println("Redis server started at", port+1000)

	id_addr := make(map[string]string)
	id_addr["8001"] = "127.0.0.1:8001"
	id_addr["8002"] = "127.0.0.1:8002"

	members := make([]string, 0)
	for k, _ := range id_addr {
		members = append(members, k)
	}

	conf := raft.OpenConfig(base_dir)
	if conf.IsNew() {
		conf.Init(nodeId, members)
	}
	logs := raft.OpenBinlog(base_dir)
	node = raft.NewNode(conf, logs)
	node.Start()
	defer node.Close()

	raft_xport := server.NewUdpTransport("127.0.0.1", port)
	for k, v := range id_addr {
		raft_xport.Connect(k, v)
	}
	defer raft_xport.Close()
	log.Println("Raft server started at", port)

	go func() {
		for {
			req := <- xport.C
			if req == nil {
				break
			}
			Process(req)
		}
	}()
	go func() {
		for {
			msg := <- node.SendC()
			if msg == nil {
				break
			}
			raft_xport.Send(msg)
		}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	quit := false
	for !quit {
		select{
		case <- c:
			quit = true
		case msg := <- raft_xport.C():
			node.RecvC() <- msg
		}
	}
}

func Process(req *redis.Request) {
	resp := new(redis.Response)
	resp.Dst = req.Src

	cmd := strings.ToLower(req.Cmd())

	if cmd == "command" {
		resp.ReplyError("not implemented")
	} else if cmd == "info" {
		resp.ReplyBulk(node.Info())
	} else {
		t, i := node.Propose(req.Encode())
		log.Println("Propose", t, i, cmd)
		if t == -1 {
			resp.ReplyError("Propose failed")
		}
		// TODO: send reply after applied, not now
	}
	xport.Send(resp)
}
