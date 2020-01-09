package main

import (
	// "fmt"
	"raft"
	"time"
	"log"
	// "math/rand"
	"os"
	"strconv"
	"strings"
)


const TimerInterval = 100

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}

	log.Println("api server started at", port+100)
	serv := raft.NewUdpTransport("127.0.0.1", port+100)
	defer serv.Stop()

	log.Println("raft server started at", port)
	transport := raft.NewUdpTransport("127.0.0.1", port)
	defer transport.Stop()

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	node := raft.NewNode()
	node.Role = "follower"

	if port == 8001 {
		node.Id = "n1"
		node.Members["n2"] = raft.NewMember("n2", "127.0.0.1:8002")
		transport.Connect("n2", "127.0.0.1:8002")
	}else{
		node.Id = "n2"
		node.Members["n1"] = raft.NewMember("n1", "127.0.0.1:8001")
		transport.Connect("n1", "127.0.0.1:8001")
	}

	node.Transport = transport

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case buf := <-transport.C:
			msg := raft.DecodeMessage(buf);
			if msg == nil {
				log.Println("decode error:", buf)
				continue
			}
			log.Printf("receive (%s)\n", strings.Trim(string(msg.Encode()), "\r\n"))
			node.HandleRaftMessage(msg)
		case buf := <-serv.C:
			s := string(buf)
			s = strings.Trim(s, "\r\n")
			ps := strings.Split(s, " ")
			log.Println(ps)
			if node.Role == "leader" {
				node.Write("set k 1")
			}
		}
	}
}

type UdpApi struct{

}
