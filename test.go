package main

import (
	"fmt"
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
	nodeId := fmt.Sprintf("%d", port)

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	store := raft.OpenStorage(fmt.Sprintf("tmp/%s", nodeId))
	defer store.Close()

	log.Println("api server started at", port+100)
	serv := raft.NewUdpTransport("127.0.0.1", port+100)
	defer serv.Stop()

	log.Println("raft server started at", port)
	transport := raft.NewUdpTransport("127.0.0.1", port)
	defer transport.Stop()

	node := raft.NewNode(store, transport)
	node.Id = nodeId

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case buf := <-transport.C:
			msg := raft.DecodeMessage(string(buf));
			if msg == nil {
				log.Println("decode error:", buf)
				continue
			}
			node.HandleRaftMessage(msg)
		case buf := <-serv.C:
			s := string(buf)
			s = strings.Trim(s, "\r\n")
			ps := strings.Split(s, " ")

			if ps[0] == "JoinGroup" {
				node.JoinGroup(ps[1], ps[2])
				continue
			}

			if node.Role == "leader" {
				if ps[0] == "AddMember" {
					node.AddMember(ps[1], ps[2])
				}
				if ps[0] == "Write" {
					node.Write(ps[1])
				}
				// log.Println("reject client's request:", s)
				// continue
			}

		}
	}
}
