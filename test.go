package main

import (
	"fmt"
	"net"
	"raft"
	"time"
	"log"
	"math/rand"
)

type UdpTransport struct{
	C chan []byte
	conn *net.UDPConn
}

func NewUdpTransport(ip string, port int) (*UdpTransport){
	s := fmt.Sprintf("%s:%d", ip, port)
	addr, _ := net.ResolveUDPAddr("udp", s)
	conn, _ := net.ListenUDP("udp", addr)

	udp := new(UdpTransport)
	udp.conn = conn
	udp.C = make(chan []byte)

	udp.start()
	return udp
}

func (udp *UdpTransport)start(){
	go func(){
		buf := make([]byte, 1024 * 64)
		for{
			n, raddr, _ := udp.conn.ReadFromUDP(buf)
			fmt.Printf("%s < %s", raddr.String(), string(buf[:n]))
			udp.C <- buf[:n]
		}	
	}()
}

func (udp *UdpTransport)Stop(){
	udp.conn.Close()
	close(udp.C)
} 

type Node struct{
	Id string
	Role string

	Term uint32
	Index uint64
	CommitIndex uint64

	VoteFor string

	Members map[string]*Member
}

type Member struct{
	Id string
	Role string
	Addr string

	NextIndex uint64
	MatchIndex uint64
}

func NewMember(id, addr string) *Member{
	ret := new(Member)
	ret.Id = id
	ret.Addr = addr
	return ret
}

const TimerInterval = 20
const ElectionTimeout = 2 * 1000
const RequestVoteTimeout = 200

func main(){
	transport := NewUdpTransport("127.0.0.1", 8001)
	defer transport.Stop()

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	node := new(Node)
	node.Id = "n1"
	node.Role = "follower"
	node.Term = 0
	node.Index = 0
	node.VoteFor = ""
	node.Members = make(map[string]*Member)

	{
		node.Members["n2"] = NewMember("n2", "127.0.0.1:8002")
		node.Members["n3"] = NewMember("n3", "127.0.0.1:8003")
	}

	var election_timeout int;
	election_timeout = ElectionTimeout + rand.Intn(100)

	var votesReceived map[string]string

	for{
		select{
		case <-ticker.C:
			election_timeout -= TimerInterval
			if election_timeout <= 0 {
				election_timeout = RequestVoteTimeout + rand.Intn(100)

				node.Role = "candidate"
				node.Term += 1
				log.Println("begin election for term", node.Term)

				votesReceived = make(map[string]string)

				// vote self
				node.VoteFor = node.Id
				votesReceived[node.Id] = ""

				for _, m := range node.Members {
					msg := new(raft.Message)
					msg.Cmd = "RequestVote"
					msg.Src = node.Id
					msg.Dst = m.Id
					msg.Idx = node.Index
					msg.Term = node.Term;
					msg.Data = "please vote me"
					log.Println("    send to", m.Addr, msg)
				}
			}
		case buf := <-transport.C:
			msg := raft.DecodeMessage(buf);
			if msg == nil {
				continue
			}
			log.Printf("%#v\n", msg)

			if msg.Cmd == "Noop" {
			}
			if msg.Cmd == "RequestVote" {
				// ignore msg.Term == node.Term(a retransimission msg)
				if msg.Term > node.Term && msg.Idx >= node.Index {
					// send ack
					node.VoteFor = msg.Src
				}
			}
			if msg.Cmd == "RequestVoteAck" {
				if node.Role == "candidate" && msg.Idx == node.Index && msg.Term == node.Term {
					votesReceived[msg.Src] = ""
					if len(votesReceived) > len(node.Members)/2 {
						log.Printf("become leader")
					}
				}
			}
			if msg.Cmd == "HeartBeat" {
			}
			if msg.Cmd == "AppendEntry" {
			}

			if msg.Term > node.Term {
				node.Term = msg.Term
				if node.Role != "follower" {
					log.Println("convert to follower")
					node.Role = "follower"
					election_timeout = ElectionTimeout + rand.Intn(100)
				}
			}
			// if msg from leader, reset election_timeout
		}
	}
}
