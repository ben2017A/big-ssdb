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
	dns map[string]string
}

func NewUdpTransport(ip string, port int) (*UdpTransport){
	s := fmt.Sprintf("%s:%d", ip, port)
	addr, _ := net.ResolveUDPAddr("udp", s)
	conn, _ := net.ListenUDP("udp", addr)

	udp := new(UdpTransport)
	udp.conn = conn
	udp.C = make(chan []byte)
	udp.dns = make(map[string]string)

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

func (udp *UdpTransport)Connect(nodeId, addr string){
	udp.dns[nodeId] = addr
}

func (udp *UdpTransport)Disconnect(nodeId string){
	delete(udp.dns, nodeId)
}

const TimerInterval = 20

func main(){
	transport := NewUdpTransport("127.0.0.1", 8001)
	defer transport.Stop()

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	node := new(raft.Node)
	node.Id = "n1"
	node.Role = "follower"
	node.Term = 0
	node.Index = 0
	node.Members = make(map[string]*raft.Member)

	{
		node.Members["n2"] = raft.NewMember("n2", "127.0.0.1:8002")
		node.Members["n3"] = raft.NewMember("n3", "127.0.0.1:8003")
	}

	node.VoteFor = ""
	node.ElectionTimeout = raft.ElectionTimeout + rand.Intn(100)

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case buf := <-transport.C:
			msg := raft.DecodeMessage(buf);
			if msg == nil {
				continue
			}
			log.Printf("%#v\n", msg)
			node.HandleMessage(msg)
		}
	}
}
