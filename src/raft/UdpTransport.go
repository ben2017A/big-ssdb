package raft

import (
	"fmt"
	"net"
	"log"
	"time"
	"strings"
	"math/rand"
	"util"
)

type UdpTransport struct{
	addr string
	c chan *Message
	conn *net.UDPConn
	dns map[string]string
}

func NewUdpTransport(ip string, port int) (*UdpTransport){
	s := fmt.Sprintf("%s:%d", ip, port)
	addr, _ := net.ResolveUDPAddr("udp", s)
	conn, _ := net.ListenUDP("udp", addr)

	tp := new(UdpTransport)
	tp.addr = fmt.Sprintf("%s:%d", ip, port)
	tp.conn = conn
	tp.c = make(chan *Message)
	tp.dns = make(map[string]string)

	tp.start()
	return tp
}

func (tp *UdpTransport)C() chan *Message {
	return tp.c
}

func (tp *UdpTransport)Addr() string {
	return tp.addr
}

func (tp *UdpTransport)simulate_bad_network(delayC chan interface{}){
	go func(){
		const MaxDelay int = 200
		const Interval int = 5
		timer := time.NewTicker(time.Duration(Interval) * time.Millisecond)
		heap := util.NewIntPriorityQueue()
		
		g_time := 0
		for {
			select {
			case <- timer.C:
				g_time += Interval
				for heap.Size() > 0 {
					w, msg := heap.Top()
					if w > g_time {
						break;
					}
					heap.Pop()
					
					log.Printf("    receive < %s\n", msg.(*Message).Encode())
					tp.c <- msg.(*Message)
				}
			case msg := <- delayC:
				delay := rand.Intn(MaxDelay) // 模拟延迟和乱序
				heap.Push(g_time + delay, msg)
				log.Println("delay", delay, "ms")
			}
		}
	}()
}

func (tp *UdpTransport)start(){
	// TODO: for testing
	const SIMULATE_BAD_NETWORK bool = false
	var delayC chan interface{}

	if SIMULATE_BAD_NETWORK {
		delayC = make(chan interface{})
		tp.simulate_bad_network(delayC)
	}
	
	go func(){
		buf := make([]byte, 64*1024)
		for{
			n, _, _ := tp.conn.ReadFromUDP(buf)
			data := string(buf[:n])
			// log.Printf("    receive < %s\n", strings.Trim(data, "\r\n"))
			msg := DecodeMessage(data);
			if msg == nil {
				log.Println("decode error:", data)
			} else {
				if SIMULATE_BAD_NETWORK {
					delayC <- msg
				}else{
					log.Printf("  receive < %s\n", msg.Encode())
					tp.c <- msg
				}
			}
		}
	}()
}

func (tp *UdpTransport)Close(){
	tp.conn.Close()
	close(tp.c)
}

func (tp *UdpTransport)Connect(nodeId, addr string){
	tp.dns[nodeId] = addr
}

func (tp *UdpTransport)Disconnect(nodeId string){
	delete(tp.dns, nodeId)
}

// TODO: thread safe
func (tp *UdpTransport)Send(msg *Message) bool{
	addr := tp.dns[msg.Dst]
	if addr == "" {
		log.Printf("dst: %s not connected", msg.Dst)
		return false
	}

	buf := []byte(msg.Encode())
	uaddr, _ := net.ResolveUDPAddr("udp", addr)
	n, _ := tp.conn.WriteToUDP(buf, uaddr)
	log.Printf("    send > %s\n", strings.Trim(string(buf), "\r\n"))
	return n > 0
}
