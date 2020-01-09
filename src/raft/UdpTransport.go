package raft

import (
	"fmt"
	"net"
	"log"
	"strings"
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

	tp := new(UdpTransport)
	tp.conn = conn
	tp.C = make(chan []byte)
	tp.dns = make(map[string]string)

	tp.start()
	return tp
}

func (tp *UdpTransport)start(){
	go func(){
		buf := make([]byte, 1024 * 64)
		for{
			n, raddr, _ := tp.conn.ReadFromUDP(buf)
			fmt.Printf("%s < %s", raddr.String(), string(buf[:n]))
			tp.C <- buf[:n]
		}	
	}()
}

func (tp *UdpTransport)Stop(){
	tp.conn.Close()
	close(tp.C)
} 

func (tp *UdpTransport)Connect(nodeId, addr string){
	tp.dns[nodeId] = addr
}

func (tp *UdpTransport)Disconnect(nodeId string){
	delete(tp.dns, nodeId)
}

func (tp *UdpTransport)SendTo(buf []byte, nodeId string) int{
	addr := tp.dns[nodeId]
	if addr == "" {
		return -1
	}

	log.Println("    send to", addr, strings.Trim(string(buf), " \t\n"))

	uaddr, _ := net.ResolveUDPAddr("udp", addr)
	n, _ := tp.conn.WriteToUDP(buf, uaddr)
	return n
}
