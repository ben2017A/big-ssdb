package raft

import (
	"fmt"
	"net"
	"log"
	"strings"
	"sync"
	"bytes"
	"math"
	"encoding/binary"
	"util"
)

type client_t struct {
	addr *net.UDPAddr

	send_seq uint32

	recv_seq uint32 // 并不要求连续
	recv_num uint16 // 分段数量
	recv_idx uint16 // 当前分段号(从 1 开始)
}

type UdpTransport struct{
	addr string
	c chan *Message
	conn *net.UDPConn
	id_clients map[string]*client_t
	addr_clients map[string]*client_t
	mux sync.Mutex
}

func NewUdpTransport(ip string, port int) (*UdpTransport){
	s := fmt.Sprintf("%s:%d", ip, port)
	addr, _ := net.ResolveUDPAddr("udp", s)
	conn, _ := net.ListenUDP("udp", addr)
	conn.SetReadBuffer(1 * 1024 * 1024)
	conn.SetWriteBuffer(1 * 1024 * 1024)

	tp := new(UdpTransport)
	tp.addr = fmt.Sprintf("%s:%d", ip, port)
	tp.conn = conn
	tp.c = make(chan *Message, 8)
	tp.id_clients = make(map[string]*client_t)
	tp.addr_clients = make(map[string]*client_t)

	tp.start()
	return tp
}

func (tp *UdpTransport)C() chan *Message {
	return tp.c
}

func (tp *UdpTransport)Addr() string {
	return tp.addr
}

func (tp *UdpTransport)start(){
	go func(){
		recv_buf := new(bytes.Buffer)
		buf := make([]byte, 64*1024)

		for{
			cnt, uaddr, _ := tp.conn.ReadFromUDP(buf)
			client := tp.addr_clients[uaddr.String()]
			if client == nil {
				log.Println("receive from unknown addr", uaddr.String())
				continue
			}
			if cnt <= 8 {
				log.Println("bad packet len =", cnt)
				continue
			}

			s := binary.BigEndian.Uint32(buf[0:4])
			n := binary.BigEndian.Uint16(buf[4:6])
			i := binary.BigEndian.Uint16(buf[6:8])
			// log.Println(s, n, i)
			if n == 0 || i == 0 {
				log.Printf("bad packet n=%d, i=%d", n, i)
				continue
			}
			if client.recv_num == 0 || client.recv_seq != s || client.recv_num != n {
				client.recv_seq = s
				client.recv_num = n
				client.recv_idx = 0
				recv_buf.Reset()
			}
			if client.recv_idx != i - 1 { // miss order
				log.Println("miss order packet")
				client.recv_num = 0
				continue
			}

			var str string

			if n == 1 && i == 1 {
				client.recv_num = 0 // finish
				str = string(buf[8:cnt])
			} else {
				client.recv_idx = i
				recv_buf.Write(buf[8:cnt])
				if client.recv_idx == client.recv_num {
					client.recv_num = 0 // finish
					str = string(recv_buf.Bytes())
				}
			}

			if len(str) > 0 {
				msg := DecodeMessage(str);
				if msg == nil {
					log.Println("decode error:", str)
				} else {
					log.Printf(" receive < %s\n", str)
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
	tp.mux.Lock()
	defer tp.mux.Unlock()

	uaddr, _ := net.ResolveUDPAddr("udp", addr)
	client := new(client_t)
	client.addr = uaddr
	client.send_seq = 1
	client.recv_num = 0

	tp.id_clients[nodeId] = client
	tp.addr_clients[addr] = client
}

func (tp *UdpTransport)Disconnect(nodeId string){
	tp.mux.Lock()
	defer tp.mux.Unlock()

	client := tp.id_clients[nodeId]
	if client == nil {
		return
	}

	delete(tp.addr_clients, client.addr.String())
	delete(tp.id_clients, nodeId)
}

// thread safe
func (tp *UdpTransport)Send(msg *Message) bool{
	tp.mux.Lock()
	client := tp.id_clients[msg.Dst]
	tp.mux.Unlock()

	if client == nil {
		log.Printf("dst: %s not connected", msg.Dst)
		return false
	}

	const PacketSize = 8 * 1024
	send_buf := new(bytes.Buffer)
	client.send_seq ++

	str := msg.Encode()
	num := uint16(math.Ceil(float64(len(str)) / PacketSize))
	for idx := uint16(0); idx < num; idx ++ {
		s := int(idx) * PacketSize
		e := util.MinInt(s + PacketSize, len(str))

		send_buf.Reset()
		binary.Write(send_buf, binary.BigEndian, client.send_seq)
		binary.Write(send_buf, binary.BigEndian, num)
		binary.Write(send_buf, binary.BigEndian, idx + 1)
		send_buf.WriteString(str[s : e])

		_, err := tp.conn.WriteToUDP(send_buf.Bytes(), client.addr)
		if err != nil {
			return false
		}
	}

	// buf := []byte(str)
	// n, _ := tp.conn.WriteToUDP(buf, addr)
	log.Printf("    send > %s\n", strings.Trim(str, "\r\n"))
	return true
}
