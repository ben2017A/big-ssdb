package server

import (
	"fmt"
	"net"
	"sync"
	"bytes"
	"math"
	"encoding/binary"

	"glog"
	"raft"
	"util"
)

// TODO: UdpLink, 可靠传输, 流量控制

type client_t struct {
	addr *net.UDPAddr

	send_seq uint32

	recv_seq uint32 // 并不要求连续
	recv_num uint16 // 分段数量
	recv_idx uint16 // 当前分段号(从 1 开始)
}

type UdpTransport struct{
	addr string
	c chan *raft.Message
	conn *net.UDPConn
	id_clients map[string]*client_t
	addr_clients map[string]*client_t
	mux sync.Mutex
}

func (tp *UdpTransport)Listen(addr string) error {
	uaddr, _ := net.ResolveUDPAddr("udp", addr)
	conn, _ := net.ListenUDP("udp", uaddr)
	conn.SetReadBuffer(1 * 1024 * 1024)
	conn.SetWriteBuffer(1 * 1024 * 1024)

	tp := new(UdpTransport)
	tp.addr = uaddr.String()
	tp.conn = conn
	tp.c = make(chan *raft.Message, 8)
	tp.id_clients = make(map[string]*client_t)
	tp.addr_clients = make(map[string]*client_t)

	go tp.Recv()

	return nil
}

func (tp *UdpTransport)C() chan *raft.Message {
	return tp.c
}

func (tp *UdpTransport)Addr() string {
	return tp.addr
}

func (tp *UdpTransport)Close(){
	// TODO: properly stop
	tp.conn.Close()
}

func (tp *UdpTransport)Connect(nodeId, addr string) error {
	tp.mux.Lock()
	defer tp.mux.Unlock()

	uaddr, _ := net.ResolveUDPAddr("udp", addr)
	addr = uaddr.String() // re format addr
	if tp.addr_clients[addr] == nil {
		client := new(client_t)
		client.addr = uaddr
		client.send_seq = 1
		client.recv_num = 0
		tp.addr_clients[addr] = client
	}
	tp.id_clients[nodeId] = tp.addr_clients[addr]

	return nil
}

func (tp *UdpTransport)Disconnect(nodeId string){
	tp.mux.Lock()
	defer tp.mux.Unlock()

	client := tp.id_clients[nodeId]
	if client == nil {
		return
	}
	delete(tp.id_clients, nodeId)

	has_ref := false
	for _, c := range tp.id_clients {
		if c == client {
			has_ref = true
			break
		}
	}
	if !has_ref {
		delete(tp.addr_clients, client.addr.String())
	}
}

// thread safe
func (tp *UdpTransport)Send(msg *raft.Message) bool {
	tp.mux.Lock()
	defer tp.mux.Unlock()

	client := tp.id_clients[msg.Dst]
	if client == nil {
		glog.Debug("dst: %s not connected", msg.Dst)
		return false
	}
	client.send_seq ++

	const PacketSize = 8 * 1024
	send_buf := new(bytes.Buffer)

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

		cnt, err := tp.conn.WriteToUDP(send_buf.Bytes(), client.addr)
		if cnt == 0 { // closed in other thread
			return false
		}
		if err != nil { // socket error
			glog.Errorln(err)
			return false
		}
	}

	// buf := []byte(str)
	// n, _ := tp.conn.WriteToUDP(buf, addr)
	glog.Debug("    send > %s\n", util.StringEscape(str))
	return true
}

func (tp *UdpTransport)Recv() {
	recv_buf := new(bytes.Buffer)
	buf := make([]byte, 64*1024)

	for{
		cnt, uaddr, err := tp.conn.ReadFromUDP(buf)
		if cnt == 0 { // closed in other thread
			break
		}
		if err != nil { // socket error
			glog.Errorln(err)
			break
		}
		if cnt <= 8 {
			glog.Error("bad packet len = %d", cnt)
			continue
		}
		client := tp.addr_clients[uaddr.String()]
		if client == nil {
			glog.Info("receive from unknown addr %s", uaddr.String())
			continue
		}

		s := binary.BigEndian.Uint32(buf[0:4])
		n := binary.BigEndian.Uint16(buf[4:6])
		i := binary.BigEndian.Uint16(buf[6:8])
		if n == 0 || i == 0 {
			glog.Debug("bad packet n=%d, i=%d", n, i)
			continue
		}
		if client.recv_num == 0 || client.recv_seq != s || client.recv_num != n {
			client.recv_seq = s
			client.recv_num = n
			client.recv_idx = 0
			recv_buf.Reset()
		}
		if client.recv_idx != i - 1 { // miss order
			glog.Info("miss ordered packet")
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
			msg := raft.DecodeMessage(str);
			if msg == nil {
				glog.Error("decode error: %s", str)
			} else {
				glog.Debug(" receive < %s\n", util.StringEscape(str))
				tp.c <- msg
			}
		}
	}

	// close received msg queue
	close(tp.c)
}
