package redis

import (
	"net"
	"log"
	"fmt"
	"sync"
	"bytes"
	"util"
)

/*
TODO: 请求响应模式, 一个连接如果有一个请求在处理时, 则不再解析报文, 等响应后再解析下一个报文.
*/

type Transport struct {
	C chan *Message
	
	lastClientId int
	conn *net.TCPListener
	clients map[int]net.Conn
	mux sync.Mutex
}

func NewTransport(ip string, port int) *Transport {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	conn, _ := net.ListenTCP("tcp", addr)

	tp := new(Transport)
	tp.C = make(chan *Message)
	tp.lastClientId = 0
	tp.conn = conn
	tp.clients = make(map[int]net.Conn)

	tp.start()
	return tp
}

func (tp *Transport)Close(){
	tp.conn.Close()
	close(tp.C)
} 

func (tp *Transport)start() {
	go func(){
		for {
			conn, err := tp.conn.Accept()
			if err != nil {
				log.Fatal(err)
			}
			tp.lastClientId ++
			log.Println("Accept connection", tp.lastClientId, conn.RemoteAddr().String())
			go tp.handleClient(tp.lastClientId, conn)
		}
	}()
}

func (tp *Transport)handleClient(clientId int, conn net.Conn) {
	tp.mux.Lock()
	tp.clients[clientId] = conn
	tp.mux.Unlock()

	defer func() {
		log.Println("Close connection", clientId, conn.RemoteAddr().String())
		tp.mux.Lock()
		delete(tp.clients, clientId)
		tp.mux.Unlock()
		conn.Close()	
	}()

	var buf bytes.Buffer
	var msg *Message
	msg = new(Message)
	tmp := make([]byte, 128*1024)

	for {
		for {
			n := msg.Decode(buf.Bytes())
			if n == -1 {
				log.Println("Parse error")
				return
			} else if (n == 0){
				// log.Println("not ready")
				break
			}
			buf.Next(n)
			msg.Src = clientId
			tp.C <- msg
			msg = new(Message)
		}
		
		n, err := conn.Read(tmp)
		if err != nil {
			break
		}
		buf.Write(tmp[0:n])
		log.Printf("    receive > %d %s\n", clientId, util.ReplaceBytes(string(tmp[0:n]), []string{"\r", "\n"}, []string{"\\r", "\\n"}))
	}
}

func (tp *Transport)Send(msg *Message) {
	tp.mux.Lock()
	conn := tp.clients[msg.Src]
	tp.mux.Unlock()

	if conn == nil {
		log.Println("connection not found:", msg.Src)
		return
	}
	
	s := msg.Encode()
	log.Printf("    send > %d %s\n", msg.Src, util.ReplaceBytes(s, []string{"\r", "\n"}, []string{"\\r", "\\n"}))
	conn.Write([]byte(s))
}
