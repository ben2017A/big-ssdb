package link

import (
	"net"
	"log"
	"fmt"
	"sync"
	"bytes"
	"util"
)

/*
请求响应模式, 一个连接如果有一个请求在处理时, 则不再解析报文, 等响应后再解析下一个报文.
*/

type Server struct {
	C chan *Message
	
	lastClientId int
	conn *net.TCPListener
	clients map[int]net.Conn
	mux sync.Mutex
}

func NewServer(ip string, port int) *Server {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	conn, _ := net.ListenTCP("tcp", addr)

	tcp := new(Server)
	tcp.C = make(chan *Message)
	tcp.lastClientId = 0
	tcp.conn = conn
	tcp.clients = make(map[int]net.Conn)

	tcp.start()
	return tcp
}

func (tcp *Server)Close(){
	tcp.conn.Close()
	close(tcp.C)
} 

func (tcp *Server)start() {
	go func(){
		for {
			conn, err := tcp.conn.Accept()
			if err != nil {
				log.Fatal(err)
			}
			tcp.lastClientId ++
			log.Println("Accept connection", tcp.lastClientId, conn.RemoteAddr().String())
			go tcp.handleClient(tcp.lastClientId, conn)
		}
	}()
}

func (tcp *Server)handleClient(clientId int, conn net.Conn) {
	tcp.mux.Lock()
	tcp.clients[clientId] = conn
	tcp.mux.Unlock()

	defer func() {
		log.Println("Close connection", clientId, conn.RemoteAddr().String())
		tcp.mux.Lock()
		delete(tcp.clients, clientId)
		tcp.mux.Unlock()
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
			tcp.C <- msg
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

func (tcp *Server)Send(msg *Message) {
	tcp.mux.Lock()
	conn := tcp.clients[msg.Src]
	tcp.mux.Unlock()

	if conn == nil {
		log.Println("connection not found:", msg.Src)
		return
	}
	
	s := msg.Encode()
	log.Printf("    send > %d %s\n", msg.Src, util.ReplaceBytes(s, []string{"\r", "\n"}, []string{"\\r", "\\n"}))
	conn.Write([]byte(s))
}
