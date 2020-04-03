package link

import (
	"net"
	"log"
	"fmt"
	"sync"
	"strings"
	"bytes"
	"strconv"
	"util"
)

/*
请求响应模式, 一个连接如果有一个请求在处理时, 则不再解析报文, 等响应后再解析下一个报文.
*/

type TcpServer struct {
	C chan *Message
	
	lastClientId int
	conn *net.TCPListener
	clients map[int]net.Conn
	mux sync.Mutex
}

func NewTcpServer(ip string, port int) *TcpServer {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	conn, _ := net.ListenTCP("tcp", addr)

	tcp := new(TcpServer)
	tcp.C = make(chan *Message)
	tcp.lastClientId = 0
	tcp.conn = conn
	tcp.clients = make(map[int]net.Conn)

	tcp.start()
	return tcp
}

func (tcp *TcpServer)Close(){
	tcp.conn.Close()
	close(tcp.C)
} 

func (tcp *TcpServer)start() {
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

func (tcp *TcpServer)handleClient(clientId int, conn net.Conn) {
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

func (tcp *TcpServer)Send(msg *Message) {
	tcp.mux.Lock()
	conn := tcp.clients[msg.Src]
	tcp.mux.Unlock()

	if conn == nil {
		log.Println("connection not found:", msg.Src)
		return
	}
	
	s := tcp.encodeResponse(msg)
	log.Printf("    send > %d %s\n", msg.Src, util.ReplaceBytes(s, []string{"\r", "\n"}, []string{"\\r", "\\n"}))
	conn.Write([]byte(s))
}

func (tcp *TcpServer)encodeResponse(m *Message) string {
	code := strings.ToLower(m.Code())
	if code == "ok" {
		if len(m.Args()) == 0 || m.Args()[0] == "" {
			return "+OK\r\n"
		}
	} else if code == "error" {
		return fmt.Sprintf("-ERR %s\r\n", m.Args()[0])
	}

	var buf bytes.Buffer
	count := len(m.Args())
	if count > 1 {
		buf.WriteString("*")
		buf.WriteString(strconv.Itoa(count))
		buf.WriteString("\r\n")
	}
	for _, p := range m.Args() {
		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(len(p)))
		buf.WriteString("\r\n")
		buf.WriteString(p)
		buf.WriteString("\r\n")
	}
	return buf.String()
}
