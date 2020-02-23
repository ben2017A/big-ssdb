package link

import (
	"net"
	"log"
	"fmt"
	"strings"
	"bytes"
)

type TcpServer struct {
	C chan *Message
	
	lastClientId int
	conn *net.TCPListener
	clients map[int]net.Conn
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
	// TODO: 加锁
	tcp.clients[clientId] = conn
	
	buf := new(bytes.Buffer)
	tmp := make([]byte, 64*1024)
	for {
		for {
			line, err := buf.ReadString('\n')
			if err != nil {
				break;
			}
			line = strings.Trim(line, "\r\n")
			log.Printf("    receive < %s\n", line)
			tcp.C <- &Message{clientId, line}
		}
		
		n, err := conn.Read(tmp)
		if err != nil {
			break
		}
		buf.Write(tmp[0:n])
	}

	log.Println("Close connection", clientId, conn.RemoteAddr().String())
	delete(tcp.clients, clientId)
	conn.Close()
}

func (tcp *TcpServer)Send(msg *Message) {
	// TODO: lock
	conn := tcp.clients[msg.Src]
	if conn == nil {
		log.Println("connection not found:", msg.Src)
		return
	}
	
	conn.Write([]byte(msg.Data + "\n"))
	log.Printf("    send > %s\n", msg.Data)
}
