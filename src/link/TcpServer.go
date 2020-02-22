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
	conn *net.TCPListener
}

func NewTcpServer(ip string, port int) *TcpServer {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	conn, _ := net.ListenTCP("tcp", addr)

	tcp := new(TcpServer)
	tcp.conn = conn
	tcp.C = make(chan *Message)

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
			log.Println("Accept connection", conn.RemoteAddr().String())
			go tcp.handleClient(conn)
		}
	}()
}

func (tcp *TcpServer)handleClient(conn net.Conn) {
	defer conn.Close()
	
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
			tcp.C <- &Message{line}
		}
		
		n, err := conn.Read(tmp)
		if err != nil {
			break
		}
		buf.Write(tmp[0:n])
	}

	log.Println("Close connection", conn.RemoteAddr().String())
}

func (tcp *TcpServer)Send() {
}
