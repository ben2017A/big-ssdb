package redis

import (
	"net"
	"fmt"
	"sync"
	"bytes"
	"glog"
	"util"
)

/*
TODO: 请求响应模式, 一个连接如果有一个请求在处理时, 则不再解析报文, 等响应后再解析下一个报文.
*/

type Transport struct {
	C chan *Request
	
	lastClientId int
	conn *net.TCPListener
	clients map[int]net.Conn
	mux sync.Mutex
}

func NewTransport(ip string, port int) *Transport {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	conn, err := net.ListenTCP("tcp", addr)
	if err != nil {
		glog.Errorln(err)
		return nil
	}

	tp := new(Transport)
	tp.C = make(chan *Request)
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
				glog.Fatalln(err)
			}
			tp.lastClientId ++
			glog.Info("Accept connection %d %s", tp.lastClientId, conn.RemoteAddr().String())
			go tp.handleClient(tp.lastClientId, conn)
		}
	}()
}

func (tp *Transport)handleClient(clientId int, conn net.Conn) {
	tp.mux.Lock()
	tp.clients[clientId] = conn
	tp.mux.Unlock()

	defer func() {
		glog.Info("Close connection %d %s", clientId, conn.RemoteAddr().String())
		tp.mux.Lock()
		delete(tp.clients, clientId)
		tp.mux.Unlock()
		conn.Close()	
	}()

	var buf bytes.Buffer
	var msg *Request
	msg = new(Request)
	tmp := make([]byte, 128*1024)

	for {
		for {
			n := msg.Decode(buf.Bytes())
			if n == -1 {
				glog.Warnln("Parse error")
				return
			} else if (n == 0){
				break
			}
			buf.Next(n)
			msg.Src = clientId
			tp.C <- msg
			msg = new(Request)
		}
		
		n, err := conn.Read(tmp)
		if err != nil {
			break
		}
		buf.Write(tmp[0:n])
		glog.Debug("    receive > %d %s", clientId, util.StringEscape(string(tmp[0:n])))
	}
}

func (tp *Transport)Send(resp *Response) {
	dst := resp.Dst
	data := resp.Encode()

	tp.mux.Lock()
	defer tp.mux.Unlock()

	conn := tp.clients[dst]
	if conn == nil {
		glog.Info("connection not found: %s", dst)
		return
	}
	
	glog.Debug("    send > %d %s\n", dst, util.StringEscape(data))
	conn.Write([]byte(data))
}
