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
	sync.Mutex
	C chan *Message
	
	lastClientId int
	conn *net.TCPListener
	clients map[int]*client_t
}

type client_t struct {
	id int
	conn net.Conn
	isRedis bool
}

func NewTransport(ip string, port int) *Transport {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	conn, err := net.ListenTCP("tcp", addr)
	if err != nil {
		glog.Errorln(err)
		return nil
	}

	tp := new(Transport)
	tp.C = make(chan *Message, 100)
	tp.lastClientId = 0
	tp.conn = conn
	tp.clients = make(map[int]*client_t)

	tp.start()
	return tp
}

func (tp *Transport)Close(){
	// TODO: properly stop
	tp.conn.Close()
	close(tp.C)
} 

func (tp *Transport)start() {
	go func(){
		for {
			conn, err := tp.conn.Accept()
			if err != nil {
				glog.Debugln(err)
				return
			}
			tp.lastClientId ++

			client := new(client_t)
			client.id = tp.lastClientId
			client.conn = conn
			tp.Lock()
			tp.clients[client.id] = client
			tp.Unlock()

			glog.Debug("Accept connection %d %s", client.id, conn.RemoteAddr().String())
			go tp.receiveClient(client)
		}
	}()
}

func (tp *Transport)receiveClient(client *client_t) {
	defer func() {
		glog.Debug("Close connection %d %s", client.id, client.conn.RemoteAddr().String())
		tp.Lock()
		delete(tp.clients, client.id)
		tp.Unlock()
		client.conn.Close()
	}()

	var buf bytes.Buffer
	var msg *Message
	msg = new(Message)
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
			
			msg.Src = client.id
			client.isRedis = msg.IsRedis

			tp.C <- msg
			msg = new(Message)
		}
		
		n, err := client.conn.Read(tmp)
		if err != nil {
			break
		}
		buf.Write(tmp[0:n])
		glog.Trace("    receive > %d %s", client.id, util.StringEscape(string(tmp[0:n])))
	}
}

func (tp *Transport)Send(resp *Response) {
	tp.Lock()
	defer tp.Unlock()

	dst := resp.Dst
	client := tp.clients[dst]
	if client == nil {
		glog.Info("connection not found: %s", dst)
		return
	}

	var data string
	if client.isRedis {
		data = resp.Encode()
	} else {
		data = resp.EncodeSSDB()
	}

	glog.Trace("    send > %d %s\n", dst, util.StringEscape(data))
	// TODO: may block
	client.conn.Write([]byte(data))
}
