package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"strings"
	"sync"

	"glog"
	"raft"
	"raft/server"
	"redis"
)

var node *raft.Node
var xport *redis.Transport
var service *TestService

type ServiceTask struct {
	Term int32
	Index int64
	Req *redis.Message
}

type TestService struct {
	sync.Mutex

	applied int64

	kvs map[string]string
	tasks map[int64]*ServiceTask
}

func NewTestService() *TestService {
	ret := new(TestService)
	ret.applied = 0
	ret.kvs = make(map[string]string)
	ret.tasks = make(map[int64]*ServiceTask)
	return ret
}

func (s *TestService)LastIndex() int64 {
	return s.applied
}

func (s *TestService)InstallSnapshot() {
	glog.Warn("not implemented")
}

func (s *TestService)ApplyEntry(ent *raft.Entry) {
	if ent.Index == 1 || ent.Index == node.CommitIndex() {
		glog.Trace("apply %d", ent.Index)
	}

	s.applied = ent.Index
	
	if ent.Type != raft.EntryTypeData {
		return
	}

	s.Lock()
	defer s.Unlock()

	task := s.tasks[ent.Index]

	var resp *redis.Response = nil
	if task != nil {
		resp = new(redis.Response)
		resp.Dst = task.Req.Src
	}

	msg := new(redis.Message)
	msg.Decode([]byte(ent.Data))

	cmd := strings.ToLower(msg.Cmd())
	key := msg.Key()

	switch cmd {
	case "set":
		if key != "" {
			s.kvs[key] = msg.Val()
		}
	case "del":
		if resp != nil {
			_, rc := s.kvs[key]
			if rc == false {
				resp.SetInt(0)
			} else {
				resp.SetInt(1)
			}
		}
		delete(s.kvs, key)
	case "incr":
		var num int64 = 0
		val, rc := s.kvs[key]
		if rc == true {
			num, _ = strconv.ParseInt(val, 10, 64)
		}
		num += 1
		s.kvs[key] = fmt.Sprintf("%d", num)
		
		if resp != nil {
			resp.SetInt(num)
		}
	}

	if task != nil {
		if task.Term != ent.Term {
			resp.SetError("Propose failed, different term")
		} else {
			//
		}

		xport.Send(resp)
	}
}

func (s *TestService)Process(req *redis.Message) {
	resp := new(redis.Response)
	resp.Dst = req.Src

	cmd := strings.ToLower(req.Cmd())

	if cmd == "command" {
		resp.SetError("not implemented")
	} else if cmd == "info" {
		resp.SetBulk(node.Info())
	} else if cmd == "get" {
		key := req.Key()

		s.Lock()
		val, rc := s.kvs[key]
		s.Unlock()

		if rc == false {
			resp.SetNull()
		} else {
			resp.SetBulk(val)
		}
	} else {
		t, i := node.Propose(req.EncodeSSDB())
		glog.Debugln("Propose", t, i, cmd)
		if t == -1 {
			resp.SetError("Propose failed")
		} else {
			task := &ServiceTask{
				Term: t,
				Index: i,
				Req: req,
			}

			s.Lock()
			s.tasks[i] = task
			s.Unlock()

			// send reply after applied, not now
			return
		}
	}
	xport.Send(resp)
}


func main(){
	// glog.SetLevel("info")

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}
	nodeId := fmt.Sprintf("%d", port)

	// /////////////////////////////////////
	base_dir := "./tmp/" + nodeId

	if err := os.MkdirAll(base_dir, 0755); err != nil {
		glog.Fatal("Failed to make dir %s, %s", base_dir, err)
	}

	xport = redis.NewTransport("127.0.0.1", port+1000)
	if xport == nil {
		glog.Fatal("Failed to start redis port")
		return
	}
	defer xport.Close()
	glog.Infoln("Redis server started at", port+1000)

	id_addr := make(map[string]string)
	id_addr["8001"] = "127.0.0.1:8001"
	id_addr["8002"] = "127.0.0.1:8002"

	members := make([]string, 0)
	for k, _ := range id_addr {
		members = append(members, k)
	}

	conf := raft.OpenConfig(base_dir)
	if conf.IsNew() {
		conf.Init(nodeId, members)
	}
	logs := raft.OpenBinlog(base_dir)

	raft_xport := server.NewUdpTransport("127.0.0.1", port)
	for k, v := range id_addr {
		raft_xport.Connect(k, v)
	}
	defer raft_xport.Close()
	glog.Infoln("Raft server started at", port)

	service = NewTestService()

	node = raft.NewNode(raft_xport, conf, logs)
	node.SetService(service)
	node.Start()
	defer node.Close()

	go func() {
		for {
			req := <- xport.C
			if req == nil {
				break
			}
			service.Process(req)
		}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	quit := false
	for !quit {
		select{
		case <- c:
			quit = true
		case msg := <- raft_xport.C():
			node.RecvC() <- msg
		}
	}
}
