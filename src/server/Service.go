package server

import (
	"log"
	"sync"
	"io/ioutil"

	"raft"
	"ssdb"
	"link"
)

type ServiceStatus int

const(
	ServiceStatusLogger = 0
	ServiceStatusActive = 1
)

type Service struct{
	status ServiceStatus
	
	lastApplied int64
	dir string
	
	db *ssdb.Db

	node *raft.Node
	xport *link.TcpServer
	
	jobs map[int64]*Request // raft.Index => Request
	mux sync.Mutex
}

func NewService(dir string, node *raft.Node, xport *link.TcpServer) *Service {
	svc := new(Service)
	svc.db = ssdb.OpenDb(dir + "/data")
	
	svc.dir = dir
	svc.status = ServiceStatusActive
	svc.lastApplied = svc.db.CommitIndex()

	svc.node = node	
	svc.xport = xport
	svc.jobs = make(map[int64]*Request)

	node.SetService(svc)
	node.Start()

	return svc
}

func (svc *Service)Close() {
	svc.xport.Close()
	svc.node.Close()
	svc.db.Close()
}

func (svc *Service)HandleClientMessage(msg *link.Message) {
	svc.mux.Lock()
	defer svc.mux.Unlock()

	req := new(Request)
	if !req.Decode(msg.Data) {
		log.Println("bad request:", msg.Data)
		return
	}
	req.Src = msg.Src
	
	cmd := req.Cmd()

	// TODO: config join_group
	if cmd == "JoinGroup" {
		svc.node.JoinGroup(req.Arg(0), req.Arg(1))
		return
	}
	// TODO: config add_member
	if cmd == "AddMember" {
		svc.node.AddMember(req.Arg(0), req.Arg(1))
		return
	}
	// TODO: config del_member
	if cmd == "DelMember" {
		svc.node.DelMember(req.Arg(0))
		return
	}
	
	if cmd == "info" {
		s := svc.node.Info()
		resp := &link.Message{req.Src, s}
		svc.xport.Send(resp)
		return
	}
	
	if svc.status != ServiceStatusActive {
		log.Println("Service unavailable")
		resp := &link.Message{req.Src, "error: service unavailable"}
		svc.xport.Send(resp)
		return
	}

	if cmd == "get" {
		s := svc.db.Get(req.Key())
		log.Println(req.Key(), "=", s)
		resp := &link.Message{req.Src, s}
		svc.xport.Send(resp)
		return
	}
	
	if cmd == "dump" {
		fn := svc.dir + "/snapshot.db"
		svc.db.MakeFileSnapshot(fn)
		data, _ := ioutil.ReadFile(fn)
		s := string(data)

		log.Println(req.Key(), "=", s)
		resp := &link.Message{req.Src, s}
		svc.xport.Send(resp)
		return
	}

	if svc.node.Role != raft.RoleLeader {
		log.Println("error: not leader")
		resp := &link.Message{req.Src, "error: not leader"}
		svc.xport.Send(resp)
		return
	}
	
	s := req.Encode()
	if s == "" {
		log.Println("error: unknown cmd: " + cmd)
		resp := &link.Message{req.Src, "error: unkown cmd " + cmd}
		svc.xport.Send(resp)
		return
	}
	
	term, idx := svc.node.Write(s)
	req.Term = term
	svc.jobs[idx] = req
}

func (svc *Service)handleRaftEntry(ent *raft.Entry) {
	svc.mux.Lock()
	defer svc.mux.Unlock()

	var ret string

	if ent.Type == raft.EntryTypeData{
		log.Println("[Apply]", ent.Index, ent.Data)

		req := new(Request)
		if !req.Decode(ent.Data) {
			log.Println("unknow entry:", ent.Data)
			return
		}

		key := req.Key()
		val := req.Val()
		
		switch req.Cmd() {
		case "set":
			svc.db.Set(ent.Index, key, val)
		case "del":
			svc.db.Del(ent.Index, key)
		case "incr":
			ret = svc.db.Incr(ent.Index, key, val)
		}
	}

	req := svc.jobs[ent.Index]
	if req == nil {
		return
	}
	delete(svc.jobs, ent.Index)
	if req.Term != ent.Term {
		log.Println("entry was overwritten by new leader")
		ret = "error"
	}
	
	if ret == "" {
		ret = "ok"
	}
	resp := &link.Message{req.Src, ret}
	svc.xport.Send(resp)
}

/* #################### raft.Service interface ######################### */

func (svc *Service)LastApplied() int64{
	return svc.lastApplied
}

func (svc *Service)ApplyEntry(ent *raft.Entry){
	// 不需要持久化, 从 Redolog 中获取
	svc.lastApplied = ent.Index
	svc.handleRaftEntry(ent)
}

func (svc *Service)InstallSnapshot() {
	svc.status = ServiceStatusLogger
	log.Println("Service become unavailable")
}
