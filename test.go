package main

import (
	"fmt"
	"time"
	"log"
	// "math/rand"
	"os"
	"strconv"
	"strings"

	"util"
	"raft"
	"store"
	"xna"
	"link"
)

type Service struct{
	lastApplied int64
	
	db *store.KVStore
	redo *xna.Redolog

	node *raft.Node
	xport *link.TcpServer
	
	jobs map[int64]*link.Message // raft.Index => msg
}

func NewService(dir string, node *raft.Node, xport *link.TcpServer) *Service {
	svc := new(Service)
	
	svc.db = store.OpenKVStore(dir + "/data")
	svc.redo = xna.OpenRedolog(dir + "/data/redo.log")
	svc.lastApplied = svc.redo.CommitIndex()

	svc.replayRedolog()

	svc.node = node
	svc.node.AddService(svc)
	
	svc.xport = xport

	svc.jobs = make(map[int64]*link.Message)

	return svc
}

func (svc *Service)replayRedolog() {
	svc.redo.SeekToLastCheckpoint()
	count := 0
	for {
		tx := svc.redo.NextTransaction()
		if tx == nil {
			break
		}
		count ++
		for _, ent := range tx.Entries() {
			log.Println("    Redo", ent)
			switch ent.Type {
			case "set":
				svc.db.Set(ent.Key, ent.Val)
			case "del":
				svc.db.Del(ent.Key)
			}
		}
	}
	if count > 0 {
		svc.redo.WriteCheckpoint()
	} else {
		log.Println("nothing to redo")
	}
}

func (svc *Service)HandleClientMessage(req *link.Message) {
	ps := strings.Split(req.Data, " ")

	cmd := ps[0]
	var key string
	var val string
	if len(ps) > 1 {
		key = ps[1]
	}
	if len(ps) > 2 {
		val = ps[2]
	}

	if cmd == "JoinGroup" {
		svc.node.JoinGroup(ps[1], ps[2])
		return
	}
	if svc.node.Role != "leader" {
		log.Println("error: not leader")
		resp := &link.Message{req.Src, "error: not leader"}
		svc.xport.Send(resp)
		return
	}
	
	
	if cmd == "AddMember" {
		svc.node.AddMember(ps[1], ps[2])
		return;
	}
	if cmd == "get" {
		s := svc.db.Get(key)
		log.Println(key, "=", s)
		resp := &link.Message{req.Src, s}
		svc.xport.Send(resp)
		return
	}

	var s string
	if cmd == "set" || cmd == "incr" {
		s = fmt.Sprintf("%s %s %s", cmd, key, val)
	}
	if cmd == "del" {
		s = fmt.Sprintf("%s %s", cmd, key)
	}
	
	if s == "" {
		log.Println("error: unknown cmd: " + cmd)
		resp := &link.Message{req.Src, "error: unkown cmd " + cmd}
		svc.xport.Send(resp)
		return
	}
	
	idx := svc.node.Write(s)
	svc.jobs[idx] = req
}

func (svc *Service)raftApplyCallback(ent *raft.Entry) {
	req := svc.jobs[ent.Index]
	if req == nil {
		return
	}
	delete(svc.jobs, ent.Index)
	
	resp := &link.Message{req.Src, "ok"}
	svc.xport.Send(resp)
}

/* #################### raft.Service interface ######################### */

func (svc *Service)LastApplied() int64{
	return svc.lastApplied
}

func (svc *Service)ApplyEntry(ent *raft.Entry){
	// 不需要持久化, 从 Redolog 中获取
	svc.lastApplied = ent.Index

	if ent.Type == "Write"{
		log.Println("[Apply]", ent.Data)
		ps := strings.SplitN(ent.Data, " ", 3)
		cmd := ps[0]
		key := ps[1]
		if cmd == "set" {
			val := ps[2]

			idx := ent.Index
			tx := xna.NewTransaction()
			tx.Set(idx, key, val)
			svc.redo.WriteTransaction(tx)

			svc.db.Set(key, val)
		} else if cmd == "incr" {
			delta := util.Atoi64(ps[2])
			old := svc.db.Get(key)
			num := util.Atoi64(old) + delta
			val := fmt.Sprintf("%d", num)
			
			idx := ent.Index
			tx := xna.NewTransaction()
			tx.Set(idx, key, val)
			svc.redo.WriteTransaction(tx)
			
			svc.db.Set(key, val)
		} else if cmd == "del" {
			idx := ent.Index
			tx := xna.NewTransaction()
			tx.Del(idx, key)
			svc.redo.WriteTransaction(tx)

			svc.db.Del(key)
		}
	}
	
	svc.raftApplyCallback(ent)
}

/* ############################################# */

///////////////////////////////////////////

const TimerInterval = 100

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	port := 8001
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}
	nodeId := fmt.Sprintf("%d", port)

	base_dir := fmt.Sprintf("./tmp/%s", nodeId);

	/////////////////////////////////////

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	/////////////////////////////////////

	log.Println("Raft server started at", port)
	store := raft.OpenStorage(base_dir + "/raft")
	raft_xport := raft.NewUdpTransport("127.0.0.1", port)

	node := raft.NewNode(nodeId, store, raft_xport)
	node.Start()

	log.Println("Service server started at", port+1000)
	svc_xport := link.NewTcpServer("127.0.0.1", port+1000)
	
	svc := NewService(base_dir, node, svc_xport)

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case msg := <-raft_xport.C:
			node.HandleRaftMessage(msg)
		case msg := <-svc_xport.C:
			svc.HandleClientMessage(msg)
		}
	}
}
