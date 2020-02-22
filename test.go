package main

import (
	"fmt"
	"time"
	"log"
	// "math/rand"
	"os"
	"strconv"
	"strings"

	"myutil"
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
	
	jobs map[int64]*link.Message
}

func NewService(dir string, node *raft.Node) *Service {
	svc := new(Service)

	svc.db = store.OpenKVStore(dir + "/data")
	svc.redo = xna.OpenRedolog(dir + "/data/redo.log")

	svc.lastApplied = svc.redo.CommitIndex()

	svc.redo.SeekToLastCheckpoint()
	for {
		tx := svc.redo.NextTransaction()
		if tx == nil {
			break
		}
		for _, ent := range tx.Entries() {
			log.Println("Redo", ent)
			switch ent.Type {
			case "set":
				svc.db.Set(ent.Key, ent.Val)
			case "del":
				svc.db.Del(ent.Key)
			}
		}
	}
	
	svc.node = node
	svc.node.AddService(svc)

	return svc
}

func (svc *Service)HandleClientMessage(msg *link.Message){
	ps := strings.Split(msg.Data, " ")

	if ps[0] == "JoinGroup" {
		svc.node.JoinGroup(ps[1], ps[2])
		return
	}

	if svc.node.Role == "leader" {
		if ps[0] == "AddMember" {
			svc.node.AddMember(ps[1], ps[2])
			return;
		}

		cmd := ps[0]
		key := ps[1]
		
		if cmd == "get" {
			s := svc.db.Get(key)
			log.Println(s)
			// resp := &link.Message{s, msg.Addr}
			// serv_link.Send(resp)
			return
		}

		var s string
		if cmd == "set" || cmd == "incr" {
			val := ps[2]
			s = fmt.Sprintf("%s %s %s", cmd, key, val)
		}
		if cmd == "del" {
			// svc.Del(ps[1])
		}
		
		if s == "" {
			log.Println("unknown cmd:", cmd)
			return
		}
		
		idx := svc.node.Write(s)
		log.Println(idx)
	}
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
		}
		if cmd == "incr" {
			delta := myutil.Atoi64(ps[2])
			old := svc.db.Get(key)
			num := myutil.Atoi64(old) + delta
			val := fmt.Sprintf("%d", num)
			
			idx := ent.Index
			tx := xna.NewTransaction()
			tx.Set(idx, key, val)
			svc.redo.WriteTransaction(tx)
			
			svc.db.Set(key, val)
		}
		if cmd == "del" {
			idx := ent.Index
			tx := xna.NewTransaction()
			tx.Del(idx, key)
			svc.redo.WriteTransaction(tx)

			svc.db.Del(key)
		}
	}
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

	log.Println("raft server started at", port)
	xport := raft.NewUdpTransport("127.0.0.1", port)
	store := raft.OpenStorage(base_dir + "/raft")

	node := raft.NewNode(nodeId, store, xport)
	node.Start()

	/////////////////////////////////////

	log.Println("api server started at", port+1000)
	serv_link := link.NewTcpServer("127.0.0.1", port+1000)
	
	svc := NewService(base_dir, node)

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case msg := <-xport.C:
			node.HandleRaftMessage(msg)
		case msg := <-serv_link.C:
			svc.HandleClientMessage(msg)
		}
	}
}
