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
)

type Service struct{
	lastApplied uint64
	
	meta_db *store.KVStore
	data_db *store.KVStore

	node *raft.Node
}

func NewService(dir string, node *raft.Node) *Service {
	svc := new(Service)

	svc.meta_db = store.OpenKVStore(dir + "/meta")
	svc.data_db = store.OpenKVStore(dir + "/data")

	s, _ := svc.meta_db.Get("LastApplied")
	svc.lastApplied = myutil.Atou64(s)

	svc.node = node
	svc.node.AddSubscriber(svc)

	return svc
}

func (svc *Service)Set(key string, val string){
	s := fmt.Sprintf("set %s %s", key, val)
	svc.node.Replicate(s)
}

func (svc *Service)Get(key string){
	log.Println(svc.data_db.Get(key))
}

func (svc *Service)Del(key string){
	s := fmt.Sprintf("del %s", key)
	svc.node.Replicate(s)
}

/* #################### raft.Subscriber interface ######################### */

func (svc *Service)LastApplied() uint64{
	return svc.lastApplied
}

func (svc *Service)ApplyEntry(ent *raft.Entry){
	svc.lastApplied = ent.Index

	if ent.Type == "Replicate"{
		log.Println("[Apply]", ent.Data)
		ps := strings.Split(ent.Data, " ")
		if ps[0] == "set" {
			svc.data_db.Set(ps[1], ps[2])
		}
		if ps[0] == "del" {
			svc.data_db.Del(ps[1])
		}
	}

	svc.meta_db.Set("LastApplied", fmt.Sprintf("%d", svc.lastApplied))
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

	node := raft.NewNode(store, xport)
	node.Id = nodeId

	/////////////////////////////////////

	log.Println("api server started at", port+100)
	serv_xport := raft.NewUdpTransport("127.0.0.1", port+100)
	serv := NewService(base_dir, node)

	node.Start()

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case buf := <-xport.C:
			msg := raft.DecodeMessage(string(buf));
			if msg == nil {
				log.Println("decode error:", buf)
				continue
			}
			node.HandleRaftMessage(msg)
		case buf := <-serv_xport.C:
			s := string(buf)
			s = strings.Trim(s, "\r\n")
			ps := strings.Split(s, " ")

			if ps[0] == "JoinGroup" {
				node.JoinGroup(ps[1], ps[2])
				continue
			}

			if node.Role == "leader" {
				if ps[0] == "AddMember" {
					node.AddMember(ps[1], ps[2])
				}

				if ps[0] == "set" {
					serv.Set(ps[1], ps[2])
				}
				if ps[0] == "get" {
					serv.Get(ps[1])
				}
				if ps[0] == "del" {
					serv.Del(ps[1])
				}
				// log.Println("reject client's request:", s)
				// continue
			}

		}
	}
}
