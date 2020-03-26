package server

import (
	"log"
)

type Container struct {
	lastApplied int64
	db Db
	admin *raft.Node
	nodes map[string]*raft.Node
}

func NewContainer(nodeId string, addr string, db raft.Db) *Container {
	ret := new(Container)
	ret.db = db
	ret.admin = raft.NewNode(nodeId, addr, db)
	ret.nodes = make(map[string]*Node)
	return ret
}

func (c *Container)Start() {
	c.admin.SetService(c)
	c.admin.Start()
}

func (c *Container)Close() {
	c.admin.Close()
}

// func (c *Container)NewGroup() (int32, int64) {
// }

// func (c *Container)AddMember(nodeId string, nodeAddr string) (int32, int64) {
// }

/* #################### raft.Service interface ######################### */

func (m *Container)LastApplied() int64{
	return m.lastApplied
}

func (m *Container)ApplyEntry(ent *raft.Entry){
	m.lastApplied = ent.Index
}

func (m *Container)InstallSnapshot() {
	log.Println("not implemented")
}
