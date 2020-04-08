package raft

import (
	"log"
	// "strings"
)

// 负责 Raft 配置的持久化
type Config struct {
	id string
	addr string

	term int32
	voteFor string
	members map[string]*Member // 不包含自己

	lastApplied int64

	node *Node
}

// 用新的配置启动, 如果指定的路径存在配置信息, 则返回 nil.
func NewConfig(id string, addr string, members map[string]string/*, db_path string*/) *Config {
	c := new(Config)
	c.id = id
	c.addr = addr
	c.members = make(map[string]*Member)
	for nodeId, nodeAddr := range members {
		if nodeId == c.id {
			continue
		}
		m := NewMember(nodeId, nodeAddr)
		c.members[nodeId] = m
	}
	return c
}

// 如果指定路径无配置, 返回 nil.
func OpenConfig(db_path string) *Config {
	return nil
}

func (c *Config)Init(node *Node) {
	c.node = node
	c.node.members = c.members
}

func (c *Config)NewTerm() {
	c.SaveState(c.term + 1, c.id)
}

func (c *Config)SetVoteFor(voteFor string) {
	c.SaveState(c.term, voteFor)
}

func (c *Config)SaveState(term int32, voteFor string) {
	c.term = term
	c.voteFor = voteFor
}

// func (c *Config)AddMember(nodeId string, nodeAddr string) {
// 	if nodeId == c.id {
// 		return
// 	}
// 	if c.members[nodeId] != nil {
// 		return
// 	}
// 	m := NewMember(nodeId, nodeAddr)
// 	c.members[m.Id] = m
// 	c.node.members = c.members
// }

// func (c *Config)DelMember(nodeId string) {
// 	if nodeId == c.id {
// 		return
// 	}
// 	if c.members[nodeId] == nil {
// 		return
// 	}
// 	delete(c.members, nodeId)
// 	c.node.members = c.members
// }


/* ###################### Service interface ####################### */

func (c *Config)LastApplied() int64{
	return c.lastApplied
}

func (c *Config)ApplyEntry(ent *Entry){
	c.lastApplied = ent.Index

	if ent.Type == EntryTypeConf {
		log.Println("[Apply]", ent.Encode())
	}
}
