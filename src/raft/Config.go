package raft

import (
	"log"
	"strings"
)

// 负责 Raft 配置的持久化
type Config struct {
	id string

	term int32
	voteFor string
	members map[string]*Member // 不包含自己

	lastApplied int64

	node *Node
}

// 用新的配置启动, 如果指定的路径存在配置信息, 则返回 nil.
func NewConfig(id string, members []string/*, db_path string*/) *Config {
	c := new(Config)
	c.id = id
	c.members = make(map[string]*Member)
	for _, nodeId := range members {
		c.AddMember(nodeId)
	}
	return c
}

// // 如果指定路径无配置, 返回 nil.
// func OpenConfig(db_path string) *Config {
// 	return nil
// }

func (c *Config)Close() {
}

func (c *Config)CleanAll() {
	c.lastApplied = 0
	c.members = make(map[string]*Member)
	c.SaveState(1, "")
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

func (c *Config)AddMember(nodeId string) {
	if nodeId == c.id {
		return
	}
	m := NewMember(nodeId)
	c.members[nodeId] = m
}

func (c *Config)DelMember(nodeId string) {
	if nodeId == c.id {
		return
	}
	delete(c.members, nodeId)
}

/* ###################### Service interface ####################### */

func (c *Config)LastApplied() int64{
	return c.lastApplied
}

func (c *Config)ApplyEntry(ent *Entry){
	c.lastApplied = ent.Index

	if ent.Type == EntryTypeConf {
		log.Println("[Apply]", ent.Encode())
		ps := strings.Split(ent.Data, " ")
		cmd := ps[0]
		if cmd == "AddMember" {
			nodeId := ps[1]
			c.AddMember(nodeId)
		} else if cmd == "DelMember" {
			nodeId := ps[1]
			c.DelMember(nodeId)
		}
	}
}
