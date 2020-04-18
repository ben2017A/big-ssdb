package raft

import (
	"log"
	"strings"
)

// 负责 Raft 配置的持久化(区别于 Option)
type Config struct {
	id string
	term int32
	voteFor string
	applied int64

	joined bool // 是否已加入组
	peers []string // 包括自己
	members map[string]*Member // 不包括自己

	node *Node
}

// 用新的配置启动, 如果指定的路径存在配置信息, 则返回 nil.
func NewConfig(id string, members []string/*, db_path string*/) *Config {
	c := new(Config)
	c.id = id
	c.peers = make([]string, 0)
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
	c.term = 0
	c.voteFor = ""
	c.applied = 0
	c.joined = false
	c.peers = make([]string, 0)
	c.members = make(map[string]*Member)
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

func (c *Config)updatePeers() {
	c.peers = make([]string, 0)
	if c.joined {
		c.peers = append(c.peers, c.id)
	}
	for id, _ := range c.members {
		c.peers = append(c.peers, id)
	}
}

func (c *Config)AddMember(nodeId string) {
	if nodeId == c.id {
		c.joined = true
	} else {
		m := NewMember(nodeId)
		c.members[nodeId] = m
	}
	c.updatePeers()
}

func (c *Config)DelMember(nodeId string) {
	if nodeId == c.id {
		c.joined = false
	} else {
		delete(c.members, nodeId)
	}
	c.updatePeers()
}

/* ###################### Log Apply ####################### */

func (c *Config)LastApplied() int64{
	return c.applied
}

func (c *Config)ApplyEntry(ent *Entry){
	c.applied = ent.Index

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
