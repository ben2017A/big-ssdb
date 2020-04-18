package raft

import (
	"log"
	"strings"
)

// 负责 Raft 配置的持久化(区别于 Option)
type Config struct {
	// ## 持久化, 且必须原子性操作 ##
	id string
	term int32
	voteFor string
	applied int64
	peers []string // 包括自己

	// ## 非持久化 ##
	joined bool // 是否已加入组
	members map[string]*Member // 不包括自己

	node *Node
}

// 用新的配置启动, 如果指定的路径存在配置信息, 则返回 nil.
func NewConfig(id string, peers []string/*, db_path string*/) *Config {
	c := new(Config)
	c.id = id
	c.SetPeers(peers)
	return c
}

// // 如果指定路径无配置, 返回 nil.
// func OpenConfig(db_path string) *Config {
// 	return nil
// }

func (c *Config)Close() {
}

func (c *Config)Fsync() {
	c.peers = make([]string, 0)
	if c.joined {
		c.peers = append(c.peers, c.id)
	}
	for id, _ := range c.members {
		c.peers = append(c.peers, id)
	}

	// TODO: save
}

func (c *Config)CleanAll() {
	c.term = 0
	c.voteFor = ""
	c.applied = 0
	c.joined = false
	c.peers = make([]string, 0)
	c.members = make(map[string]*Member)
	c.Fsync()
}

func (c *Config)SetRound(term int32, voteFor string) {
	c.term = term
	c.voteFor = voteFor
	c.Fsync()
}

func (c *Config)SetPeers(peers []string) {
	c.members = make(map[string]*Member)
	for _, nodeId := range peers {
		if nodeId == c.id {
			c.joined = true
		} else {
			m := NewMember(nodeId)
			c.members[nodeId] = m
		}
	}
	c.Fsync()
}

func (c *Config)addMember(nodeId string) {
	if nodeId == c.id {
		c.joined = true
	} else {
		m := NewMember(nodeId)
		c.members[nodeId] = m
	}
	c.Fsync()
}

func (c *Config)delMember(nodeId string) {
	if nodeId == c.id {
		c.joined = false
	} else {
		delete(c.members, nodeId)
	}
	c.Fsync()
}

/* ###################### Log Apply ####################### */

// 在 Raft 主线程内被调用
func (c *Config)ApplyEntry(ent *Entry) {
	c.applied = ent.Index

	if ent.Type == EntryTypeConf {
		log.Println("[Apply]", ent.Encode())
		ps := strings.Split(ent.Data, " ")
		cmd := ps[0]
		if cmd == "AddMember" {
			nodeId := ps[1]
			c.addMember(nodeId)
		} else if cmd == "DelMember" {
			nodeId := ps[1]
			c.delMember(nodeId)
		}
	}
}

func (c *Config)ResetFromSnapshot(sn *Snapshot) {
	c.CleanAll()
	c.term = sn.term
	c.applied = sn.applied
	c.SetPeers(sn.peers)
}
