package raft

import (
	"os"
	"log"
	"strings"
	"encoding/json"

	"store"
	"util"
)

// 负责 Raft 配置的持久化(区别于 Option)
type Config struct {
	// ## 持久化, 且必须原子性操作 ##
	id string
	term int32
	commit int64 // 事实上可以不持久化
	applied int64
	peers []string // 包括自己

	// ## 非持久化 ##
	vote string
	joined bool // 是否已加入组
	members map[string]*Member // 不包括自己

	node *Node
	wal *store.WalFile
}

// 新配置, 如果指定目录已存在旧配置, 则先删除旧配置
func NewConfig(id string, peers []string, dir string) *Config {
	fn := dir + "/config.wal"
	if util.FileExists(fn) {
		err := os.Remove(fn)
		if err != nil {
			log.Printf("Failed to remove config file: %s", fn)
			return nil
		}
	}
	
	c := OpenConfig(dir)
	if c == nil {
		return nil
	}
	c.Init(id, peers)
	
	return c
}

// 尝试从指定目录加载配置, 如果之前没有保存配置, 则 IsNew() 返回 true.
func OpenConfig(dir string) *Config {
	fn := dir + "/config.wal"
	wal := store.OpenWalFile(fn)
	if wal == nil {
		log.Printf("Failed to open wal file: %s", fn)
		return nil
	}

	c := new(Config)
	c.wal = wal
	// don't vote for any one after a restart using previous term
	c.vote = c.id

	data := c.wal.ReadLast()
	if len(data) > 0 {
		c.decode(data)
	}

	return c
}

func (c *Config)Init(id string, peers []string) {
	c.id = id
	c.SetPeers(peers)
	c.Fsync()
}

func (c *Config)Close() {
	c.Fsync()
	c.wal.Close()
}

func (c *Config)IsNew() bool {
	return len(c.peers) == 0
}

func (c *Config)encode() string {
	arr := map[string]string{
		"id": c.id,
		"term": util.I32toa(c.term),
		"commit": util.I64toa(c.commit),
		"applied": util.I64toa(c.applied),
	}
	ps := ""
	for _, p := range c.peers {
		if len(ps) > 0 {
			ps += ","
		}
		ps += p
	}
	arr["peers"] = ps
	bs, _ := json.Marshal(arr)
	return string(bs)
}

func (c *Config)decode(data string) {
	var arr map[string]string
	err := json.Unmarshal([]byte(data), &arr)
	if err != nil {
		log.Fatal("bad data:", data)
	}

	c.id = arr["id"]
	c.term = util.Atoi32(arr["term"])
	c.commit = util.Atoi64(arr["commit"])
	c.applied = util.Atoi64(arr["applied"])
	if len(arr["peers"]) > 0 {
		ps := strings.Split(arr["peers"], ",")
		c.SetPeers(ps)
	}
}

func (c *Config)Fsync() {
	c.peers = make([]string, 0)
	if c.joined {
		c.peers = append(c.peers, c.id)
	}
	for id, _ := range c.members {
		c.peers = append(c.peers, id)
	}

	// persist data
	s := c.encode()
	c.wal.Append(s)
	c.wal.Fsync()
}

func (c *Config)CleanAll() {
	c.term = 0
	c.vote = ""
	c.commit = 0
	c.applied = 0
	c.joined = false
	c.peers = make([]string, 0)
	c.members = make(map[string]*Member)
	c.Fsync()
}

func (c *Config)SetRound(term int32, vote string) {
	c.term = term
	c.vote = vote
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
}

func (c *Config)delMember(nodeId string) {
	if nodeId == c.id {
		c.joined = false
	} else {
		delete(c.members, nodeId)
	}
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

	// TODO: 优化
	c.Fsync()
}

func (c *Config)RecoverFromSnapshot(sn *Snapshot) {
	c.CleanAll()
	c.term = sn.LastTerm()
	c.commit = sn.LastIndex()
	c.applied = sn.LastIndex()
	c.SetPeers(sn.peers)
	c.Fsync()
}
