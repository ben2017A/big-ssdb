package raft

import (
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

// 尝试从指定目录加载配置, 如果之前没有保存配置, 则 IsNew() 返回 true.
func OpenConfig(dir string) *Config {
	c := new(Config)
	c.peers = make([]string, 0)
	c.members = make(map[string]*Member)
	if !c.Open(dir) {
		return nil
	}
	return c
}

func (c *Config)Open(dir string) bool {
	fn := dir + "/config.wal"
	wal := store.OpenWalFile(fn)
	if wal == nil {
		log.Printf("Failed to open wal file: %s", fn)
		return false
	}

	c.wal = wal
	// don't vote for any one after a restart using previous term
	c.vote = c.id

	// TODO: 优化点
	data := c.wal.ReadLast()
	if len(data) > 0 {
		c.decode(data)
	}
	return true
}

func (c *Config)IsNew() bool {
	return len(c.peers) == 0
}

func (c *Config)Init(id string, peers []string) {
	c.id = id
	c.SetPeers(peers)
}

func (c *Config)Close() {
	c.wal.Close()
}

func (c *Config)Fsync() {
	// persist data
	s := c.encode()
	// TODO: 优化点
	c.wal.Append(s)
	if err := c.wal.Fsync(); err != nil {
		log.Fatal(err)
	}
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

func (c *Config)SetRound(term int32, vote string) {
	c.term = term
	c.vote = vote
	c.Fsync()
}

func (c *Config)SetPeers(peers []string) {
	c.members = make(map[string]*Member)
	for _, nodeId := range peers {
		c.addMember(nodeId)
	}
}

func (c *Config)addMember(nodeId string) {
	if c.members[nodeId] != nil {
		return
	}
	if nodeId == c.id {
		c.joined = true
	} else {
		m := NewMember(nodeId)
		c.members[nodeId] = m
	}
	c.updatePeers()
}

func (c *Config)delMember(nodeId string) {
	if nodeId == c.id {
		c.joined = false
	} else {
		delete(c.members, nodeId)
	}
	c.updatePeers()
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

/* ###################### Log Apply ####################### */

// 在 Raft 主线程内被调用
func (c *Config)ApplyEntry(ent *Entry) {
	c.applied = ent.Index

	log.Println("[Apply]", util.StringEscape(ent.Encode()))
	if ent.Type == EntryTypeConf {
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

	// TODO: 优化点, 不需要每次都 fsync
	c.Fsync()
}

func (c *Config)Clean() {
	c.term = 0
	c.vote = ""
	c.commit = 0
	c.applied = 0
	c.joined = false
	c.peers = make([]string, 0)
	c.members = make(map[string]*Member)
	if err := c.wal.Clean(); err != nil {
		log.Fatal(err)
	}
}

func (c *Config)RecoverFromSnapshot(sn *Snapshot) {
	c.Clean()
	c.term = sn.LastTerm()
	c.commit = sn.LastIndex()
	c.applied = sn.LastIndex()
	c.SetPeers(sn.peers)
	c.Fsync()
}
