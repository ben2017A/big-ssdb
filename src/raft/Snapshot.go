package raft

import (
	"log"
	"encoding/json"
)

type Snapshot struct {
	nodeId string
	peers []string
	lastEntry *Entry
}

func MakeSnapshot(node *Node) *Snapshot {
	sn := new(Snapshot)
	sn.nodeId = node.conf.id
	sn.peers = node.conf.peers
	sn.lastEntry = node.logs.GetEntry(node.conf.applied)

	return sn
}

func (sn *Snapshot)LastTerm() int32 {
	return sn.lastEntry.Term
}

func (sn *Snapshot)LastIndex() int64 {
	return sn.lastEntry.Index
}

func (sn *Snapshot)Encode() string {
	var bs []byte

	bs, _ = json.Marshal(sn.peers)
	peers := string(bs)

	ps := map[string]string{
		"node": sn.nodeId,
		"peers": peers,
		"lastEntry": sn.lastEntry.Encode(),
	}

	bs, _ = json.Marshal(ps)
	// bs, _ = json.MarshalIndent(ps, "", "    ")
	return string(bs)
}

func (sn *Snapshot)Decode(data string) bool {
	var err error

	var ps map[string]string
	err = json.Unmarshal([]byte(data), &ps)
	if err != nil {
		log.Println(err)
		return false
	}

	var peers []string
	err = json.Unmarshal([]byte(ps["peers"]), &peers)
	if err != nil {
		log.Println(err)
		return false
	}

	sn.nodeId = ps["node"]
	sn.peers = peers
	sn.lastEntry = DecodeEntry(ps["lastEntry"])

	return true
}
