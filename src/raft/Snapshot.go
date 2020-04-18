package raft

import (
	"fmt"
	"log"
	"encoding/json"
	"util"
)

type Snapshot struct {
	nodeId string
	term int32
	applied int64
	commitIndex int64
	peers []string
	entries []Entry
}

func MakeSnapshot(node *Node) *Snapshot {
	sn := new(Snapshot)
	sn.nodeId = node.conf.id
	sn.term = node.conf.term
	sn.applied = node.conf.applied
	sn.commitIndex = node.commitIndex
	sn.peers = node.conf.peers
	sn.entries = make([]Entry, 0)

	for idx := node.logs.lastIndex - 1; idx <= node.logs.lastIndex; idx ++ {
		ent := node.logs.GetEntry(idx)
		sn.entries = append(sn.entries, *ent)
	}

	return sn
}

func (sn *Snapshot)Encode() string {
	var bs []byte

	bs, _ = json.Marshal(sn.peers)
	peers := string(bs)

	ents := make([]string, 0)
	for _, e := range sn.entries {
		ents = append(ents, e.Encode())
	}
	bs, _ = json.Marshal(ents)
	logs := string(bs)

	ps := map[string]string{
		"node": sn.nodeId,
		"term": fmt.Sprintf("%d", sn.term),
		"applied": fmt.Sprintf("%d", sn.applied),
		"commitIndex": fmt.Sprintf("%d", sn.commitIndex),
		"peers": peers,
		"logs": logs,
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

	var logs []string
	err = json.Unmarshal([]byte(ps["logs"]), &logs)
	if err != nil {
		log.Println(err)
		return false
	}
	var ents []Entry
	for _, l := range logs {
		ent := DecodeEntry(l)
		ents = append(ents, *ent)
	}

	sn.nodeId = ps["node"]
	sn.term = util.Atoi32(ps["term"])
	sn.applied = util.Atoi64(ps["applied"])
	sn.commitIndex = util.Atoi64(ps["commitIndex"])
	sn.peers = peers
	sn.entries = ents

	return true
}
