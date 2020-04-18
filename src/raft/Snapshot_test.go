package raft

import (
	"testing"
	"log"
	"util"
	// "bytes"
	// "sync"
)

func TestSnapshot(t *testing.T){
	n1 := NewNode(NewConfig("n1", []string{"n1"}))
	n1.Start()
	util.Sleep(0.1)
	log.Println(n1.Info())

	n1.Propose("a")
	n1.Propose("b")
	n1.Propose("c")
	util.Sleep(0.1)

	sn := MakeSnapshot(n1)
	enc := sn.Encode()

	var s2 Snapshot
	s2.Decode(enc)

	log.Println(enc)
	log.Println(s2)
}
