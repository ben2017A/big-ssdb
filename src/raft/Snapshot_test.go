package raft

import (
	"testing"
	"log"
	"util"
	// "bytes"
	// "sync"
)

func TestSnapshot(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	conf := OpenConfig("./tmp/n1")
	conf.Clean()
	conf.Init("n1", []string{"n1"})

	logs := OpenBinlog("./tmp/n1")
	logs.Clean()

	n1 := NewNode(conf, logs)
	defer n1.Close()

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
	log.Println(s2.Encode())

	if enc != s2.Encode() {
		t.Fatal("")
	}
}
