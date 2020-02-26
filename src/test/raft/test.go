package main

import (
	"fmt"
	"time"
	"log"
	"os"
	"strconv"

	"raft"
)

const TimerInterval = 100

func main(){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	ticker := time.NewTicker(TimerInterval * time.Millisecond)
	defer ticker.Stop()

	store := test.raft.NewFakeStorage()
	xport := test.raft.NewFakeTransport()

	node := raft.NewNode("n1", store, xport)
	node.Start()

	for{
		select{
		case <-ticker.C:
			node.Tick(TimerInterval)
		case msg := <-raft_xport.C:
			node.HandleRaftMessage(msg)
	}
}