package raft

import (
	"testing"
	"fmt"
	"log"
)

func TestUdpTransport(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	xport := NewUdpTransport("127.0.0.1", 9000)

	for {
		msg := <-xport.C
		fmt.Println(msg)
	}
}
