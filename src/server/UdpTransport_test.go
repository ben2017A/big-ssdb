package server

import (
	"testing"
	"fmt"
	"log"

	"raft"
	"util"
)

func TestUdpTransport(t *testing.T){
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	
	log.Println("Use nc 127.0.0.1 9000 to connect")

	xport := NewUdpTransport("127.0.0.1", 9000)
	xport.Connect("1", "127.0.0.1:9000")

	// close socket
	go func(){
		util.Sleep(0.01)
		xport.Close()
	}()

	// send
	go func(){
		for {
			msg := raft.NewGossipMsg("1")
			msg.Src = "me"
			if xport.Send(msg) == false {
				break
			}
			util.Sleep(0.007)
		}
	}()

	// recv
	for {
		msg := <-xport.C()
		if msg == nil {
			log.Println("xport closed")
			break
		}
		fmt.Println(msg)
	}

	util.Sleep(0.1)
}
