package raft

import (
	"log"
)

type Role string

const(
	Leader Role = "leader"
	Follower 	= "follower"
	Candidate	= "candidate"
)

type Node struct{
	id string
	role Role

	term int32
	voteFor string
}

type Member struct{
	id string
}

// 事件处理函数
func (this *Node)Run() {
	/*
	*/

}


func (this *Node)AddMember() {
	if this.role != Leader {
		log.Fatal("www")
		return
	}
}
