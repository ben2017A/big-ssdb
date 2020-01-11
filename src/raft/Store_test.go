package raft

import (
	"testing"
)

func TestStore(t *testing.T){
	s := OpenStore("tmp")
	t.Log(s)
}
