package raft

import (
	"testing"
)

func TestStorage(t *testing.T){
	s := OpenStorage("tmp")
	t.Log(s)
}
