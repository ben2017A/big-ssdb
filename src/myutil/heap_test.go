package myutil

import (
	"testing"
	"fmt"
)

func TestHeap(t *testing.T){
	q := new(IntPriorityQueue)
	q.Push(2, 2)
	q.Push(1, 1)
	q.Push(3, 3)
	fmt.Println(q)
	
	for q.Size() > 0 {
		w, v := q.Pop()
		fmt.Println("pop", w, v)
	}
}
