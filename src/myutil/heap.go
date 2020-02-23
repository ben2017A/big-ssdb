package myutil

import (
	"container/heap"
)

type HeapItem struct {
	value interface{}
	int_weight int
}

// go is Min Heap, C++ STL priority queue is Max Heap
type _intPriorityQueue []*HeapItem

type IntPriorityQueue struct {
	q _intPriorityQueue
}

func NewIntPriorityQueue() *IntPriorityQueue {
	ret := new(IntPriorityQueue)
	ret.q = make(_intPriorityQueue, 0)
	heap.Init(&ret.q)
	return ret
}

func (pq *IntPriorityQueue)Size() int {
	return len(pq.q)
}

func (pq *IntPriorityQueue)Top() (int, interface{}) {
	return pq.q[0].int_weight, pq.q[0].value
}

func (pq *IntPriorityQueue)Push(weight int, value interface{}) {
	item := &HeapItem{value, weight}
	heap.Push(&pq.q, item)
}

func (pq *IntPriorityQueue)Pop() (int, interface{}) {
	item := heap.Pop(&pq.q).(*HeapItem)
	return item.int_weight, item.value
}

///////////////////////// WTF! /////////////////////////////////////


func (pq _intPriorityQueue) Len() int { return len(pq) }

func (pq _intPriorityQueue) Less(i, j int) bool {
	return pq[i].int_weight < pq[j].int_weight
}

func (pq _intPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *_intPriorityQueue) Push(x interface{}) {
	item := x.(*HeapItem)
	*pq = append(*pq, item)
}

func (pq *_intPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

