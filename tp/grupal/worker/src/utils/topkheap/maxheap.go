package topkheap

import (
	"container/heap"
	"sort"
)

func NewTopKMaxHeap[V Ordered, D any](k int) TopKHeap[V, D] {
	bh := &baseHeap[V, D]{}
	bh.less = func(i, j int) bool {
		return bh.items[i].value < bh.items[j].value // internal min-heap
	}
	heap.Init(bh)
	return &TopKMaxHeap[V, D]{h: bh, k: k}
}

type TopKMaxHeap[V Ordered, D any] struct {
	h *baseHeap[V, D]
	k int
}

func (t *TopKMaxHeap[V, D]) Insert(value V, data D) {
	if t.h.Len() < t.k {
		heap.Push(t.h, item[V, D]{value, data})
		return
	}
	if t.k == 0 {
		return
	}
	if value > t.h.items[0].value {
		heap.Pop(t.h)
		heap.Push(t.h, item[V, D]{value, data})
	}
}

func (t *TopKMaxHeap[V, D]) GetTopK() []D {
	tmp := make([]item[V, D], len(t.h.items))
	copy(tmp, t.h.items)
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].value > tmp[j].value
	})
	res := make([]D, len(tmp))
	for i, it := range tmp {
		res[i] = it.data
	}
	return res
}

func (t *TopKMaxHeap[V, D]) Len() int {
	return t.h.Len()
}

func (t *TopKMaxHeap[V, D]) Delete() {
	if t.Len() > 0 {
		heap.Pop(t.h)
	}
}
