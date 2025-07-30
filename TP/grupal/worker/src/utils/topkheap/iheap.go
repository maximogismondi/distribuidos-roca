package topkheap

// Ordered defines types that can be ordered
// like integers, floats, and strings.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

// TopKHeap interface for a heap that stores values with ordering
// and can return the top-K items.
type TopKHeap[V Ordered, D any] interface {
	Insert(value V, data D)
	GetTopK() []D
	Len() int
	Delete()
}

type item[V Ordered, D any] struct {
	value V
	data  D
}

type baseHeap[V Ordered, D any] struct {
	items []item[V, D]
	less  func(i, j int) bool
}

func (h *baseHeap[V, D]) Len() int {
	return len(h.items)
}

func (h *baseHeap[V, D]) Less(i, j int) bool {
	return h.less(i, j)
}

func (h *baseHeap[V, D]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *baseHeap[V, D]) Push(x any) {
	h.items = append(h.items, x.(item[V, D]))
}

func (h *baseHeap[V, D]) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}
