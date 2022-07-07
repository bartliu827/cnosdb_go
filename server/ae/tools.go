package ae

// An Int64Heap is a min-heap of ints.
type Int64Heap []int64

func (h Int64Heap) Len() int           { return len(h) }
func (h Int64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h Int64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Int64Heap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int64))
}

func (h *Int64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
