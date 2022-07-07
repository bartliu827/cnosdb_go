package ae_test

import (
	"container/heap"
	"fmt"
	"github.com/cnosdb/cnosdb/server/ae"
	"testing"
)

func TestHeap(t *testing.T) {
	h := &ae.Int64Heap{}
	heap.Init(h)
	heap.Push(h, int64(1))
	heap.Push(h, int64(100))
	heap.Push(h, int64(2))

	pop := heap.Pop(h)
	fmt.Println(pop)
}
