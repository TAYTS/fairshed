package dispatcher

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tayts/fairsched/internal/engine"
)

func task(vft float64) *engine.Task {
	return &engine.Task{VFT: vft}
}

// --- heap.Interface (via container/heap) ---

func TestHeapPop_ReturnsMinVFT(t *testing.T) {
	h := &TaskHeap{}
	heap.Init(h)
	heap.Push(h, task(3.0))
	heap.Push(h, task(1.0))
	heap.Push(h, task(2.0))

	assert.Equal(t, 1.0, heap.Pop(h).(*engine.Task).VFT, "first pop should return lowest VFT")
	assert.Equal(t, 2.0, heap.Pop(h).(*engine.Task).VFT)
	assert.Equal(t, 3.0, heap.Pop(h).(*engine.Task).VFT)
}

func TestHeapPop_StableMinOrder(t *testing.T) {
	h := &TaskHeap{}
	heap.Init(h)

	vfts := []float64{5.0, 0.5, 3.0, 1.5, 4.0, 2.0}
	for _, v := range vfts {
		heap.Push(h, task(v))
	}

	prev := 0.0
	for h.Len() > 0 {
		got := heap.Pop(h).(*engine.Task).VFT
		assert.GreaterOrEqual(t, got, prev, "pops must be in non-decreasing VFT order")
		prev = got
	}
}

func TestHeapInit_HeapifiesExistingSlice(t *testing.T) {
	h := &TaskHeap{task(3.0), task(1.0), task(2.0)}
	heap.Init(h)

	assert.Equal(t, 1.0, heap.Pop(h).(*engine.Task).VFT, "heap.Init should establish min-heap property")
}

func TestHeap_EmptyAfterAllPops(t *testing.T) {
	h := &TaskHeap{}
	heap.Init(h)
	heap.Push(h, task(1.0))
	heap.Pop(h)

	assert.Equal(t, 0, h.Len())
}

// --- Benchmarks ---

// BenchmarkHeapPush-4   	23897618	        50.90 ns/op	     123 B/op	       1 allocs/op
func BenchmarkHeapPush(b *testing.B) {
	h := &TaskHeap{}
	heap.Init(h)

	for b.Loop() {
		heap.Push(h, task(float64(b.N)))
	}
}

// BenchmarkHeapPop-4   	    1645	    629701 ns/op	       0 B/op	       0 allocs/op
func BenchmarkHeapPop(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		h := &TaskHeap{}
		heap.Init(h)
		for i := range 1000 {
			heap.Push(h, task(float64(i)))
		}
		b.StartTimer()

		for h.Len() > 0 {
			heap.Pop(h)
		}
	}
}

// BenchmarkHeapPushPop-4   	 5397280	       202.8 ns/op	      80 B/op	       1 allocs/op
func BenchmarkHeapPushPop(b *testing.B) {
	h := &TaskHeap{}
	heap.Init(h)

	for b.Loop() {
		heap.Push(h, task(1.0))
		heap.Pop(h)
	}
}
