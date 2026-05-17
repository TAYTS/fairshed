package dispatcher

import "github.com/tayts/fairsched/internal/engine"

// TaskHeap implements heap.Interface for task priority queue (min-heap on VFT)
type TaskHeap []*engine.Task

func (h TaskHeap) Len() int {
	return len(h)
}

func (h TaskHeap) Less(i, j int) bool {
	return h[i].VFT < h[j].VFT
}

func (h TaskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TaskHeap) Push(x any) {
	*h = append(*h, x.(*engine.Task))
}

func (h *TaskHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
