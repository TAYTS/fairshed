package dispatcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tayts/fairsched/internal/engine"
)

func newTask(tenantID string, cost float64) *engine.Task {
	return &engine.Task{TenantId: tenantID, Cost: cost}
}

// --- Enqueue ---

func TestEnqueue_AssignsVFT(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	task := newTask("t1", 2.0)

	err := d.Enqueue(task)

	assert.NoError(t, err)
	assert.Equal(t, 2.0, task.VFT, "VFT should be stamped on task after enqueue")
}

func TestEnqueue_AddsToQueue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
	assert.Equal(t, 2, d.Len())
}

func TestEnqueue_StoppedReturnsError(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.Stop()

	err := d.Enqueue(newTask("t1", 1.0))
	assert.ErrorIs(t, err, ErrStopped)
}

// --- DequeueNonBlocking ---

func TestDequeueNonBlocking_EmptyReturnsError(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	_, err := d.DequeueNonBlocking()
	assert.ErrorIs(t, err, ErrNoTasks)
}

func TestDequeueNonBlocking_ReturnsMinVFT(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	// Enqueue all tasks before any dequeue so VFT engine sees all weights upfront.
	// Each independent tenant starts from vt≈0, so VFT ≈ cost/weight.
	// We compare relative ordering, not exact values, since real elapsed time shifts VFTs slightly.
	taskA := newTask("a", 3.0)
	taskB := newTask("b", 1.0)
	taskC := newTask("c", 2.0)
	assert.NoError(t, d.Enqueue(taskA))
	assert.NoError(t, d.Enqueue(taskB))
	assert.NoError(t, d.Enqueue(taskC))

	first, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.Equal(t, taskB, first, "task with lowest VFT (cost=1.0) should be dequeued first")
}

func TestDequeueNonBlocking_DrainOrder(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	err := d.Enqueue(newTask("a", 3.0))
	assert.NoError(t, err)
	err = d.Enqueue(newTask("b", 1.0))
	assert.NoError(t, err)
	err = d.Enqueue(newTask("c", 2.0))
	assert.NoError(t, err)

	prev := 0.0
	for d.Len() > 0 {
		task, err := d.DequeueNonBlocking()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, task.VFT, prev, "tasks must be dequeued in increasing VFT order")
		prev = task.VFT
	}
}

func TestDequeueNonBlocking_StoppedReturnsError(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	err := d.Enqueue(newTask("t1", 1.0))
	assert.NoError(t, err)
	d.Stop()

	_, err = d.DequeueNonBlocking()
	assert.ErrorIs(t, err, ErrStopped)
}


func TestDequeueNonBlocking_MultiTenant_Fairness(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	// Enqueue one task each from two tenants with equal cost
	taskA := newTask("tenantA", 1.0)
	taskB := newTask("tenantB", 1.0)
	assert.NoError(t, d.Enqueue(taskA))
	assert.NoError(t, d.Enqueue(taskB))

	// Dispatch one task — VT advances to ~1.0
	first, err := d.DequeueNonBlocking()
	assert.NoError(t, err)

	// Re-enqueue a second task for the tenant that was just served
	taskSecond := newTask(first.TenantId, 1.0)
	assert.NoError(t, d.Enqueue(taskSecond))

	// The un-served tenant should be prioritised next
	second, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.NotEqual(t, first.TenantId, second.TenantId,
		"un-served tenant should be prioritised after VT advances")

	third, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.Equal(t, first.TenantId, third.TenantId,
		"served tenant should be prioritised after second task for that tenant is enqueued")
}

func TestEnqueueDequeue_MultiTenant_WeightedOrdering(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	// heavy weight=4.0: VFT per task = cost/4.0 = 0.25, 0.50, 0.75
	// light weight=1.0: VFT per task = cost/1.0 = 1.00, 2.00, 3.00
	// All heavy VFTs are lower than all light VFTs, so heavy tasks drain first
	d.vftEngine.SetTenantWeight("heavy", 4.0)
	d.vftEngine.SetTenantWeight("light", 1.0)

	for range 3 {
		assert.NoError(t, d.Enqueue(newTask("heavy", 1.0)))
		assert.NoError(t, d.Enqueue(newTask("light", 1.0)))
	}

	for range 3 {
		task, err := d.DequeueNonBlocking()
		assert.NoError(t, err)
		assert.Equal(t, "heavy", task.TenantId, "heavy tasks should be dequeued first")
	}
	for range 3 {
		task, err := d.DequeueNonBlocking()
		assert.NoError(t, err)
		assert.Equal(t, "light", task.TenantId, "light tasks should follow")
	}
}


// --- Dequeue (blocking) ---

func TestDequeue_BlocksUntilEnqueue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	ctx := context.Background()

	var task *engine.Task
	var wg sync.WaitGroup
	wg.Go(func() {
		task, _ = d.Dequeue(ctx)
	})

	time.Sleep(20 * time.Millisecond) // let goroutine block on cond.Wait
	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
	wg.Wait()

	assert.NotNil(t, task)
	assert.Equal(t, "t1", task.TenantId)
}

func TestDequeue_ContextCancelledWhileWaiting(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	ctx, cancel := context.WithCancel(context.Background())

	var err error
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err = d.Dequeue(ctx)
	})

	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.ErrorIs(t, err, context.Canceled)
}

func TestDequeue_StoppedWhileWaiting(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	var err error
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err = d.Dequeue(context.Background())
	})

	time.Sleep(20 * time.Millisecond)
	d.Stop()
	wg.Wait()

	assert.ErrorIs(t, err, ErrStopped)
}

func TestDequeue_StoppedBeforeCall(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.Stop()

	_, err := d.Dequeue(context.Background())
	assert.ErrorIs(t, err, ErrStopped)
}

// --- Stop ---

func TestStop_IdempotentEnqueue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.Stop()
	d.Stop() // must not panic

	err := d.Enqueue(newTask("t1", 1.0))
	assert.ErrorIs(t, err, ErrStopped)
}

// --- Concurrency ---

func TestConcurrentEnqueueDequeue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	const n = 100
	var wg sync.WaitGroup

	for range n {
		wg.Go(func() {
			assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
		})
		wg.Go(func() {
			_, err := d.DequeueNonBlocking()
			if err != nil {
				assert.ErrorIs(t, err, ErrNoTasks)
			}
		})
	}
	wg.Wait()

	// Drain remainder until empty
	for {
		_, err := d.DequeueNonBlocking()
		if err == ErrNoTasks {
			break
		}
	}
}

// --- Benchmarks ---

// BenchmarkEnqueue-4   	  863566	      1378 ns/op	     121 B/op	       1 allocs/op
func BenchmarkEnqueue(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())
	b.ResetTimer()

	for b.Loop() {
		_ = d.Enqueue(newTask("t1", 1.0))
	}
}

// BenchmarkEnqueue_PreallocatedTasks-4   	  897829	      1342 ns/op	      39 B/op	       0 allocs/op
func BenchmarkEnqueue_PreallocatedTasks(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())

	// Pre-allocate task pool to isolate heap + VFT overhead
	b.StopTimer()
	const poolSize = 10000
	tasks := make([]*engine.Task, poolSize)
	for i := range poolSize {
		tasks[i] = newTask("t1", 1.0)
	}
	b.StartTimer()

	taskIdx := 0
	for b.Loop() {
		_ = d.Enqueue(tasks[taskIdx%poolSize])
		taskIdx++
	}
}

// BenchmarkDequeueNonBlocking-4   	  311307	      3867 ns/op	      80 B/op	       1 allocs/op
func BenchmarkDequeueNonBlocking(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())

	// Pre-fill queue
	b.StopTimer()
	const preloadSize = 10000
	for range preloadSize {
		_ = d.Enqueue(newTask("t1", 1.0))
	}
	b.StartTimer()

	for b.Loop() {
		_, _ = d.DequeueNonBlocking()
		// Re-enqueue to keep queue populated
		_ = d.Enqueue(newTask("t1", 1.0))
	}
}

// BenchmarkConcurrentEnqueueDequeue-4   	    2061	    516981 ns/op	   16145 B/op	     501 allocs/op
func BenchmarkConcurrentEnqueueDequeue(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())

	b.ResetTimer()
	for b.Loop() {
		var wg sync.WaitGroup

		const n = 100
		for range n {
			wg.Go(func() {
				_ = d.Enqueue(newTask("t1", 1.0))
			})
			wg.Go(func() {
				_, _ = d.DequeueNonBlocking()
			})
		}
		wg.Wait()
	}
}

// BenchmarkConcurrentEnqueueDequeue_PreallocatedTasks-4   	    2370	    526037 ns/op	   11352 B/op	     401 allocs/op
func BenchmarkConcurrentEnqueueDequeue_PreallocatedTasks(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())

	// Pre-allocate task pool to isolate goroutine/sync overhead
	b.StopTimer()
	const poolSize = 10000
	tasks := make([]*engine.Task, poolSize)
	for i := range poolSize {
		tasks[i] = newTask("t1", 1.0)
	}
	b.StartTimer()

	taskIdx := 0
	for b.Loop() {
		var wg sync.WaitGroup

		const n = 100
		for i := range n {
			idx := (taskIdx + i) % poolSize // Compute index before launching goroutine
			wg.Go(func() {
				_ = d.Enqueue(tasks[idx])
			})
			wg.Go(func() {
				_, _ = d.DequeueNonBlocking()
			})
		}
		taskIdx += n
		wg.Wait()
	}
}
