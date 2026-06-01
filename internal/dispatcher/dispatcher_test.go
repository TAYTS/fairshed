package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/limiter"
	"github.com/tayts/fairsched/internal/tenant"
)

func newTask(tenantID string, cost float64) *engine.Task {
	return &engine.Task{TenantID: tenantID, Cost: cost}
}

// --- Enqueue ---

func TestEnqueue_AssignsVFT(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 2.0, MaxConcurrency: tenant.DefaultConcurrency})
	task := newTask("t1", 2.0)

	err := d.Enqueue(task)

	assert.NoError(t, err)
	assert.InDelta(t, 1.0, task.VFT, 0.01, "VFT = cost/weight = 2.0/2.0 = 1.0")
}

func TestEnqueue_AddsToQueue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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

func TestEnqueue_UnregisteredTenantReturnsError(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	// No call to RegisterTenant
	err := d.Enqueue(newTask("unknown", 1.0))
	assert.ErrorIs(t, err, engine.ErrTenantNotRegistered,
		"enqueueing for an unregistered tenant must be rejected")
}

func TestDequeueNonBlocking_EmptyReturnsError(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	_, err := d.DequeueNonBlocking()
	assert.ErrorIs(t, err, ErrNoTasks)
}

func TestDequeueNonBlocking_ReturnsMinVFT(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "a", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	d.RegisterTenant(&tenant.Tenant{ID: "b", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	d.RegisterTenant(&tenant.Tenant{ID: "c", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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
	d.RegisterTenant(&tenant.Tenant{ID: "a", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	d.RegisterTenant(&tenant.Tenant{ID: "b", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	d.RegisterTenant(&tenant.Tenant{ID: "c", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
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
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	err := d.Enqueue(newTask("t1", 1.0))
	assert.NoError(t, err)
	d.Stop()

	_, err = d.DequeueNonBlocking()
	assert.ErrorIs(t, err, ErrStopped)
}

func TestDequeueNonBlocking_MultiTenant_Fairness(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "tenantA", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	d.RegisterTenant(&tenant.Tenant{ID: "tenantB", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

	// Enqueue one task each from two tenants with equal cost
	taskA := newTask("tenantA", 1.0)
	taskB := newTask("tenantB", 1.0)
	assert.NoError(t, d.Enqueue(taskA))
	assert.NoError(t, d.Enqueue(taskB))

	// Dispatch one task — VT advances to ~1.0
	first, err := d.DequeueNonBlocking()
	assert.NoError(t, err)

	// Re-enqueue a second task for the tenant that was just served
	taskSecond := newTask(first.TenantID, 1.0)
	assert.NoError(t, d.Enqueue(taskSecond))

	// The un-served tenant should be prioritised next
	second, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.NotEqual(t, first.TenantID, second.TenantID,
		"un-served tenant should be prioritised after VT advances")

	third, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.Equal(t, first.TenantID, third.TenantID,
		"served tenant should be prioritised after second task for that tenant is enqueued")
}

func TestEnqueueDequeue_MultiTenant_WeightedOrdering(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())

	// heavy weight=4.0: VFT per task = cost/4.0 = 0.25, 0.50, 0.75
	// light weight=1.0: VFT per task = cost/1.0 = 1.00, 2.00, 3.00
	// All heavy VFTs are lower than all light VFTs, so heavy tasks drain first
	d.RegisterTenant(&tenant.Tenant{ID: "heavy", Weight: 4.0, MaxConcurrency: tenant.DefaultConcurrency})
	d.RegisterTenant(&tenant.Tenant{ID: "light", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

	for range 3 {
		assert.NoError(t, d.Enqueue(newTask("heavy", 1.0)))
		assert.NoError(t, d.Enqueue(newTask("light", 1.0)))
	}

	for range 3 {
		task, err := d.DequeueNonBlocking()
		assert.NoError(t, err)
		assert.Equal(t, "heavy", task.TenantID, "heavy tasks should be dequeued first")
	}
	for range 3 {
		task, err := d.DequeueNonBlocking()
		assert.NoError(t, err)
		assert.Equal(t, "light", task.TenantID, "light tasks should follow")
	}
}

// --- Dequeue (blocking) ---

func TestDequeue_BlocksUntilEnqueue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
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
	assert.Equal(t, "t1", task.TenantID)
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
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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

func TestConcurrentEnqueue_LimitNeverExceeded(t *testing.T) {
	const limit = 5
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: limit})

	// 100 goroutines race to acquire the same 5 slots; none ever call Complete.
	const goroutines = 100
	var wg sync.WaitGroup
	acquired := make(chan struct{}, goroutines)
	for range goroutines {
		wg.Go(func() {
			if d.Enqueue(newTask("t1", 1.0)) == nil {
				acquired <- struct{}{}
			}
		})
	}
	wg.Wait()
	close(acquired)

	assert.Equal(t, limit, len(acquired),
		"exactly limit goroutines must acquire slots; none must slip through under concurrency")
}

func TestConcurrentCompleteAndEnqueue_LimitRespected(t *testing.T) {
	const limit = 5
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: limit})

	// Fill all slots upfront.
	inFlight := make([]*engine.Task, limit)
	for i := range limit {
		task := newTask("t1", 1.0)
		assert.NoError(t, d.Enqueue(task))
		inFlight[i], _ = d.DequeueNonBlocking()
	}
	assert.ErrorIs(t, d.Enqueue(newTask("t1", 1.0)), limiter.ErrLimitExceeded,
		"all slots must be taken before the test begins")

	// Complete all slots and try to re-enqueue concurrently.
	var wg sync.WaitGroup
	var mu sync.Mutex
	newCount := 0

	for _, task := range inFlight {
		wg.Go(func() {
			d.Complete(task)
			if d.Enqueue(newTask("t1", 1.0)) == nil {
				mu.Lock()
				newCount++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	assert.Equal(t, limit, newCount,
		"each completed slot must allow exactly one new enqueue to succeed")
}

// --- RegisterTenant ---

func TestRegisterTenant_SetsWeight(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 4.0, MaxConcurrency: tenant.DefaultConcurrency})

	// Enqueue a task; VFT should reflect the configured weight
	task := newTask("t1", 4.0)
	assert.NoError(t, d.Enqueue(task))
	assert.InDelta(t, 1.0, task.VFT, 0.01, "VFT = cost/weight = 4.0/4.0 = 1.0")
}

func TestRegisterTenant_LimitEnforced(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 2})

	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
	assert.ErrorIs(t, d.Enqueue(newTask("t1", 1.0)), limiter.ErrLimitExceeded,
		"third enqueue must be rejected when limit=2")
}

func TestRegisterTenant_DefaultConcurrency(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	// Registry pre-fills MaxConcurrency=DefaultConcurrency when Create is called with 0.
	// Here we simulate that by passing DefaultConcurrency directly.
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

	// Fill up to the default limit without error
	for range tenant.DefaultConcurrency {
		assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
	}
	// One over the default limit must be rejected
	assert.ErrorIs(t, d.Enqueue(newTask("t1", 1.0)), limiter.ErrLimitExceeded,
		"exceeding DefaultConcurrency must be rejected")
}

func TestRegisterTenant_BurstReducesVFT(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	// BurstRate:0 → no refill, so the full bucket is available for the first task
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency, BurstCapacity: 10.0, BurstRate: 0.0})

	task := newTask("t1", 4.0)
	assert.NoError(t, d.Enqueue(task))
	// Burst absorbed: VFT must be less than the non-burst value (cost/weight = 4.0).
	// Exact reduction ratio is the engine's concern; here we only verify the effect is wired up.
	assert.Less(t, task.VFT, 4.0, "burst should reduce VFT below the non-burst cost/weight")
}

// --- ReEnqueue ---

func TestReEnqueue_TaskReturnsToQueue(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

	task := newTask("t1", 1.0)
	assert.NoError(t, d.Enqueue(task))

	dequeued, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.Equal(t, 0, d.Len())

	assert.NoError(t, d.ReEnqueue(dequeued))
	assert.Equal(t, 1, d.Len(), "task must be back in queue after ReEnqueue")

	redequeued, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	assert.Equal(t, dequeued.TenantID, redequeued.TenantID)
}

func TestReEnqueue_SlotCarriesOver(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	// limit=1: only one slot available for this tenant
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	task := newTask("t1", 1.0)
	assert.NoError(t, d.Enqueue(task))

	dequeued, err := d.DequeueNonBlocking()
	assert.NoError(t, err)

	// ReEnqueue must succeed even though the slot is still held — it carries
	// over the existing slot rather than calling TryAcquire again.
	// If it did TryAcquire, it would hit limit=1 and return ErrLimitExceeded.
	assert.NoError(t, d.ReEnqueue(dequeued), "ReEnqueue must not re-acquire the slot")

	// Slot is still held: a fresh Enqueue must be rejected
	assert.ErrorIs(t, d.Enqueue(newTask("t1", 1.0)), limiter.ErrLimitExceeded,
		"slot must still be held while task is back in queue")

	// Dequeue and complete: slot is released
	task2, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	d.Complete(task2)

	// Slot is now free
	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)),
		"slot must be free after completing the re-enqueued task")
}

func TestReEnqueue_StoppedReleasesSlot(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	task := newTask("t1", 1.0)
	assert.NoError(t, d.Enqueue(task))

	dequeued, err := d.DequeueNonBlocking()
	assert.NoError(t, err)

	d.Stop()

	// ReEnqueue on a stopped dispatcher must return ErrStopped and release the
	// held slot so it is not leaked. We verify the release by spinning up a
	// fresh dispatcher sharing the same limiter and confirming the slot is free.
	err = d.ReEnqueue(dequeued)
	assert.ErrorIs(t, err, ErrStopped)

	// Slot released: a new acquire on the same limiter must succeed.
	assert.NoError(t, d.limiter.TryAcquire("t1"),
		"slot must be released when ReEnqueue is called on a stopped dispatcher")
}

// --- Complete ---

func TestComplete_ReleasesSlot(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	// Fill the single slot
	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))

	// Slot is taken: further enqueue must fail
	assert.ErrorIs(t, d.Enqueue(newTask("t1", 1.0)), limiter.ErrLimitExceeded)

	// Dequeue and complete the task
	task, err := d.DequeueNonBlocking()
	assert.NoError(t, err)
	d.Complete(task)

	// Slot is free: enqueue must succeed
	assert.NoError(t, d.Enqueue(newTask("t1", 1.0)))
}

func TestComplete_DoesNotPanicOnUnknownTenant(t *testing.T) {
	d := NewDispatcher(engine.NewVFTEngine())
	// Complete on a task whose tenant has no registered limit must not panic
	assert.NotPanics(t, func() {
		d.Complete(&engine.Task{TenantID: "ghost"})
	})
}

// --- Benchmarks ---

// BenchmarkEnqueue-4   	  863566	      1378 ns/op	     121 B/op	       1 allocs/op
func BenchmarkEnqueue(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	b.ResetTimer()

	for b.Loop() {
		_ = d.Enqueue(newTask("t1", 1.0))
	}
}

// BenchmarkEnqueue_PreallocatedTasks-4   	  897829	      1342 ns/op	      39 B/op	       0 allocs/op
func BenchmarkEnqueue_PreallocatedTasks(b *testing.B) {
	d := NewDispatcher(engine.NewVFTEngine())
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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
	d.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})

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

func BenchmarkStress_1000Tenants_10KTasks(b *testing.B) {
	const (
		numTenants = 1000
		numTasks   = 10000
	)

	// Pre-compute tenant IDs and tasks to exclude string allocation from measurement
	b.StopTimer()
	tenantIDs := make([]string, numTenants)
	for i := range numTenants {
		tenantIDs[i] = fmt.Sprintf("tenant-%d", i)
	}
	tasks := make([]*engine.Task, numTasks)
	for i := range numTasks {
		tasks[i] = newTask(tenantIDs[i%numTenants], 1.0)
	}

	// Exclude dispatcher + weight setup from measurement
	d := NewDispatcher(engine.NewVFTEngine())
	for i, id := range tenantIDs {
		d.RegisterTenant(&tenant.Tenant{
			ID:             id,
			Weight:         1.0 + float64(i%10), // weights 1.0–10.0
			MaxConcurrency: tenant.DefaultConcurrency,
		})
	}
	b.StartTimer()

	for b.Loop() {
		// Measure: enqueue 10K tasks across 1000 tenants
		for _, task := range tasks {
			_ = d.Enqueue(task)
		}

		// Measure: drain all tasks in VFT order
		for d.Len() > 0 {
			_, _ = d.DequeueNonBlocking()
		}
	}
}
