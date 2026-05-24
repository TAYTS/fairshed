package dispatcher

import (
	"container/heap"
	"context"
	"errors"
	"sync"

	"github.com/tayts/fairsched/internal/burst"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/limiter"
)

var (
	ErrNoTasks = errors.New("no pending tasks")
	ErrStopped = errors.New("dispatcher stopped")
)

type Dispatcher struct {
	vftEngine *engine.VFTEngine
	taskQueue *TaskHeap
	mu        sync.Mutex
	cond      *sync.Cond
	stopped   bool
	limiter   *limiter.ConcurrencyLimiter
}

// NewDispatcher creates a new dispatcher.
func NewDispatcher(vftEngine *engine.VFTEngine) *Dispatcher {
	d := &Dispatcher{
		vftEngine: vftEngine,
		taskQueue: &TaskHeap{},
		limiter:   limiter.NewConcurrencyLimiter(),
	}
	d.cond = sync.NewCond(&d.mu)
	heap.Init(d.taskQueue)
	return d
}

// Enqueue adds a task to the dispatch queue
func (d *Dispatcher) Enqueue(task *engine.Task) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return ErrStopped
	}

	if err := d.limiter.TryAcquire(task.TenantId); err != nil {
		return err
	}

	// compute VFT for current task
	task.VFT = d.vftEngine.ComputeVFT(task)

	heap.Push(d.taskQueue, task)

	// signal waiting dispatcher
	d.cond.Signal()

	return nil
}

// Dequeue retrieves the task with smaller VFT (blocks if empty).
// Callers must call Complete(task) once the task has finished executing.
func (d *Dispatcher) Dequeue(ctx context.Context) (*engine.Task, error) {
	// When ctx is cancelled, broadcast to unpark any cond.Wait() calls.
	stop := context.AfterFunc(ctx, func() { d.cond.Broadcast() })
	defer stop()

	d.mu.Lock()
	defer d.mu.Unlock()

	for {
		if d.stopped {
			return nil, ErrStopped
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if d.taskQueue.Len() > 0 {
			task := heap.Pop(d.taskQueue).(*engine.Task)
			d.vftEngine.OnTaskDispatched(task)
			return task, nil
		}
		d.cond.Wait()
	}
}

// DequeueNonBlocking attempts to dequeue without blocking.
// Callers must call Complete(task) once the task has finished executing.
func (d *Dispatcher) DequeueNonBlocking() (*engine.Task, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return nil, ErrStopped
	}

	if d.taskQueue.Len() == 0 {
		return nil, ErrNoTasks
	}

	task := heap.Pop(d.taskQueue).(*engine.Task)
	d.vftEngine.OnTaskDispatched(task)
	return task, nil
}

// Complete signals that the caller has finished executing the task.
// Complete must be called exactly once per successfully dequeued task.
func (d *Dispatcher) Complete(task *engine.Task) {
	d.limiter.Release(task.TenantId)
}

// Len returns number of pending tasks
func (d *Dispatcher) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.taskQueue.Len()
}

// Stop gracefully stops the dispatcher
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	d.cond.Broadcast()
}

// AddTenant configures a tenant's scheduling parameters.
func (d *Dispatcher) AddTenant(tenantId string, cfg TenantConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.vftEngine.SetTenantWeight(tenantId, orDefault(cfg.Weight, defaultWeight))
	d.limiter.SetLimit(tenantId, orDefault(cfg.Limit, defaultLimit))

	if cfg.BurstCapacity > 0 {
		d.vftEngine.SetTenantBurst(tenantId, burst.NewBurstBucket(cfg.BurstCapacity, orDefault(cfg.BurstRate, defaultBurstRate)))
	}
}
