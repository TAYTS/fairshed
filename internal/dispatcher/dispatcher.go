package dispatcher

import (
	"container/heap"
	"context"
	"errors"
	"sync"

	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/limiter"
	"github.com/tayts/fairsched/internal/tenant"
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

// Enqueue adds a task to the dispatch queue.
// Returns ErrStopped if the dispatcher has been stopped,
// ErrLimitExceeded if the tenant's concurrency limit is reached,
// ErrTenantNotRegistered if the tenant has not been configured via RegisterTenant.
func (d *Dispatcher) Enqueue(task *engine.Task) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return ErrStopped
	}

	if err := d.limiter.TryAcquire(task.TenantID); err != nil {
		return err
	}

	if err := d.push(task); err != nil {
		d.limiter.Release(task.TenantID) // roll back the slot we just acquired
		return err
	}

	return nil
}

// ReEnqueue puts a dequeued task back into the queue after a failed dispatch.
// Unlike Enqueue, it does not acquire a new concurrency slot — the slot from
// the original Enqueue is still held and carries over.
// ReEnqueue must be called at most once per dequeued task.
func (d *Dispatcher) ReEnqueue(task *engine.Task) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		d.limiter.Release(task.TenantID) // can't re-enqueue; release the slot to avoid leaking it
		return ErrStopped
	}

	if err := d.push(task); err != nil {
		d.limiter.Release(task.TenantID) // roll back the slot acquired in Enqueue
		return err
	}

	return nil
}

// push computes a fresh VFT for the task and pushes it onto the heap.
// Caller must hold the lock.
func (d *Dispatcher) push(task *engine.Task) error {
	// Compute VFT for current task. Returns ErrTenantNotRegistered if the
	// tenant has not been configured via RegisterTenant.
	vft, err := d.vftEngine.ComputeVFT(task)
	if err != nil {
		return err
	}
	task.VFT = vft

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
	d.limiter.Release(task.TenantID)
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

// RegisterTenant configures a tenant's scheduling parameters.
func (d *Dispatcher) RegisterTenant(t *tenant.Tenant) {
	d.vftEngine.RegisterTenant(t)
	d.limiter.SetLimit(t.ID, t.MaxConcurrency)
}

// Stats returns a snapshot of the engine's scheduling state.
func (d *Dispatcher) Stats() engine.EngineStats {
	return d.vftEngine.Stats()
}
