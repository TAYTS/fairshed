package workerapi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerv1 "github.com/tayts/fairsched/gen/worker/v1"
	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/store"
	"github.com/tayts/fairsched/internal/tenant"
)

// newTestServer creates a fresh WorkerServer wired to a real Dispatcher
// and a real TaskStore. Tenant "t1" is registered with DefaultConcurrency.
func newTestServer(t *testing.T) (*WorkerServer, *dispatcher.Dispatcher, *store.TaskStore) {
	t.Helper()
	eng := engine.NewVFTEngine()
	disp := dispatcher.NewDispatcher(eng)
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: tenant.DefaultConcurrency})
	s := store.NewTaskStore()
	return NewWorkerServer(disp, s), disp, s
}

// enqueueTask creates a task record in the store and enqueues it in the dispatcher.
func enqueueTask(t *testing.T, disp *dispatcher.Dispatcher, s *store.TaskStore, task *engine.Task) {
	t.Helper()
	require.NoError(t, s.Create(task))
	require.NoError(t, disp.Enqueue(task))
}

// setupDispatchedTask creates a task, enqueues it, dequeues it via
// DequeueNonBlocking, and marks it as dispatched in the store.
// It returns the dequeued task so callers can pass it to CompleteTask / FailTask.
func setupDispatchedTask(t *testing.T, disp *dispatcher.Dispatcher, s *store.TaskStore, workerID string) *engine.Task {
	t.Helper()
	task := &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0}
	enqueueTask(t, disp, s, task)
	dequeued, err := disp.DequeueNonBlocking()
	require.NoError(t, err)
	require.NoError(t, s.MarkDispatched(dequeued.ID, workerID))
	return dequeued
}

// ---------------------------------------------------------------------------
// PollTask
// ---------------------------------------------------------------------------

func TestPollTask_DequeuesAndReturnsTask(t *testing.T) {
	srv, disp, s := newTestServer(t)

	enqueueTask(t, disp, s, &engine.Task{
		ID:       "task-1",
		TenantID: "t1",
		Cost:     2.5,
		Metadata: map[string]any{"type": "snorlax", "payload": "pokeball"},
	})

	resp, err := srv.PollTask(context.Background(), &workerv1.PollTaskRequest{WorkerId: "w1"})

	require.NoError(t, err)
	assert.Equal(t, "task-1", resp.TaskId)
	assert.Equal(t, "t1", resp.TenantId)
	assert.Equal(t, 2.5, resp.Cost)
	assert.Equal(t, "snorlax", resp.TaskType)
	assert.Equal(t, "pokeball", resp.Payload)
}

func TestPollTask_BlocksUntilTaskAvailable(t *testing.T) {
	srv, disp, s := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())

	var (
		resp    *workerv1.PollTaskResponse
		pollErr error
		wg      sync.WaitGroup
	)
	wg.Go(func() {
		resp, pollErr = srv.PollTask(ctx, &workerv1.PollTaskRequest{WorkerId: "w1"})
	})

	time.Sleep(20 * time.Millisecond) // confirm it's blocking with empty queue
	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})
	wg.Wait()
	cancel()

	require.NoError(t, pollErr)
	assert.Equal(t, "task-1", resp.TaskId)
}

func TestPollTask_ContextCancelledWhileBlocking(t *testing.T) {
	srv, _, _ := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())

	var pollErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		_, pollErr = srv.PollTask(ctx, &workerv1.PollTaskRequest{WorkerId: "w1"})
	})

	time.Sleep(20 * time.Millisecond) // let the goroutine block on Dequeue
	cancel()
	wg.Wait()

	// Parent context cancelled -> Dequeue returns context.Canceled, which the
	// server wraps as Unavailable (real failure, worker should back off).
	assert.Equal(t, codes.Unavailable, status.Code(pollErr))
}

func TestPollTask_PollTimeoutReturnsNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	srv.pollTimeout = 30 * time.Millisecond // no queue, window expires quickly

	_, err := srv.PollTask(context.Background(), &workerv1.PollTaskRequest{WorkerId: "w1"})

	// Poll window elapsed with no task → NOT_FOUND (worker retries immediately).
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPollTask_MarkDispatchedFails_ReleasesSlotAndReturnsError(t *testing.T) {
	srv, disp, s := newTestServer(t)
	// Use limit=1 so we can verify the slot is released after the failure.
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})

	// Pre-dispatch so that MarkDispatched inside PollTask fails with
	// ErrTaskHasDispatched — simulates a race where the store is ahead of the dispatcher.
	require.NoError(t, s.MarkDispatched("task-1", "other-worker"))

	_, err := srv.PollTask(context.Background(), &workerv1.PollTaskRequest{WorkerId: "w1"})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))

	// The failed task called Complete, which released the slot.
	// With limit=1, a fresh Enqueue must now succeed.
	newTask := &engine.Task{ID: "task-2", TenantID: "t1", Cost: 1.0}
	require.NoError(t, s.Create(newTask))
	assert.NoError(t, disp.Enqueue(newTask), "slot must be released after MarkDispatched failure")
}

// ---------------------------------------------------------------------------
// CompleteTask
// ---------------------------------------------------------------------------

func TestCompleteTask_Success(t *testing.T) {
	srv, disp, s := newTestServer(t)
	// Use limit=1 so we can verify that the slot is released after the call.
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	task := setupDispatchedTask(t, disp, s, "w1")

	resp, err := srv.CompleteTask(context.Background(), &workerv1.CompleteTaskRequest{
		TaskId: task.ID, WorkerId: "w1",
	})

	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Store must reflect the completed status.
	record, err := s.GetTask(task.ID)
	require.NoError(t, err)
	assert.Equal(t, store.TaskStatusCompleted, record.Status)
	assert.NotNil(t, record.CompletedAt)

	// The concurrency slot must be released: a new Enqueue must succeed.
	newTask := &engine.Task{ID: "task-2", TenantID: "t1", Cost: 1.0}
	require.NoError(t, s.Create(newTask))
	assert.NoError(t, disp.Enqueue(newTask), "slot must be released after CompleteTask")
}

func TestCompleteTask_TaskNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)

	_, err := srv.CompleteTask(context.Background(), &workerv1.CompleteTaskRequest{
		TaskId: "does-not-exist",
	})

	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestCompleteTask_InvalidState_NotDispatched(t *testing.T) {
	srv, disp, s := newTestServer(t)

	// Task is in store (pending) but has never been dequeued/dispatched.
	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})

	_, err := srv.CompleteTask(context.Background(), &workerv1.CompleteTaskRequest{
		TaskId: "task-1",
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"completing a pending task must be rejected as invalid state")
}

// ---------------------------------------------------------------------------
// FailTask
// ---------------------------------------------------------------------------

func TestFailTask_Success(t *testing.T) {
	srv, disp, s := newTestServer(t)
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	task := setupDispatchedTask(t, disp, s, "w1")

	resp, err := srv.FailTask(context.Background(), &workerv1.FailTaskRequest{
		TaskId: task.ID, WorkerId: "w1", Error: "out of memory",
	})

	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Store must reflect the failed status with the error message.
	record, err := s.GetTask(task.ID)
	require.NoError(t, err)
	assert.Equal(t, store.TaskStatusFailed, record.Status)
	assert.Equal(t, "out of memory", record.Error)
	assert.NotNil(t, record.CompletedAt)

	// The concurrency slot must be released: a new Enqueue must succeed.
	newTask := &engine.Task{ID: "task-2", TenantID: "t1", Cost: 1.0}
	require.NoError(t, s.Create(newTask))
	assert.NoError(t, disp.Enqueue(newTask), "slot must be released after FailTask")
}

func TestFailTask_TaskNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)

	_, err := srv.FailTask(context.Background(), &workerv1.FailTaskRequest{
		TaskId: "does-not-exist", Error: "timeout",
	})

	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestFailTask_InvalidState_NotDispatched(t *testing.T) {
	srv, disp, s := newTestServer(t)

	// Task is in store (pending) but has never been dequeued/dispatched.
	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})

	_, err := srv.FailTask(context.Background(), &workerv1.FailTaskRequest{
		TaskId: "task-1", Error: "crashed",
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"failing a pending task must be rejected as invalid state")
}
