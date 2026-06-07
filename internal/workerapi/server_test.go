package workerapi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	workerv1 "github.com/tayts/fairsched/gen/worker/v1"
	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/limiter"
	"github.com/tayts/fairsched/internal/store"
	"github.com/tayts/fairsched/internal/tenant"
)

// mockStream is a test double for grpc.ServerStreamingServer[workerv1.PollTaskResponse].
//
//   - Set cancelFn to cancel the stream context after the first successful Send
//     (idiomatic way to stop PollTask after asserting one delivery).
//   - Set sendErr to make every Send return that error.
//   - Set onSend to run a side-effect just before Send returns sendErr (only
//     fired on the error path); useful for stopping the dispatcher so the
//     subsequent ReEnqueue call also fails.
type mockStream struct {
	ctx      context.Context
	cancelFn context.CancelFunc // called after first successful Send; nil = no-op
	onSend   func()             // called just before returning sendErr; nil = no-op

	mu      sync.Mutex
	sent    []*workerv1.PollTaskResponse
	sendErr error
}

var _ grpc.ServerStreamingServer[workerv1.PollTaskResponse] = (*mockStream)(nil)

func (m *mockStream) Send(resp *workerv1.PollTaskResponse) error {
	if m.sendErr != nil {
		if m.onSend != nil {
			m.onSend()
		}
		return m.sendErr
	}
	m.mu.Lock()
	m.sent = append(m.sent, resp)
	m.mu.Unlock()
	if m.cancelFn != nil {
		m.cancelFn()
	}
	return nil
}

func (m *mockStream) Context() context.Context     { return m.ctx }
func (m *mockStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockStream) SendHeader(metadata.MD) error { return nil }
func (m *mockStream) SetTrailer(metadata.MD)       {}
func (m *mockStream) SendMsg(any) error            { return nil }
func (m *mockStream) RecvMsg(any) error            { return nil }

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

func TestPollTask_DequeuesAndSendsTask(t *testing.T) {
	srv, disp, s := newTestServer(t)

	enqueueTask(t, disp, s, &engine.Task{
		ID:       "task-1",
		TenantID: "t1",
		Cost:     2.5,
		Metadata: map[string]any{"type": "snorlax", "payload": "pokeball"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	stream := &mockStream{ctx: ctx, cancelFn: cancel}

	err := srv.PollTask(&workerv1.PollTaskRequest{WorkerId: "w1"}, stream)

	// After the first successful send, cancelFn fires. Dequeue returns
	// context.Canceled on the next iteration.
	assert.ErrorIs(t, err, context.Canceled)
	require.Len(t, stream.sent, 1, "exactly one task should be sent before context is cancelled")
	resp := stream.sent[0]
	assert.Equal(t, "task-1", resp.TaskId)
	assert.Equal(t, "t1", resp.TenantId)
	assert.Equal(t, 2.5, resp.Cost)
	assert.Equal(t, "snorlax", resp.TaskType)
	assert.Equal(t, "pokeball", resp.Payload)
}

func TestPollTask_ContextCancelledStopsLoop(t *testing.T) {
	srv, _, _ := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	stream := &mockStream{ctx: ctx}

	var pollErr error
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Go(func() {
		defer wg.Done()
		pollErr = srv.PollTask(&workerv1.PollTaskRequest{WorkerId: "w1"}, stream)
	})

	time.Sleep(20 * time.Millisecond) // let the goroutine block on Dequeue
	cancel()
	wg.Wait()

	assert.ErrorIs(t, pollErr, context.Canceled)
	assert.Empty(t, stream.sent, "no tasks should be sent when context is cancelled before any task arrives")
}

func TestPollTask_MarkDispatchedFails_SkipsAndReleasesSlot(t *testing.T) {
	srv, disp, s := newTestServer(t)
	// Use limit=1 so we can verify the slot is released after the skip.
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})

	// Pre-dispatch so that MarkDispatched inside PollTask will fail with
	// ErrTaskHasDispatched — simulating a race where the store is ahead of
	// the dispatcher.
	require.NoError(t, s.MarkDispatched("task-1", "other-worker"))

	ctx, cancel := context.WithCancel(context.Background())
	stream := &mockStream{ctx: ctx}

	var pollErr error
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Go(func() {
		defer wg.Done()
		pollErr = srv.PollTask(&workerv1.PollTaskRequest{WorkerId: "w1"}, stream)
	})

	// Give the goroutine time to dequeue, fail MarkDispatched, call Complete,
	// and block on the next Dequeue call (queue is now empty).
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.ErrorIs(t, pollErr, context.Canceled)
	assert.Empty(t, stream.sent, "no task should be streamed when MarkDispatched fails")

	// The skipped task called Complete, which released the slot.
	// With limit=1, a fresh Enqueue must now succeed.
	newTask := &engine.Task{ID: "task-2", TenantID: "t1", Cost: 1.0}
	require.NoError(t, s.Create(newTask))
	assert.NoError(t, disp.Enqueue(newTask), "slot must be released after the skipped task")
}

func TestPollTask_SendFails_ReEnqueuesTask(t *testing.T) {
	srv, disp, s := newTestServer(t)
	// Use limit=1 to verify slot accounting: the slot must stay held while the
	// task sits back in the queue after the failed send.
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})

	sendErr := errors.New("connection reset by peer")
	stream := &mockStream{ctx: context.Background(), sendErr: sendErr}

	err := srv.PollTask(&workerv1.PollTaskRequest{WorkerId: "w1"}, stream)

	assert.ErrorIs(t, err, sendErr, "the original send error must be propagated")
	assert.Equal(t, 1, disp.Len(), "task must be back in the queue after the failed send")

	// The slot is still held (carried over by ReEnqueue).
	// A fresh Enqueue must be rejected because the limit is already at capacity.
	newTask := &engine.Task{ID: "task-2", TenantID: "t1", Cost: 1.0}
	require.NoError(t, s.Create(newTask))
	assert.ErrorIs(t, disp.Enqueue(newTask), limiter.ErrLimitExceeded,
		"slot must remain held while the re-enqueued task is in the queue")
}

func TestPollTask_SendFails_ReEnqueueAlsoFails_TaskLost(t *testing.T) {
	srv, disp, s := newTestServer(t)
	// limit=1 so we can observe slot accounting afterwards.
	disp.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, MaxConcurrency: 1})

	enqueueTask(t, disp, s, &engine.Task{ID: "task-1", TenantID: "t1", Cost: 1.0})

	sendErr := errors.New("connection reset by peer")
	stream := &mockStream{
		ctx:     context.Background(),
		sendErr: sendErr,
		// Stop the dispatcher inside Send so that the immediately following
		// ReEnqueue call sees a stopped dispatcher and returns ErrStopped.
		onSend: func() { disp.Stop() },
	}

	err := srv.PollTask(&workerv1.PollTaskRequest{WorkerId: "w1"}, stream)

	// PollTask must surface both errors via errors.Join.
	assert.ErrorIs(t, err, sendErr, "original send error must be present")
	assert.ErrorIs(t, err, dispatcher.ErrStopped, "re-enqueue error must be present")

	// ReEnqueue releases the slot when the dispatcher is stopped, so the queue
	// is empty and the slot is no longer held.
	assert.Equal(t, 0, disp.Len(), "task must NOT be in the queue (re-enqueue failed)")
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
