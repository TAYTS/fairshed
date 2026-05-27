package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tayts/fairsched/internal/engine"
)

// newTask is a test helper to create a minimal engine.Task.
func newTask(id, tenantId string) *engine.Task {
	return &engine.Task{Id: id, TenantId: tenantId, Cost: 1.0}
}

// ---- Create ----------------------------------------------------------------

func TestCreate_Success(t *testing.T) {
	s := NewTaskStore()
	task := newTask("t1", "ash")

	err := s.Create(task)
	assert.NoError(t, err, "expect no error when create new task")

	record, ok := s.records["t1"]
	assert.True(t, ok, "expect record to exist after Create")
	assert.Equal(t, TaskStatusPending, record.Status, "expect status to be pending")
	assert.False(t, record.EnqueuedAt.IsZero(), "expect EnqueuedAt to be set")
}

func TestCreate_Duplicate(t *testing.T) {
	s := NewTaskStore()
	task := newTask("t1", "ash")

	_ = s.Create(task)
	err := s.Create(task)

	assert.ErrorIs(t, err, ErrDuplicateTask, "expect ErrDuplicateTask on duplicate create")
}

// ---- MarkDispatched --------------------------------------------------------

func TestMarkDispatched_Success(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))

	err := s.MarkDispatched("t1", "worker-1")
	assert.NoError(t, err, "expect no error when dispatching a pending task")

	r := s.records["t1"]
	assert.Equal(t, TaskStatusDispatched, r.Status, "expect status to be dispatched")
	assert.Equal(t, "worker-1", r.WorkerId, "expect WorkerId to be set")
	assert.NotNil(t, r.DispatchedAt, "expect DispatchedAt to be set")
}

func TestMarkDispatched_NotFound(t *testing.T) {
	s := NewTaskStore()
	err := s.MarkDispatched("nonexistent", "worker-1")
	assert.ErrorIs(t, err, ErrTaskNotFound, "expect ErrTaskNotFound for unknown task")
}

func TestMarkDispatched_AlreadyDispatched(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")

	err := s.MarkDispatched("t1", "worker-2")
	assert.ErrorIs(t, err, ErrTaskHasDispatched, "expect ErrTaskHasDispatched on double-dispatch")
}

// ---- MarkCompleted ---------------------------------------------------------

func TestMarkCompleted_Success(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")

	err := s.MarkCompleted("t1")
	assert.NoError(t, err, "expect no error when completing a dispatched task")

	r := s.records["t1"]
	assert.Equal(t, TaskStatusCompleted, r.Status, "expect status to be completed")
	assert.NotNil(t, r.CompletedAt, "expect CompletedAt to be set")
}

func TestMarkCompleted_NotFound(t *testing.T) {
	s := NewTaskStore()
	err := s.MarkCompleted("nonexistent")
	assert.ErrorIs(t, err, ErrTaskNotFound, "expect ErrTaskNotFound for unknown task")
}

func TestMarkCompleted_NotDispatched(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash")) // still pending

	err := s.MarkCompleted("t1")
	assert.ErrorIs(t, err, ErrTaskNotDispatched, "expect ErrTaskNotDispatched when task is still pending")
}

func TestMarkCompleted_AlreadyCompleted(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkCompleted("t1")

	err := s.MarkCompleted("t1")
	assert.ErrorIs(t, err, ErrTaskAlreadyTerminated, "expect ErrTaskAlreadyTerminated on double-complete")
}

func TestMarkCompleted_AlreadyFailed(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkFailed("t1", "timeout")

	err := s.MarkCompleted("t1")
	assert.ErrorIs(t, err, ErrTaskAlreadyTerminated, "expect ErrTaskAlreadyTerminated when completing an already-failed task")
}

// ---- MarkFailed ------------------------------------------------------------

func TestMarkFailed_Success(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")

	err := s.MarkFailed("t1", "timeout")
	assert.NoError(t, err, "expect no error when failing a dispatched task")

	r := s.records["t1"]
	assert.Equal(t, TaskStatusFailed, r.Status, "expect status to be failed")
	assert.Equal(t, "timeout", r.Error, "expect error message to be set")
	assert.NotNil(t, r.CompletedAt, "expect CompletedAt to be set")
}

func TestMarkFailed_NotFound(t *testing.T) {
	s := NewTaskStore()
	err := s.MarkFailed("nonexistent", "some error")
	assert.ErrorIs(t, err, ErrTaskNotFound, "expect ErrTaskNotFound for unknown task")
}

func TestMarkFailed_NotDispatched(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash")) // still pending

	err := s.MarkFailed("t1", "some error")
	assert.ErrorIs(t, err, ErrTaskNotDispatched, "expect ErrTaskNotDispatched when task is still pending")
}

func TestMarkFailed_AlreadyCompleted(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkCompleted("t1")

	err := s.MarkFailed("t1", "late failure")
	assert.ErrorIs(t, err, ErrTaskAlreadyTerminated, "expect ErrTaskAlreadyTerminated when failing an already-completed task")
}

func TestMarkFailed_AlreadyFailed(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkFailed("t1", "first error")

	err := s.MarkFailed("t1", "second error")
	assert.ErrorIs(t, err, ErrTaskAlreadyTerminated, "expect ErrTaskAlreadyTerminated on double-fail")
}

// ---- GetTask ---------------------------------------------------------------

func TestGetTask_Success(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))

	record, err := s.GetTask("t1")
	assert.NoError(t, err, "expect no error when getting an existing task")
	assert.Equal(t, "t1", record.Task.Id, "expect task id to match")
	assert.Equal(t, TaskStatusPending, record.Status, "expect status to be pending")
}

func TestGetTask_NotFound(t *testing.T) {
	s := NewTaskStore()
	_, err := s.GetTask("nonexistent")
	assert.ErrorIs(t, err, ErrTaskNotFound, "expect ErrTaskNotFound for unknown task")
}

// ---- GetWorkerDurationByTenant ---------------------------------------------

func TestGetWorkerDurationByTenant_Empty(t *testing.T) {
	s := NewTaskStore()
	result := s.GetWorkerDurationByTenant()
	assert.Empty(t, result, "expect empty map when no tasks exist")
}

func TestGetWorkerDurationByTenant_IgnoresPendingAndDispatched(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.Create(newTask("t2", "ash"))
	_ = s.MarkDispatched("t2", "worker-1")
	// t1 is pending, t2 is dispatched — neither should contribute

	result := s.GetWorkerDurationByTenant()
	assert.Empty(t, result, "expect empty map when no tasks are completed or failed")
}

func TestGetWorkerDurationByTenant_AccumulatesCompletedByTenant(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.Create(newTask("t2", "ash"))
	_ = s.Create(newTask("t3", "misty"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkDispatched("t2", "worker-2")
	_ = s.MarkDispatched("t3", "worker-1")
	_ = s.MarkCompleted("t1")
	_ = s.MarkCompleted("t2")
	_ = s.MarkCompleted("t3")

	result := s.GetWorkerDurationByTenant()

	assert.Contains(t, result, "ash", "expect entry for tenant 'ash'")
	assert.Contains(t, result, "misty", "expect entry for tenant 'misty'")
	// two tasks from "ash" — duration must be positive
	assert.Positive(t, result["ash"], "expect positive duration for 'ash'")
	assert.Positive(t, result["misty"], "expect positive duration for 'misty'")
}

func TestGetWorkerDurationByTenant_IncludesFailedTasks(t *testing.T) {
	s := NewTaskStore()
	_ = s.Create(newTask("t1", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkFailed("t1", "timeout")

	result := s.GetWorkerDurationByTenant()
	assert.Contains(t, result, "ash", "expect failed task to contribute to tenant duration")
	assert.Positive(t, result["ash"], "expect positive duration for 'ash'")
}

func TestGetWorkerDurationByTenant_KeyedByTenantNotWorker(t *testing.T) {
	s := NewTaskStore()
	// Two tasks for the same tenant, dispatched by different workers
	_ = s.Create(newTask("t1", "ash"))
	_ = s.Create(newTask("t2", "ash"))
	_ = s.MarkDispatched("t1", "worker-1")
	_ = s.MarkDispatched("t2", "worker-2")
	_ = s.MarkCompleted("t1")
	_ = s.MarkCompleted("t2")

	result := s.GetWorkerDurationByTenant()

	// Must have exactly one key ("ash"), not two ("worker-1", "worker-2")
	assert.Len(t, result, 1, "expect exactly 1 tenant entry, not one per worker")
	assert.Contains(t, result, "ash", "expect key to be tenant id 'ash', not worker id")
}

// ---- Concurrency -----------------------------------------------------------

func TestTaskStore_ConcurrentCreates(t *testing.T) {
	s := NewTaskStore()
	const n = 100

	var wg sync.WaitGroup
	errs := make([]error, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "t1" // same ID — only one should succeed
			errs[i] = s.Create(newTask(id, "ash"))
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		} else {
			assert.ErrorIs(t, err, ErrDuplicateTask, "expect only ErrDuplicateTask errors from concurrent creates")
		}
	}
	assert.Equal(t, 1, successCount, "expect exactly 1 successful Create")
}
