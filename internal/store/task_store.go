package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tayts/fairsched/internal/engine"
)

var (
	ErrDuplicateTask         = errors.New("duplicate task")
	ErrTaskNotFound          = errors.New("task not found")
	ErrTaskHasDispatched     = errors.New("task has already been dispatched")
	ErrTaskNotDispatched     = errors.New("task has not been dispatched")
	ErrTaskAlreadyTerminated = errors.New("task has already terminated")
)

// TaskStore is in-memory database for testing.
type TaskStore struct {
	mu      sync.RWMutex
	records map[string]*TaskRecord // taskId -> record
}

// NewTaskStore creates an in-memory task store.
func NewTaskStore() *TaskStore {
	return &TaskStore{
		records: make(map[string]*TaskRecord),
	}
}

// GetTask retrieves task by task id.
// Returns error if task is not found
func (s *TaskStore) GetTask(taskID string) (*TaskRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.records[taskID]
	if !ok {
		return nil, fmt.Errorf("task %s: %w", taskID, ErrTaskNotFound)
	}
	return record, nil
}

// Create creates a new task record.
// Returns error if task already exists
func (s *TaskStore) Create(task *engine.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.records[task.ID]; ok {
		return fmt.Errorf("task %s: %w", task.ID, ErrDuplicateTask)
	}

	now := time.Now()
	record := &TaskRecord{
		Task:       task,
		Status:     TaskStatusPending,
		EnqueuedAt: now,
	}
	s.records[task.ID] = record
	return nil
}

// MarkDispatched marks a task as dispatched.
// Return error if task not found or task has dispatched.
func (s *TaskStore) MarkDispatched(taskID, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[taskID]
	if !ok {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskNotFound)
	}
	if record.DispatchedAt != nil || record.WorkerID != "" || record.Status != TaskStatusPending {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskHasDispatched)
	}

	now := time.Now()
	record.Status = TaskStatusDispatched
	record.DispatchedAt = &now
	record.WorkerID = workerID
	return nil
}

// MarkCompleted marks a dispatched task as completed.
// Returns error if task not found or task has not dispatched.
func (s *TaskStore) MarkCompleted(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[taskID]
	if !ok {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskNotFound)
	}
	if record.Status == TaskStatusCompleted || record.Status == TaskStatusFailed {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskAlreadyTerminated)
	}
	if record.Status == TaskStatusPending {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskNotDispatched)
	}

	now := time.Now()
	record.Status = TaskStatusCompleted
	record.CompletedAt = &now
	return nil
}

// MarkFailed marks a dispatched task as failed.
// Returns error if task not found or task has not dispatched.
func (s *TaskStore) MarkFailed(taskID, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[taskID]
	if !ok {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskNotFound)
	}
	if record.Status == TaskStatusCompleted || record.Status == TaskStatusFailed {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskAlreadyTerminated)
	}
	if record.Status == TaskStatusPending {
		return fmt.Errorf("task %s: %w", taskID, ErrTaskNotDispatched)
	}

	now := time.Now()
	record.Status = TaskStatusFailed
	record.CompletedAt = &now
	record.Error = errMsg
	return nil
}

// GetWorkerDurationByTenant returns the total worker duration consumed per tenant for
// tasks that have completed or failed.
func (s *TaskStore) GetWorkerDurationByTenant() map[string]time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workerDurations := make(map[string]time.Duration)
	for _, record := range s.records {
		if record.Status == TaskStatusCompleted || record.Status == TaskStatusFailed {
			workerDurations[record.Task.TenantID] += record.CompletedAt.Sub(*record.DispatchedAt)
		}
	}
	return workerDurations
}
