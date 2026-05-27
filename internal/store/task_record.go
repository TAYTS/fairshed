package store

import (
	"time"

	"github.com/tayts/fairsched/internal/engine"
)

// TaskStatus represents the status of a task.
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusDispatched TaskStatus = "dispatched"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
)

// TaskRecord represents a single row/record inside the database.
type TaskRecord struct {
	Task         *engine.Task
	Status       TaskStatus
	EnqueuedAt   time.Time
	DispatchedAt *time.Time
	CompletedAt  *time.Time
	WorkerId     string
	Error        string
}
