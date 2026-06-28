package workerapi

import (
	"context"
	"errors"
	"log"
	"time"

	workerv1 "github.com/tayts/fairsched/gen/worker/v1"
	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const pollTimeout = 30 * time.Second

// WorkerServer implements the gRPC WorkerService.
type WorkerServer struct {
	workerv1.UnimplementedWorkerServiceServer
	dispatcher  *dispatcher.Dispatcher
	store       *store.TaskStore
	pollTimeout time.Duration
}

// NewWorkerServer creates a WorkerServer.
func NewWorkerServer(dispatcher *dispatcher.Dispatcher, store *store.TaskStore) *WorkerServer {
	return &WorkerServer{
		dispatcher:  dispatcher,
		store:       store,
		pollTimeout: pollTimeout,
	}
}

// PollTask implements [workerv1.WorkerServiceServer].
// Long-polls for up to pollTimeout. Returns NOT_FOUND if no task arrives within
// the window (worker may retry immediately). Any other error indicates a real
// failure and the worker should back off.
func (s *WorkerServer) PollTask(ctx context.Context, req *workerv1.PollTaskRequest) (*workerv1.PollTaskResponse, error) {
	pollCtx, cancel := context.WithTimeout(ctx, s.pollTimeout)
	defer cancel()

	task, err := s.dispatcher.Dequeue(pollCtx)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, status.Error(codes.NotFound, "no task available")
	}
	if err != nil {
		log.Printf("failed to dequeue task for worker %s: %v", req.WorkerId, err)
		return nil, status.Errorf(codes.Unavailable, "dequeue failed: %v", err)
	}

	if err := s.store.MarkDispatched(task.ID, req.WorkerId); err != nil {
		// log and mark the task completed as it is probably due to malformed data
		log.Printf("failed to dispatch task %s: %v", task.ID, err)
		s.dispatcher.Complete(task)
		return nil, status.Errorf(codes.Internal, "dispatch failed: %v", err)
	}

	taskType, _ := task.Metadata["type"].(string)
	payload, _ := task.Metadata["payload"].(string)

	log.Printf("dispatched task %s to worker %s", task.ID, req.WorkerId)
	return &workerv1.PollTaskResponse{
		TaskId:   task.ID,
		TenantId: task.TenantID,
		TaskType: taskType,
		Cost:     task.Cost,
		Payload:  payload,
	}, nil
}

// CompleteTask implements [workerv1.WorkerServiceServer].
func (s *WorkerServer) CompleteTask(ctx context.Context, req *workerv1.CompleteTaskRequest) (*workerv1.CompleteTaskResponse, error) {
	record, err := s.store.GetTask(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
	}

	if err := s.store.MarkCompleted(req.TaskId); err != nil {
		log.Printf("failed to complete task %s: %v", req.TaskId, err)
		return nil, status.Errorf(codes.InvalidArgument, "task %s has invalid state: %v", req.TaskId, err)
	}
	s.dispatcher.Complete(record.Task)
	return &workerv1.CompleteTaskResponse{}, nil
}

// FailTask implements [workerv1.WorkerServiceServer].
func (s *WorkerServer) FailTask(ctx context.Context, req *workerv1.FailTaskRequest) (*workerv1.FailTaskResponse, error) {
	record, err := s.store.GetTask(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
	}

	if err := s.store.MarkFailed(req.TaskId, req.Error); err != nil {
		log.Printf("failed to fail task %s: %v", req.TaskId, err)
		return nil, status.Errorf(codes.InvalidArgument, "task %s has invalid state: %v", req.TaskId, err)
	}
	s.dispatcher.Complete(record.Task)
	return &workerv1.FailTaskResponse{}, nil
}
