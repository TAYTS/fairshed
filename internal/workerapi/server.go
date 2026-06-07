package workerapi

import (
	"context"
	"errors"
	"log"

	workerv1 "github.com/tayts/fairsched/gen/worker/v1"
	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WorkerServer implements the gRPC WorkerService.
type WorkerServer struct {
	workerv1.UnimplementedWorkerServiceServer
	dispatcher *dispatcher.Dispatcher
	store      *store.TaskStore
}

// NewWorkerServer creates a WorkerServer.
func NewWorkerServer(dispatcher *dispatcher.Dispatcher, store *store.TaskStore) *WorkerServer {
	return &WorkerServer{
		dispatcher: dispatcher,
		store:      store,
	}
}

// PollTask implements [workerv1.WorkerServiceServer].
func (s *WorkerServer) PollTask(req *workerv1.PollTaskRequest, stream grpc.ServerStreamingServer[workerv1.PollTaskResponse]) error {
	ctx := stream.Context()

	for {
		// wait until task available
		task, err := s.dispatcher.Dequeue(ctx)
		if err != nil {
			// ctx cancelled -> worker disconnected
			return err
		}

		if err := s.store.MarkDispatched(task.ID, req.WorkerId); err != nil {
			// log and mark the task completed as it is probably due to malformed data
			log.Printf("failed to dispatch task %s: %v", task.ID, err)
			s.dispatcher.Complete(task)
			continue
		}

		taskType, _ := task.Metadata["type"].(string)
		payload, _ := task.Metadata["payload"].(string)

		resp := &workerv1.PollTaskResponse{
			TaskId:   task.ID,
			TenantId: task.TenantID,
			TaskType: taskType,
			Cost:     task.Cost,
			Payload:  payload,
		}

		if err := stream.Send(resp); err != nil {
			// send returned an error, try to re-enqueue the task.
			if requeueErr := s.dispatcher.ReEnqueue(task); requeueErr != nil {
				// task is lost — log prominently before returning
				log.Printf("task %s lost after send failure: send=%v re-enqueue=%v", task.ID, err, requeueErr)
				return errors.Join(err, requeueErr)
			}
			// send failed, but we put the task back in the queue. Return the original send error.
			return err
		}

		log.Printf("Dispatched task %s to worker %s", task.ID, req.WorkerId)
	}
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
