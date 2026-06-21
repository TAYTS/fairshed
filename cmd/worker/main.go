package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerv1 "github.com/tayts/fairsched/gen/worker/v1"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run() error {
	serverAddr := flag.String("server", "localhost:9090", "fairsched gRPC address")
	workerID := flag.String("id", uuid.New().String(), "worker identifier")
	flag.Parse()

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", *serverAddr, err)
	}
	defer conn.Close() //nolint:errcheck

	client := workerv1.NewWorkerServiceClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker %s starting, connecting to %s", *workerID, *serverAddr)
	err = runPollLoop(ctx, client, *workerID)
	log.Printf("shutting down worker")
	return err
}

// runPollLoop opens a streaming poll to the server and reconnects with
// exponential backoff if the stream drops. Returns when ctx is cancelled.
func runPollLoop(ctx context.Context, client workerv1.WorkerServiceClient, workerID string) error {
	const maxBackoff = 30 * time.Second
	backoff := time.Second

	for {
		err := poll(ctx, client, workerID)
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("stream closed, reconnecting in %s: %v", backoff, err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// poll opens one PollTask stream and processes tasks until the stream closes
// or ctx is cancelled. Returns nil on clean shutdown, error otherwise.
func poll(ctx context.Context, client workerv1.WorkerServiceClient, workerID string) error {
	stream, err := client.PollTask(ctx, &workerv1.PollTaskRequest{WorkerId: workerID})
	if err != nil {
		return err
	}

	log.Printf("worker %s connected, waiting for tasks", workerID)

	for {
		task, err := stream.Recv()
		if err != nil {
			// context cancelled is not an error - just return
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		processTask(ctx, client, workerID, task)
	}
}

func processTask(ctx context.Context, client workerv1.WorkerServiceClient, workerID string, task *workerv1.PollTaskResponse) {
	log.Printf("executing task %s (tenant=%s, type=%s, cost=%.2f)", task.TaskId, task.TenantId, task.TaskType, task.Cost)

	start := time.Now()
	executeTask(ctx, task)
	elapsed := time.Since(start)

	log.Printf("completed task %s (type=%s, cost=%.2f, actual=%.2fs)", task.TaskId, task.TaskType, task.Cost, elapsed.Seconds())

	if _, err := client.CompleteTask(ctx, &workerv1.CompleteTaskRequest{
		TaskId:   task.TaskId,
		WorkerId: workerID,
	}); err != nil {
		log.Printf("CompleteTask %s failed: %v", task.TaskId, err)
	}
}

// executeTask sleeps for task.Cost seconds, simulating holding a concurrency slot.
// The sleep is interrupted if the worker context is cancelled.
func executeTask(ctx context.Context, task *workerv1.PollTaskResponse) {
	d := time.Duration(task.Cost * float64(time.Second))
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
