package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	workerv1 "github.com/tayts/fairsched/gen/worker/v1"
	"github.com/tayts/fairsched/internal/api"
	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/store"
	"github.com/tayts/fairsched/internal/tenant"
	"github.com/tayts/fairsched/internal/workerapi"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run() error {
	// --- Dependencies ---
	registry := tenant.NewRegistry()
	taskStore := store.NewTaskStore()
	eng := engine.NewVFTEngine()
	disp := dispatcher.NewDispatcher(eng)

	// --- HTTP server (admin + submit API on :8080) ---
	apiServer := api.NewServer(8080)
	apiServer.RegisterRoutes(
		api.NewTaskHandler(disp, taskStore, registry),
		api.NewTenantHandler(disp, registry),
		api.NewStatsHandler(disp, taskStore),
	)

	// --- gRPC server (worker streaming API on :9090) ---
	grpcServer := grpc.NewServer()
	workerv1.RegisterWorkerServiceServer(grpcServer, workerapi.NewWorkerServer(disp, taskStore))

	grpcListener, err := net.Listen("tcp", ":9090")
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	// signal.NotifyContext cancels ctx on SIGINT / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// errgroup propagates ctx cancellation across all goroutines.
	// If any goroutine returns a non-nil error, the group context is cancelled
	// and the shutdown goroutine triggers graceful teardown.
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return apiServer.Listen()
	})

	g.Go(func() error {
		log.Println("gRPC worker API listening on :9090")
		if err := grpcServer.Serve(grpcListener); err != nil {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		// Wait until a signal fires or another goroutine fails.
		<-gCtx.Done()
		log.Println("shutting down")

		disp.Stop()

		// GracefulStop drains in-flight RPCs before closing.
		grpcServer.GracefulStop()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := apiServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	})

	return g.Wait()
}
