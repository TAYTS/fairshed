package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tayts/fairsched/internal/api"
	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/store"
	"github.com/tayts/fairsched/internal/tenant"
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
	)

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
		// Wait until a signal fires or another goroutine fails.
		<-gCtx.Done()
		log.Println("shutting down")

		disp.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := apiServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	})

	return g.Wait()
}
