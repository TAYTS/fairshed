package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/engine"
)

func main() {
	eng := engine.NewVFTEngine()
	disp := dispatcher.NewDispatcher(eng)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/tasks", submitTask(disp))
	mux.HandleFunc("GET /v1/stats", getStats(disp))

	svr := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		if err := svr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("shutting down")

	// stop dispatcher
	disp.Stop()

	// shutdown server with bounded time for in-flight requests to complete
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := svr.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("server exited")
}

func submitTask(disp *dispatcher.Dispatcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TenantId string  `json:"tenant_id"`
			Cost     float64 `json:"costd"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		req.TenantId = strings.TrimSpace(req.TenantId)

		if req.TenantId == "" {
			http.Error(w, "tenant_id is required", http.StatusBadRequest)
			return
		}

		if req.Cost <= 0 {
			http.Error(w, "cost must be positive", http.StatusBadRequest)
			return
		}

		task := &engine.Task{
			TenantID: req.TenantId,
			Cost:     req.Cost,
		}

		if err := disp.Enqueue(task); err != nil {
			http.Error(w, fmt.Sprintf("internal error: failed to submit task - %s", err.Error()), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "queued"})
	}
}

func getStats(disp *dispatcher.Dispatcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queueLen := disp.Len()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"queued_tasks": queueLen,
		})
	}
}
