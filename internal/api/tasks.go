package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/engine"
	"github.com/tayts/fairsched/internal/store"
	"github.com/tayts/fairsched/internal/tenant"
)

type TaskHandler struct {
	dispatcher *dispatcher.Dispatcher
	store      *store.TaskStore
	registry   *tenant.Registry
}

func NewTaskHandler(
	dispatcher *dispatcher.Dispatcher,
	store *store.TaskStore,
	registry *tenant.Registry,
) HandlerRegistry {
	h := &TaskHandler{
		dispatcher: dispatcher,
		store:      store,
		registry:   registry,
	}
	return h.getRegistry()
}

func (h *TaskHandler) getRegistry() HandlerRegistry {
	return func(mux *http.ServeMux) {
		mux.HandleFunc("POST /v1/tasks", h.submitTask)
		mux.HandleFunc("GET /v1/tasks/{id}/status", h.getTaskStatus)
	}
}

type submitTaskRequest struct {
	TenantID string  `json:"tenant_id"`
	Type     string  `json:"type"`
	Cost     float64 `json:"cost"`
	Payload  string  `json:"payload"`
}

type submitTaskResponse struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

func (h *TaskHandler) submitTask(w http.ResponseWriter, r *http.Request) {
	var req submitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	req.TenantID = strings.TrimSpace(req.TenantID)
	if req.TenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}
	if req.Cost <= 0 {
		writeError(w, http.StatusBadRequest, "cost must be positive")
		return
	}

	// Reject unknown tenants before touching the dispatcher.
	if _, err := h.registry.Get(req.TenantID); err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			writeError(w, http.StatusNotFound, "unknown tenant")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to verify tenant")
		return
	}

	task := &engine.Task{
		ID:       newID(),
		TenantID: req.TenantID,
		Cost:     req.Cost,
		Arrived:  time.Now(),
		Metadata: map[string]any{
			"type":    req.Type,
			"payload": req.Payload,
		},
	}

	if err := h.store.Create(task); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to record task")
		return
	}

	if err := h.dispatcher.Enqueue(task); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to enqueue task")
		return
	}

	writeJSON(w, http.StatusCreated, submitTaskResponse{
		TaskID: task.ID,
		Status: "queued",
	})
}

type taskStatusResponse struct {
	TaskID       string     `json:"task_id"`
	Status       string     `json:"status"`
	EnqueuedAt   time.Time  `json:"enqueued_at"`
	DispatchedAt *time.Time `json:"dispatched_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	WorkerID     string     `json:"worker_id,omitempty"`
	Error        string     `json:"error,omitempty"`
}

func (h *TaskHandler) getTaskStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	record, err := h.store.GetTask(id)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to retrieve task")
		return
	}

	writeJSON(w, http.StatusOK, taskStatusResponse{
		TaskID:       record.Task.ID,
		Status:       string(record.Status),
		EnqueuedAt:   record.EnqueuedAt,
		DispatchedAt: record.DispatchedAt,
		CompletedAt:  record.CompletedAt,
		WorkerID:     record.WorkerID,
		Error:        record.Error,
	})
}
