package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/tenant"
)

type TenantHandler struct {
	dispatcher *dispatcher.Dispatcher
	registry   *tenant.Registry
}

func NewTenantHandler(dispatcher *dispatcher.Dispatcher, registry *tenant.Registry) HandlerRegistry {
	h := TenantHandler{
		dispatcher: dispatcher,
		registry:   registry,
	}
	return h.getRegistry()
}

func (h *TenantHandler) getRegistry() HandlerRegistry {
	return func(mux *http.ServeMux) {
		mux.HandleFunc("POST /v1/tenants", h.registerTenant)
		mux.HandleFunc("GET /v1/tenants/{id}", h.getTenant)
		mux.HandleFunc("PATCH /v1/tenants/{id}/weight", h.patchTenantWeight)
	}
}

type registerTenantRequest struct {
	ID             string  `json:"id"`
	Weight         float64 `json:"weight"`
	MaxConcurrency int     `json:"max_concurrency"`
	BurstCapacity  float64 `json:"burst_capacity"`
	BurstRate      float64 `json:"burst_rate"`
}

type tenantResponse struct {
	ID             string  `json:"id"`
	Weight         float64 `json:"weight"`
	MaxConcurrency int     `json:"max_concurrency"`
	BurstCapacity  float64 `json:"burst_capacity"`
	BurstRate      float64 `json:"burst_rate"`
}

func tenantToResponse(t *tenant.Tenant) tenantResponse {
	return tenantResponse{
		ID:             t.ID,
		Weight:         t.Weight,
		MaxConcurrency: t.MaxConcurrency,
		BurstCapacity:  t.BurstCapacity,
		BurstRate:      t.BurstRate,
	}
}

func (h *TenantHandler) registerTenant(w http.ResponseWriter, r *http.Request) {
	var req registerTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	t := tenant.Tenant{
		ID:             req.ID,
		Weight:         req.Weight,
		MaxConcurrency: req.MaxConcurrency,
		BurstCapacity:  req.BurstCapacity,
		BurstRate:      req.BurstRate,
	}

	if err := h.registry.Create(t); err != nil {
		if errors.Is(err, tenant.ErrTenantAlreadyExists) {
			writeError(w, http.StatusConflict, "tenant already exists")
			return
		}
		if errors.Is(err, tenant.ErrInvalidWeight) || errors.Is(err, tenant.ErrInvalidValue) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create tenant")
		return
	}

	// Re-read to get the normalized value set by registry
	stored, err := h.registry.Get(req.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read tenant after creation")
		return
	}

	h.dispatcher.RegisterTenant(stored)

	writeJSON(w, http.StatusCreated, tenantToResponse(stored))
}

func (h *TenantHandler) getTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	t, err := h.registry.Get(id)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			writeError(w, http.StatusNotFound, "tenant not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to retrieve tenant")
		return
	}

	writeJSON(w, http.StatusOK, tenantToResponse(t))
}

type patchWeightRequest struct {
	Weight float64 `json:"weight"`
}

func (h *TenantHandler) patchTenantWeight(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req patchWeightRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if err := h.registry.UpdateWeight(id, req.Weight); err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			writeError(w, http.StatusNotFound, "tenant not found")
			return
		}
		if errors.Is(err, tenant.ErrInvalidWeight) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to update tenant weight")
		return
	}

	// Propagate the updated weight to the VFT engine and limiter.
	updated, err := h.registry.Get(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read tenant after update")
		return
	}

	h.dispatcher.RegisterTenant(updated)

	writeJSON(w, http.StatusOK, tenantToResponse(updated))
}
