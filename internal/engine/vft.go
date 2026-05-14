package engine

import (
	"math"
	"sync"
)

// VFTEngine computes virtual finish times for tasks
type VFTEngine struct {
	vtTracker *VTTracker
	tenants   map[string]*Tenant
	mu        sync.RWMutex
}

// NewVFTEngine creates a new VFTEngine
func NewVFTEngine() *VFTEngine {
	return &VFTEngine{
		vtTracker: NewVTTracker(),
		tenants:   make(map[string]*Tenant),
	}
}

// ComputeVFT calculates virtual finish time for a task
// Formula: F_i = max(V(t), F_{i,prev}) + (cost_i / weight_i)
func (e *VFTEngine) ComputeVFT(task *Task) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	tenant, exists := e.tenants[task.tenantID]
	if !exists {
		// New tenant, create with default weight
		tenant = &Tenant{id: task.tenantID, weight: 1.0}
		e.tenants[task.tenantID] = tenant
	}

	// Get current virtual time
	vt := e.vtTracker.Get()

	// Compute virtual start time
	vst := math.Max(vt, tenant.lastVFT)

	// Compute virtual finish time
	vft := vst + (task.cost / tenant.weight)

	// Update tenant state
	tenant.lastVFT = vft
	tenant.pendingTasks++

	// If this is the first task, add tenant weight to active weight
	if tenant.pendingTasks == 1 {
		e.vtTracker.UpdateActiveWeight(tenant.weight)
	}

	return vft
}

// OnTaskDispatched updates state when a task completes dispatch
func (e *VFTEngine) OnTaskDispatched(task *Task) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tenant, exists := e.tenants[task.tenantID]
	if !exists {
		return
	}

	if tenant.pendingTasks > 0 {
		tenant.pendingTasks--
		tenant.totalDispatched++

		// if no more pending task, remove weight from active weight
		if tenant.pendingTasks == 0 {
			e.vtTracker.UpdateActiveWeight(-tenant.weight)
		}
	}
}

// SetTenantWeight updates a tenant's fairness weight
func (e *VFTEngine) SetTenantWeight(tenantID string, weight float64) {
	// clamp weight to [0.001, 1000]
	weight = math.Max(0.001, math.Min(1000.0, weight))

	e.mu.Lock()
	defer e.mu.Unlock()

	tenant, exists := e.tenants[tenantID]
	if !exists {
		tenant = &Tenant{id: tenantID, weight: weight}
		e.tenants[tenantID] = tenant
		return
	}

	// if tenant has pending tasks, adjust active weight
	if tenant.pendingTasks > 0 {
		delta := weight - tenant.weight
		e.vtTracker.UpdateActiveWeight(delta)
	}
	tenant.weight = weight
}
