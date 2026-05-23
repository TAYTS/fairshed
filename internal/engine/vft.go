package engine

import (
	"math"
	"sync"
)

// burstFactor reduces the VFT advance for burst-absorbed tasks.
// A value of 0.5 gives ~2x throughput during a burst window.
// TODO: make this configurable per tenant.
const burstFactor = 0.5

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

	tenant, exists := e.tenants[task.TenantId]
	if !exists {
		// New tenant, create with default weight
		tenant = &Tenant{Id: task.TenantId, Weight: 1.0}
		e.tenants[task.TenantId] = tenant
	}

	// Get current virtual time
	vt := e.vtTracker.Get()

	// Compute virtual start time
	vst := math.Max(vt, tenant.LastVFT)

	// Compute virtual finish time
	var vft float64

	// Try absorb burst cost
	if tenant.BurstBucket != nil && tenant.BurstBucket.TryConsume(task.Cost) {
		// absorb burst, reduce cost
		vft = vst + (task.Cost/tenant.Weight)*burstFactor
	} else {
		// normal cost
		vft = vst + (task.Cost / tenant.Weight)
	}

	// Update tenant state
	tenant.LastVFT = vft
	tenant.PendingTasks++

	// If this is the first task, add tenant weight to active weight
	if tenant.PendingTasks == 1 {
		e.vtTracker.UpdateActiveWeight(tenant.Weight)
	}

	return vft
}

// OnTaskDispatched updates state when a task completes dispatch
func (e *VFTEngine) OnTaskDispatched(task *Task) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tenant, exists := e.tenants[task.TenantId]
	if !exists {
		return
	}

	if tenant.PendingTasks > 0 {
		tenant.PendingTasks--
		tenant.TotalDispatched++

		// if no more pending task, remove weight from active weight
		if tenant.PendingTasks == 0 {
			e.vtTracker.UpdateActiveWeight(-tenant.Weight)
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
		tenant = &Tenant{Id: tenantID, Weight: weight}
		e.tenants[tenantID] = tenant
		return
	}

	// if tenant has pending tasks, adjust active weight
	if tenant.PendingTasks > 0 {
		delta := weight - tenant.Weight
		e.vtTracker.UpdateActiveWeight(delta)
	}
	tenant.Weight = weight
}
