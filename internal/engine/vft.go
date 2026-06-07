package engine

import (
	"errors"
	"math"
	"sync"

	"github.com/tayts/fairsched/internal/burst"
	"github.com/tayts/fairsched/internal/tenant"
)

// ErrTenantNotRegistered is returned by ComputeVFT when the task's tenant has
// not been registered via RegisterTenant. Tasks for unknown tenants are rejected
// to prevent them jumping to the front of the queue with VFT=0.
var ErrTenantNotRegistered = errors.New("tenant not registered in engine")

// burstFactor reduces the VFT advance for burst-absorbed tasks.
// A value of 0.5 gives ~2x throughput during a burst window.
// TODO: make this configurable per tenant.
const burstFactor = 0.5

// VFTEngine computes virtual finish times for tasks
type VFTEngine struct {
	vtTracker *VTTracker
	tenants   map[string]*tenantState
	mu        sync.RWMutex
}

// NewVFTEngine creates a new VFTEngine
func NewVFTEngine() *VFTEngine {
	return &VFTEngine{
		vtTracker: NewVTTracker(),
		tenants:   make(map[string]*tenantState),
	}
}

// RegisterTenant registers a tenant with the engine.
// If the tenant is already registered, its weight and burst config are updated.
// Callers should call this before submitting tasks.
// burstCapacity <= 0 disables burst for this tenant.
func (e *VFTEngine) RegisterTenant(tenant *tenant.Tenant) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state, exists := e.tenants[tenant.ID]
	if !exists {
		state = &tenantState{weight: tenant.Weight}
		e.tenants[tenant.ID] = state
	}

	if tenant.BurstCapacity > 0 {
		if state.burstBucket == nil || state.burstBucket.Capacity != tenant.BurstCapacity || state.burstBucket.RefillRate != tenant.BurstRate {
			state.burstBucket = burst.NewBurstBucket(tenant.BurstCapacity, tenant.BurstRate)
		}
	} else {
		state.burstBucket = nil
	}

	// If the weight changed and tenant has pending tasks, adjust active weight.
	if state.pendingTasks > 0 {
		e.vtTracker.UpdateActiveWeight(tenant.Weight - state.weight)
	}
	state.weight = tenant.Weight
}

// ComputeVFT calculates virtual finish time for a task.
// Formula: F_i = max(V(t), F_{i,prev}) + (cost_i / weight_i)
// Returns (0.0, ErrTenantNotRegistered) if the tenant has not been registered via RegisterTenant.
func (e *VFTEngine) ComputeVFT(task *Task) (float64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	s, exists := e.tenants[task.TenantID]
	if !exists {
		return 0.0, ErrTenantNotRegistered
	}

	// Get current virtual time
	vt := e.vtTracker.Get()

	// Compute virtual start time
	vst := math.Max(vt, s.lastVFT)

	// Compute virtual finish time
	var vft float64

	// Try absorb burst cost
	if s.burstBucket != nil && s.burstBucket.TryConsume(task.Cost) {
		// absorb burst, reduce cost
		vft = vst + (task.Cost/s.weight)*burstFactor
	} else {
		// normal cost
		vft = vst + (task.Cost / s.weight)
	}

	// Update tenant state
	s.lastVFT = vft
	s.pendingTasks++

	// If this is the first task, add tenant weight to active weight
	if s.pendingTasks == 1 {
		e.vtTracker.UpdateActiveWeight(s.weight)
	}

	return vft, nil
}

// OnTaskDispatched updates state when a task leaves the queue.
func (e *VFTEngine) OnTaskDispatched(task *Task) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state, exists := e.tenants[task.TenantID]
	if !exists {
		return
	}

	if state.pendingTasks > 0 {
		state.pendingTasks--
		state.totalDispatched++

		// if no more pending task, remove weight from active weight
		if state.pendingTasks == 0 {
			e.vtTracker.UpdateActiveWeight(-state.weight)
		}
	}
}

// EngineStats is a snapshot of the VFTEngine's scheduling state.
type EngineStats struct {
	VirtualTime float64       // Current global virtual time V(t)
	Tenants     []TenantStats // Per-tenant snapshot, sorted by TenantID
}

// TenantStats is a snapshot of a single tenant's scheduling state.
type TenantStats struct {
	TenantID string
	Weight   float64
	LastVFT  float64
}

// Stats returns a consistent point-in-time snapshot of the engine's scheduling state.
// Safe to call concurrently with ComputeVFT and OnTaskDispatched.
func (e *VFTEngine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tenants := make([]TenantStats, 0, len(e.tenants))
	for id, s := range e.tenants {
		tenants = append(tenants, TenantStats{
			TenantID: id,
			Weight:   s.weight,
			LastVFT:  s.lastVFT,
		})
	}

	return EngineStats{
		VirtualTime: e.vtTracker.Snapshot().Current,
		Tenants:     tenants,
	}
}
