package engine

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Tenant represents a single tenant with its fairness properties
type Tenant struct {
	id              string
	weight          float64 // Fairness weight (default 1.0, range 0.001 - 1000)
	lastVFT         float64 // Last virtual finish time for this tenant
	pendingTasks    int64   // Count of backlog tasks
	totalDispatched int64   // Lifetime dispatch count
	mu              sync.RWMutex
}

func clampWeight(weight float64) float64 {
	return math.Max(0.001, math.Min(1000.0, weight))
}

// NewTenant creates a new tenant with the given weight.
// If weight is out of the range [0.001, 1000], it will be clamped to the nearest boundary.
func NewTenant(id string, weight float64) *Tenant {
	return &Tenant{id: id, weight: clampWeight(weight)}
}

// UpdateWeight updates the tenant's fairness weight.
// If weight is out of the range [0.001, 1000], it will be clamped to the nearest boundary.
func (t *Tenant) UpdateWeight(weight float64) {
	t.mu.Lock()
	t.weight = clampWeight(weight)
	t.mu.Unlock()
}

// GetWeight returns the tenant's fairness weight
func (t *Tenant) GetWeight() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.weight
}

// GetLastVFT returns the tenant's last virtual finish time
func (t *Tenant) GetLastVFT() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastVFT
}

// SetLastVFT sets the tenant's last virtual finish time
func (t *Tenant) SetLastVFT(vft float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastVFT = vft
}

// GetPendingTasks returns the number of pending tasks for the tenant
func (t *Tenant) GetPendingTasks() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.pendingTasks
}

// CompleteTask marks a task as completed, updating the tenant's state accordingly
func (t *Tenant) CompleteTask() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pendingTasks > 0 {
		t.pendingTasks--
		t.totalDispatched++
	}
}

func (t *Tenant) NewTask(cost float64) *Task {
	t.mu.Lock()
	t.pendingTasks++
	seq := t.pendingTasks + t.totalDispatched
	t.mu.Unlock()
	return &Task{
		id:       fmt.Sprintf("%s-%d", t.id, seq),
		tenantID: t.id,
		cost:     cost,
		arrived:  time.Now(),
	}
}

// Task represents a dispatchable unit of work
type Task struct {
	id       string
	tenantID string
	cost     float64        // Task cost (default 1.0)
	vft      float64        // Computed virtual finish time
	arrived  time.Time      // Arrival timestamp
	metadata map[string]any // Extra metadata
}

// VirtualTime tracks global virtual time state
type VirtualTime struct {
	current      float64 // Current virtual time V(t)
	lastUpdate   time.Time
	activeWeight float64 // Sum of weights of backlog tenants
	mu           sync.RWMutex
}

// NewVirtualTime initializes a new VirtualTime instance
func NewVirtualTime() *VirtualTime {
	return &VirtualTime{
		current:      0.0,
		lastUpdate:   time.Now(),
		activeWeight: 0.0,
	}
}
