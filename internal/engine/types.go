package engine

import (
	"fmt"
	"sync"
	"time"
)

// Tenant represents a single tenant with its fairness properties
type Tenant struct {
	ID              string
	fairnessWeight  float64 // Fairness weight (default 1.0, range 0.001 - 1000)
	lastVFT         float64 // Last virtual finish time for this tenant
	pendingTasks    int64   // Count of backlog tasks
	totalDispatched int64   // Lifetime dispatch count
	mu              sync.RWMutex
}

func isInvalidWeight(weight float64) bool {
	return weight < 0.001 || weight > 1000
}

func NewTenant(id string, weight float64) (*Tenant, error) {
	if isInvalidWeight(weight) {
		return nil, fmt.Errorf("weight %f out of range (0.001 - 1000)", weight)
	}
	return &Tenant{ID: id, fairnessWeight: weight}, nil
}

// UpdateWeight updates the tenant's fairness weight
func (t *Tenant) UpdateWeight(weight float64) error {
	if isInvalidWeight(weight) {
		return fmt.Errorf("weight %f out of range (0.001 - 1000)", weight)
	}
	t.mu.Lock()
	t.fairnessWeight = weight
	t.mu.Unlock()
	return nil
}

func (t *Tenant) GetWeight() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.fairnessWeight
}

func (t *Tenant) GetLastVFT() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastVFT
}

func (t *Tenant) SetLastVFT(vft float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastVFT = vft
}

func (t *Tenant) GetPendingTasks() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.pendingTasks
}

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
		id:       fmt.Sprintf("%s-%d", t.ID, seq),
		tenantID: t.ID,
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

func NewVirtualTime() *VirtualTime {
	return &VirtualTime{
		current:    0,
		lastUpdate: time.Now(),
	}
}
