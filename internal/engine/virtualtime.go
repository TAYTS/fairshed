package engine

import (
	"sync"
	"time"
)

// VTTracker manages global virtual time across all tenants
type VTTracker struct {
	vt *VirtualTime
	mu sync.RWMutex
}

// NewVTTracker creates a new VTTracker
func NewVTTracker() *VTTracker {
	return &VTTracker{
		vt: NewVirtualTime(),
	}
}

// Advance updates virtual time based on elapsed real time
// Formula: V(t) = V(t_prev) + (t - t_prev) / ActiveWeight
func (vt *VTTracker) Advance() float64 {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// no backlog tenants, V(t) does not advance
	if vt.vt.activeWeight == 0 {
		vt.vt.lastUpdate = time.Now()
		return vt.vt.current
	}

	now := time.Now()
	elapsed := now.Sub(vt.vt.lastUpdate).Seconds()

	// V(t) advances inversely proportional to active weight
	delta := elapsed / vt.vt.activeWeight
	vt.vt.current += delta
	vt.vt.lastUpdate = now

	return vt.vt.current
}

// UpdateActiveWeight adjusts the sum of active tenant weights
func (vt *VTTracker) UpdateActiveWeight(delta float64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	vt.vt.activeWeight += delta
	if vt.vt.activeWeight < 0 {
		vt.vt.activeWeight = 0
	}
}

// Get return current virtual time (try advance first)
func (vt *VTTracker) Get() float64 {
	return vt.Advance()
}
