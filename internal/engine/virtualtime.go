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
		vt: &VirtualTime{
			Current:      0.0,
			LastUpdate:   time.Now(),
			ActiveWeight: 0.0,
		},
	}
}

// Advance updates virtual time based on elapsed real time
// Formula: V(t) = V(t_prev) + (t - t_prev) / ActiveWeight
func (vt *VTTracker) Advance() float64 {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// no backlog tenants, V(t) does not advance
	if vt.vt.ActiveWeight == 0 {
		vt.vt.LastUpdate = time.Now()
		return vt.vt.Current
	}

	now := time.Now()
	elapsed := now.Sub(vt.vt.LastUpdate).Seconds()

	// V(t) advances inversely proportional to active weight
	delta := elapsed / vt.vt.ActiveWeight
	vt.vt.Current += delta
	vt.vt.LastUpdate = now

	return vt.vt.Current
}

// UpdateActiveWeight adjusts the sum of active tenant weights
func (vt *VTTracker) UpdateActiveWeight(delta float64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	vt.vt.ActiveWeight += delta
	if vt.vt.ActiveWeight < 0 {
		vt.vt.ActiveWeight = 0
	}
}

// Get return current virtual time (try advance first)
func (vt *VTTracker) Get() float64 {
	return vt.Advance()
}

// VTSnapshot is a consistent point-in-time view of virtual time state.
type VTSnapshot struct {
	Current      float64
	ActiveWeight float64
}

// Snapshot returns the last-committed values of Current and ActiveWeight
// without advancing virtual time. The value of Current reflects the state
// as of the most recent ComputeVFT call; it may lag real time if no tasks
// have been enqueued recently, which is an accurate description of the
// engine's state and safe for observability use.
func (vt *VTTracker) Snapshot() VTSnapshot {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	return VTSnapshot{
		Current:      vt.vt.Current,
		ActiveWeight: vt.vt.ActiveWeight,
	}
}
