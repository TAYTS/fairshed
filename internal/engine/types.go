package engine

import (
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
