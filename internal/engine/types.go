package engine

import (
	"sync"
	"time"

	"github.com/tayts/fairsched/internal/burst"
)

// Tenant represents a single tenant with its fairness properties
type Tenant struct {
	Id              string
	Weight          float64 // Fairness weight (default 1.0, range 0.001 - 1000)
	LastVFT         float64 // Last virtual finish time for this tenant
	PendingTasks    int64   // Count of backlog tasks
	TotalDispatched int64   // Lifetime dispatch count
	BurstBucket     *burst.BurstBucket
}

// Task represents a dispatchable unit of work
type Task struct {
	Id       string
	TenantId string
	Cost     float64        // Task cost (default 1.0)
	VFT      float64        // Computed virtual finish time
	Arrived  time.Time      // Arrival timestamp
	Metadata map[string]any // Extra metadata
}

// VirtualTime tracks global virtual time state
type VirtualTime struct {
	Current      float64 // Current virtual time V(t)
	LastUpdate   time.Time
	ActiveWeight float64 // Sum of weights of backlog tenants
	mu           sync.RWMutex
}
