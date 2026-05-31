package engine

import (
	"sync"
	"time"

	"github.com/tayts/fairsched/internal/burst"
)

// tenantState holds engine-internal scheduling state for a single tenant.
// This is not the domain Tenant — see internal/tenant for the canonical config.
type tenantState struct {
	weight          float64            // fairness weight; mirrors tenant.Tenant.Weight
	lastVFT         float64            // last computed virtual finish time
	pendingTasks    int64              // count of tasks in the queue for this tenant
	totalDispatched int64              // lifetime dispatch count
	burstBucket     *burst.BurstBucket // nil if burst is disabled
}

// Task represents a dispatchable unit of work
type Task struct {
	ID       string
	TenantID string
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
