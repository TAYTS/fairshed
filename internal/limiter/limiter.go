package limiter

import (
	"errors"
	"sync"
)

var ErrLimitExceeded = errors.New("concurrency limit exceeded")

type ConcurrencyLimiter struct {
	limits  map[string]int // tenantId -> max concurrency
	current map[string]int // tenantId -> current count
	mu      sync.Mutex
}

// NewConcurrencyLimiter create new concurrency limiter instance.
func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{
		limits:  make(map[string]int),
		current: make(map[string]int),
	}
}

// SetLimit set/override the concurrency limit of a tenant.
func (cl *ConcurrencyLimiter) SetLimit(tenantID string, limit int) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.limits[tenantID] = limit
}

// TryAcquire increase the concurrency counter of the tenant if exist.
// Return ErrLimitExceeded if the concurrency counter is already at its limit.
// No-op if the tenant doesn't exist.
func (cl *ConcurrencyLimiter) TryAcquire(tenantID string) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if limit, ok := cl.limits[tenantID]; ok {
		current := cl.current[tenantID]
		if current >= limit {
			return ErrLimitExceeded
		}
	}

	cl.current[tenantID]++
	return nil
}

// Release reduce the concurrency counter of the tenant if exist.
// No-op if the tenant doesn't exist or the concurrency counter is already 0.
func (cl *ConcurrencyLimiter) Release(tenantID string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	current, ok := cl.current[tenantID]
	if !ok || current == 0 {
		return
	}
	cl.current[tenantID]--
}
