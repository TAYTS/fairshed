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

func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{
		limits:  make(map[string]int),
		current: make(map[string]int),
	}
}

func (cl *ConcurrencyLimiter) SetLimit(tenantId string, limit int) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.limits[tenantId] = limit
}

func (cl *ConcurrencyLimiter) TryAcquire(tenantId string) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if limit, ok := cl.limits[tenantId]; ok {
		current := cl.current[tenantId]
		if current >= limit {
			return ErrLimitExceeded
		}
	}

	cl.current[tenantId]++
	return nil
}

func (cl *ConcurrencyLimiter) Release(tenantId string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	current, ok := cl.current[tenantId]
	if !ok || current == 0 {
		return
	}
	cl.current[tenantId]--
}
