package burst

import (
	"sync"
	"time"
)

// BurstBucket implements token bucket for burst tolerance
type BurstBucket struct {
	Capacity   float64 // Maximum burst token
	RefillRate float64 // Tokens per second
	Tokens     float64 // Current tokens
	LastRefill time.Time
	mu         sync.Mutex
}

func NewBurstBucket(capacity, refillRate float64) *BurstBucket {
	return &BurstBucket{
		Capacity:   capacity,
		RefillRate: refillRate,
		Tokens:     capacity,
		LastRefill: time.Now(),
	}
}

// TryConsume attempts to consume tokens for a burst
func (b *BurstBucket) TryConsume(amount float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elasped := now.Sub(b.LastRefill).Seconds()
	refilled := elasped * b.RefillRate
	b.Tokens = min(b.Capacity, b.Tokens+refilled)
	b.LastRefill = now

	// consume tokens if available
	if b.Tokens >= amount {
		b.Tokens -= amount
		return true
	}
	return false
}
