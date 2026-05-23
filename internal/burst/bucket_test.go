package burst

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- NewBurstBucket ---

func TestNewBurstBucket_InitialState(t *testing.T) {
	b := NewBurstBucket(10.0, 2.0)

	assert.Equal(t, 10.0, b.Capacity)
	assert.Equal(t, 2.0, b.RefillRate)
	assert.Equal(t, 10.0, b.Tokens, "bucket should start full")
	assert.WithinDuration(t, time.Now(), b.LastRefill, time.Second)
}

// --- TryConsume ---

func TestTryConsume_SucceedsWhenTokensAvailable(t *testing.T) {
	b := NewBurstBucket(10.0, 0.0)

	ok := b.TryConsume(5.0)

	assert.True(t, ok)
	assert.Equal(t, 5.0, b.Tokens)
}

func TestTryConsume_ExactAmount(t *testing.T) {
	b := NewBurstBucket(10.0, 0.0)

	ok := b.TryConsume(10.0)

	assert.True(t, ok)
	assert.Equal(t, 0.0, b.Tokens)
}

func TestTryConsume_FailsWhenInsufficientTokens(t *testing.T) {
	b := NewBurstBucket(10.0, 0.0)
	b.Tokens = 3.0

	ok := b.TryConsume(5.0)

	assert.False(t, ok)
	assert.Equal(t, 3.0, b.Tokens, "tokens should be unchanged on failure")
}

func TestTryConsume_FailsWhenEmpty(t *testing.T) {
	b := NewBurstBucket(10.0, 0.0)
	b.Tokens = 0.0

	ok := b.TryConsume(1.0)

	assert.False(t, ok)
}

// --- Refill ---

func TestTryConsume_RefillsOverTime(t *testing.T) {
	b := NewBurstBucket(10.0, 2.0) // 2 tokens/s
	b.Tokens = 0.0
	b.LastRefill = time.Now().Add(-3 * time.Second) // simulate 3s elapsed -> 6 tokens refilled

	ok := b.TryConsume(5.0)

	assert.True(t, ok)
	assert.InDelta(t, 1.0, b.Tokens, 0.01) // 6 - 5 = 1
}

func TestTryConsume_PartialRefillNotEnough(t *testing.T) {
	b := NewBurstBucket(10.0, 1.0) // 1 token/s
	b.Tokens = 0.0
	b.LastRefill = time.Now().Add(-2 * time.Second) // 2 tokens refilled

	ok := b.TryConsume(5.0) // needs 5, only 2 available

	assert.False(t, ok)
	assert.InDelta(t, 2.0, b.Tokens, 0.01, "tokens should be refilled but not consumed on failure")
}

func TestTryConsume_RefillCappedAtCapacity(t *testing.T) {
	b := NewBurstBucket(10.0, 2.0) // 2 tokens/s
	b.Tokens = 8.0
	b.LastRefill = time.Now().Add(-10 * time.Second) // would add 20, but capped

	b.TryConsume(1.0)

	assert.InDelta(t, 9.0, b.Tokens, 0.01, "tokens should be capped at capacity after refill")
}

// --- Concurrency ---

func TestTryConsume_Concurrent(t *testing.T) {
	const n = 100
	b := NewBurstBucket(float64(n), 0.0) // exactly enough tokens for n goroutines

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for range n {
		wg.Go(func() {
			if b.TryConsume(1.0) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	assert.Equal(t, n, successCount, "all goroutines should succeed")
	assert.InDelta(t, 0.0, b.Tokens, 0.01, "bucket should be empty")
}

func TestTryConsume_ConcurrentOverdraft(t *testing.T) {
	b := NewBurstBucket(50.0, 0.0) // only enough for 50 of 100 goroutines

	const n = 100
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for range n {
		wg.Go(func() {
			if b.TryConsume(1.0) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	assert.Equal(t, 50, successCount, "only 50 goroutines should succeed")
	assert.InDelta(t, 0.0, b.Tokens, 0.01, "bucket should be empty, never negative")
}
