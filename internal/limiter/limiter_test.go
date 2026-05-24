package limiter

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- TryAcquire ---

func TestTryAcquire_NoLimit_AlwaysSucceeds(t *testing.T) {
	cl := NewConcurrencyLimiter()

	for range 1000 {
		assert.NoError(t, cl.TryAcquire("t1"),
			"tenant with no registered limit should never be rejected")
	}
}

func TestTryAcquire_WithinLimit_Succeeds(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 3)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))
}

func TestTryAcquire_AtLimit_Rejected(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 2)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded)
}

func TestTryAcquire_LimitPerTenant_Independent(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 1)
	cl.SetLimit("t2", 1)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded, "t1 should be at limit")

	// t2 has its own independent slot
	assert.NoError(t, cl.TryAcquire("t2"), "t2 should be unaffected by t1's limit")
}

// --- Release ---

func TestRelease_FreesSlot(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 1)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded)

	cl.Release("t1")

	assert.NoError(t, cl.TryAcquire("t1"), "slot should be available after release")
}

func TestRelease_Underflow_IsNoOp(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 2)

	assert.NoError(t, cl.TryAcquire("t1"))
	cl.Release("t1")

	// Extra release must not underflow below zero
	cl.Release("t1")
	cl.Release("t1")

	// Should still be able to acquire up to the full limit
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded,
		"underflow releases must not inflate available slots")
}

func TestRelease_UnknownTenant_IsNoOp(t *testing.T) {
	cl := NewConcurrencyLimiter()

	// Must not panic or create state for an unknown tenant
	assert.NotPanics(t, func() { cl.Release("ghost") })
	_, exists := cl.current["ghost"]
	assert.False(t, exists, "release on unknown tenant must not create state")
}

// --- SetLimit ---

func TestSetLimit_UpdatesExistingLimit(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 1)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded)

	// Raise the limit — the existing in-flight slot is preserved
	cl.SetLimit("t1", 3)
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded,
		"new limit of 3 with 1 already in-flight means 2 more slots")
}

// --- Concurrency ---

func TestConcurrentTryAcquire_NeverExceedsLimit(t *testing.T) {
	cl := NewConcurrencyLimiter()
	const limit = 10
	cl.SetLimit("t1", limit)

	const tasks = 200
	var wg sync.WaitGroup
	wg.Add(tasks)

	acquired := make(chan struct{}, tasks)
	for range tasks {
		go func() {
			defer wg.Done()
			if cl.TryAcquire("t1") == nil {
				acquired <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(acquired)

	assert.Equal(t, limit, len(acquired),
		"concurrent acquires must never exceed the configured limit")
}

func TestConcurrentAcquireRelease_NoDataRace(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 5)

	const tasks = 100
	var wg sync.WaitGroup
	wg.Add(tasks)

	for range tasks {
		go func() {
			defer wg.Done()
			if cl.TryAcquire("t1") == nil {
				cl.Release("t1")
			}
		}()
	}
	wg.Wait()
	// No assertion needed — the race detector catches data races
}
