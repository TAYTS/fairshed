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

func TestRelease_LazyDeletesCurrentEntry(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 2)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))

	// Remove limit while tasks are in-flight — counter must survive.
	cl.SetLimit("t1", 0)
	_, exists := cl.current["t1"]
	assert.True(t, exists, "current entry must survive SetLimit(0) while tasks are in-flight")

	// Release all in-flight tasks — entry lazily removed once count hits 0.
	cl.Release("t1")
	cl.Release("t1")
	_, exists = cl.current["t1"]
	assert.False(t, exists, "current entry should be removed once count hits 0 with no limit")
}

func TestRelease_UnknownTenant_IsNoOp(t *testing.T) {
	cl := NewConcurrencyLimiter()

	// Must not panic or create state for an unknown tenant
	assert.NotPanics(t, func() { cl.Release("ghost") })
	_, exists := cl.current["ghost"]
	assert.False(t, exists, "release on unknown tenant must not create state")
}

// --- SetLimit ---

func TestSetLimit_ZeroRemovesLimit(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 2)

	assert.NoError(t, cl.TryAcquire("t1"))
	assert.NoError(t, cl.TryAcquire("t1"))
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded)

	// Remove the limit — tenant becomes unlimited.
	cl.SetLimit("t1", 0)
	for range 100 {
		assert.NoError(t, cl.TryAcquire("t1"), "tenant with no limit should never be rejected")
	}
}

func TestSetLimit_LowerLimit_BackpressureUntilDrained(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 5)
	for range 5 {
		assert.NoError(t, cl.TryAcquire("t1"))
	}

	// Lower to 2 while 5 tasks are in-flight — no clamping; counter stays at 5.
	cl.SetLimit("t1", 2)

	// No new tasks accepted until the real in-flight count drops below the new limit.
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded,
		"5 in-flight against limit 2: no slots available immediately")

	// Release 3: current 5->4->3->2 — still at the limit.
	cl.Release("t1")
	cl.Release("t1")
	cl.Release("t1")
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded,
		"current=2 equals the limit; still blocked")

	// One more release: current 2->1 — slot opens.
	cl.Release("t1")
	assert.NoError(t, cl.TryAcquire("t1"), "slot opens once count drops below the new limit")
}

func TestSetLimit_Cycle_RemoveAndReapply(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("t1", 5)
	for range 3 {
		assert.NoError(t, cl.TryAcquire("t1"))
	}

	// Remove the limit — in-flight counter must be preserved (not deleted).
	cl.SetLimit("t1", 0)
	assert.Equal(t, 3, cl.current["t1"], "in-flight count must survive SetLimit(0)")

	// Acquire 4 more while unlimited — counter accurately reflects all 7 in-flight.
	for range 4 {
		assert.NoError(t, cl.TryAcquire("t1"))
	}
	assert.Equal(t, 7, cl.current["t1"])

	// Re-enable with limit 3; 7 in-flight exceeds it — backpressure applies immediately.
	cl.SetLimit("t1", 3)
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded,
		"7 in-flight against re-enabled limit of 3: blocked")

	// Release until count drops below 3: 7→6→5→4→3 still blocked, 3→2 opens a slot.
	for range 4 {
		cl.Release("t1")
	}
	assert.ErrorIs(t, cl.TryAcquire("t1"), ErrLimitExceeded, "current=3 equals limit; still blocked")
	cl.Release("t1")
	assert.NoError(t, cl.TryAcquire("t1"), "slot opens once count drops below re-enabled limit")
}

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
