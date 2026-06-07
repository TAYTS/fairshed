package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- NewVTTracker ---

func TestNewVTTracker(t *testing.T) {
	tracker := NewVTTracker()

	assert.NotNil(t, tracker, "NewTracker should not be nil")
	assert.NotNil(t, tracker.vt, "NewTracker vt field should not be nil")
}

// --- Advance with zero active weight ---

func TestAdvance_ZeroActiveWeight(t *testing.T) {
	tracker := NewVTTracker()

	assert.Equal(t, 0.0, tracker.Advance(), "Advance with 0 activeWeight should return 0")

	// Wait a bit and advance again — should still be 0
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0.0, tracker.Advance(), "Advance after sleep with 0 activeWeight should return 0")
}

// --- Advance with active weight ---

func TestAdvance_WithActiveWeight(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	// Initial advance may have small elapsed time, just check it's small
	assert.LessOrEqual(t, tracker.Advance(), 0.01, "initial Advance should be ~0")

	// Wait 100ms and advance; delta = 0.1 / 1.0 = 0.1
	time.Sleep(100 * time.Millisecond)
	assert.InDelta(t, 0.1, tracker.Advance(), 0.02, "Advance after 100ms with weight 1.0 should be ~0.1")
}

// --- Advance with varying active weight ---

func TestAdvance_VaryingActiveWeight(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(2.0)

	// Initial advance may have small elapsed time
	assert.LessOrEqual(t, tracker.Advance(), 0.01, "initial Advance should be ~0")

	// Wait 100ms: delta = 0.1 / 2.0 = 0.05
	time.Sleep(100 * time.Millisecond)
	vt2 := tracker.Advance()
	assert.InDelta(t, 0.05, vt2, 0.02, "Advance after 100ms with weight 2.0 should be ~0.05")

	// Increase weight and wait more; delta = 0.1 / 4.0 = 0.025
	tracker.UpdateActiveWeight(2.0) // now activeWeight = 4.0
	time.Sleep(100 * time.Millisecond)
	vt3 := tracker.Advance()
	assert.Greater(t, vt3, vt2, "virtual time should increase after second advance")
	assert.InDelta(t, vt2+0.025, vt3, 0.02, "Advance after weight change should be ~vt2+0.025")
}

// --- UpdateActiveWeight ---

func TestUpdateActiveWeight_Positive(t *testing.T) {
	tracker := NewVTTracker()

	tracker.UpdateActiveWeight(1.5)
	assert.Equal(t, 1.5, tracker.vt.ActiveWeight, "activeWeight after +1.5")

	tracker.UpdateActiveWeight(2.5)
	assert.Equal(t, 4.0, tracker.vt.ActiveWeight, "activeWeight after +2.5")
}

func TestUpdateActiveWeight_Negative(t *testing.T) {
	tracker := NewVTTracker()

	tracker.UpdateActiveWeight(5.0)
	assert.Equal(t, 5.0, tracker.vt.ActiveWeight, "activeWeight after +5.0")

	tracker.UpdateActiveWeight(-3.0)
	assert.Equal(t, 2.0, tracker.vt.ActiveWeight, "activeWeight after -3.0")
}

func TestUpdateActiveWeight_BelowZero(t *testing.T) {
	tracker := NewVTTracker()

	tracker.UpdateActiveWeight(1.0)
	tracker.UpdateActiveWeight(-5.0)
	assert.Equal(t, 0.0, tracker.vt.ActiveWeight, "activeWeight should be clamped to 0")
}

// --- Get ---

func TestGet_CallsAdvance(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	// Initial Get may have small elapsed time
	assert.LessOrEqual(t, tracker.Get(), 0.01, "Get() initial should be ~0")

	// delta = 0.05 / 1.0 = 0.05
	time.Sleep(50 * time.Millisecond)
	assert.InDelta(t, 0.05, tracker.Get(), 0.02, "Get() after 50ms should be ~0.05")
}

// --- Concurrency ---

func TestConcurrentAdvanceWithNoActiveWeight(t *testing.T) {
	tracker := NewVTTracker()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			tracker.Advance()
		}()
	}
	wg.Wait()

	assert.Equal(t, float64(0.0), tracker.Get(), "virtual time should not advance when there is no active weight")
}

func TestConcurrentAdvanceWithActiveWeight(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	vtBefore := tracker.Advance()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			tracker.Advance()
		}()
	}
	wg.Wait()

	// Per-goroutine ordering is non-deterministic, but the aggregate must advance
	assert.Greater(t, tracker.Advance(), vtBefore, "virtual time should be greater after concurrent advances")
}

func TestConcurrentUpdateActiveWeight(t *testing.T) {
	tracker := NewVTTracker()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			tracker.UpdateActiveWeight(1.0)
		}()
	}
	wg.Wait()

	assert.Equal(t, float64(goroutines), tracker.vt.ActiveWeight, "activeWeight should equal sum of all increments")
}

func TestConcurrentUpdateActiveWeightWithNegative(t *testing.T) {
	tracker := NewVTTracker()

	// Use large negatives to stress the clamp-to-zero path under concurrency
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(2 * goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			tracker.UpdateActiveWeight(1.0)
		}()
		go func() {
			defer wg.Done()
			tracker.UpdateActiveWeight(-100.0)
		}()
	}
	wg.Wait()

	// Exact final value is non-deterministic due to scheduling, but the clamp invariant must hold
	assert.GreaterOrEqual(t, tracker.vt.ActiveWeight, 0.0, "activeWeight should never go negative")
}

func TestConcurrentAdvanceAndUpdateWeight(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			tracker.Advance()
		}()
		go func() {
			defer wg.Done()
			tracker.UpdateActiveWeight(1.0)
		}()
	}
	wg.Wait()

	// After mixed ops, activeWeight should be non-negative and VT should not regress
	assert.GreaterOrEqual(t, tracker.vt.ActiveWeight, 0.0, "activeWeight should never be negative")
	assert.GreaterOrEqual(t, tracker.Advance(), 0.0, "virtual time should never be negative")
}

// --- Monotonic advancement ---

func TestMonotonicAdvance(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	vt := make([]float64, 5)
	for i := range 5 {
		vt[i] = tracker.Advance()
		time.Sleep(20 * time.Millisecond)
	}

	for i := 1; i < 5; i++ {
		assert.Greater(t, vt[i], vt[i-1], "virtual time should be monotonically increasing")
	}
}

// --- Snapshot ---

func TestSnapshot_InitialValuesAreZero(t *testing.T) {
	tracker := NewVTTracker()
	snap := tracker.Snapshot()
	assert.Equal(t, 0.0, snap.Current)
	assert.Equal(t, 0.0, snap.ActiveWeight)
}

func TestSnapshot_ReflectsCommittedValues(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(3.0)
	committed := tracker.Advance()

	snap := tracker.Snapshot()
	assert.Equal(t, committed, snap.Current)
	assert.Equal(t, 3.0, snap.ActiveWeight)
}

func TestSnapshot_DoesNotAdvanceVirtualTime(t *testing.T) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)
	committed := tracker.Advance()

	time.Sleep(20 * time.Millisecond)
	snap := tracker.Snapshot()

	// Snapshot must return the last committed value, not advance VT.
	assert.Equal(t, committed, snap.Current, "Snapshot must not advance virtual time")

	// A subsequent Advance must show that the elapsed time was preserved.
	assert.Greater(t, tracker.Advance(), snap.Current, "Advance after Snapshot must reflect elapsed time")
}

func TestSnapshot_ZeroActiveWeightNeverAdvances(t *testing.T) {
	tracker := NewVTTracker()
	// No active weight: VT is frozen regardless of elapsed time.
	time.Sleep(20 * time.Millisecond)
	snap := tracker.Snapshot()
	assert.Equal(t, 0.0, snap.Current)
}

// --- Benchmark ---

func BenchmarkAdvance(b *testing.B) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	b.ResetTimer()
	for b.Loop() {
		tracker.Advance()
	}
}

func BenchmarkUpdateActiveWeightAndAdvanceConcurrent(b *testing.B) {
	tracker := NewVTTracker()
	tracker.UpdateActiveWeight(1.0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				tracker.UpdateActiveWeight(1.0)
			} else {
				tracker.Advance()
			}
			i++
		}
	})
}
