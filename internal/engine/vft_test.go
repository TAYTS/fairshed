package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tayts/fairsched/internal/tenant"
)

// --- ComputeVFT ---

func TestComputeVFT_UnregisteredTenantReturnsError(t *testing.T) {
	e := NewVFTEngine()
	task := &Task{TenantID: "ghost", Cost: 1.0}

	vft, err := e.ComputeVFT(task)

	assert.ErrorIs(t, err, ErrTenantNotRegistered, "unknown tenant must return ErrTenantNotRegistered")
	assert.Equal(t, 0.0, vft, "VFT must be 0 when error is returned")
	_, exists := e.tenants["ghost"]
	assert.False(t, exists, "unknown tenant must not be auto-created")
}

func TestComputeVFT_Formula(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})

	// With activeWeight=0 virtual time stays at 0, so VFT = 0 + cost/weight
	vft1, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 2.0})
	assert.NoError(t, err)
	assert.Equal(t, 2.0, vft1, "first VFT = 0 + 2.0/1.0")

	// Second task: lastVFT=2.0, vt≈0 -> vst=max(0,2.0)=2.0, VFT=2.0+3.0=5.0
	vft2, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 3.0})
	assert.NoError(t, err)
	assert.Equal(t, 5.0, vft2, "second VFT = max(vt, 2.0) + 3.0/1.0")

	assert.Greater(t, vft2, vft1, "VFTs must be monotonically increasing")
}

func TestComputeVFT_VTAhead(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})

	// Manually push virtual time ahead by adding active weight and advancing
	e.vtTracker.UpdateActiveWeight(1.0)
	e.vtTracker.Advance()

	// Force lastVFT to a value behind current virtual time
	_, _ = e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0}) // establishes lastVFT
	e.OnTaskDispatched(&Task{TenantID: "t1"})             // clear pending so activeWeight resets

	// Set lastVFT=0 directly to simulate vt > lastVFT
	e.tenants["t1"].lastVFT = 0

	vt := e.vtTracker.Get()
	vft, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})
	assert.NoError(t, err)

	// vst = max(vt, 0) = vt; VFT = vt + 1.0
	assert.InDelta(t, vt+1.0, vft, 0.01, "when vt > lastVFT, vt is used as base")
}

func TestComputeVFT_MultiTenant(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "a", Weight: 1.0})
	e.RegisterTenant(&tenant.Tenant{ID: "b", Weight: 1.0})

	vftA, errA := e.ComputeVFT(&Task{TenantID: "a", Cost: 1.0})
	vftB, errB := e.ComputeVFT(&Task{TenantID: "b", Cost: 1.0})
	assert.NoError(t, errA)
	assert.NoError(t, errB)

	// Both start from the same virtual time with the same weight/cost
	assert.InDelta(t, vftA, vftB, 0.01, "independent tenants should get similar VFTs")

	// Second task for each advances independently
	vftA2, _ := e.ComputeVFT(&Task{TenantID: "a", Cost: 1.0})
	vftB2, _ := e.ComputeVFT(&Task{TenantID: "b", Cost: 1.0})
	assert.Greater(t, vftA2, vftA, "tenant a VFT must advance")
	assert.Greater(t, vftB2, vftB, "tenant b VFT must advance")
}

func TestComputeVFT_WeightedOrdering(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "heavy", Weight: 4.0}) // higher weight -> smaller VFT increment
	e.RegisterTenant(&tenant.Tenant{ID: "light", Weight: 1.0})

	// Same cost, but heavy tenant has 4x the weight -> its VFT advances 4x slower
	vftHeavy, _ := e.ComputeVFT(&Task{TenantID: "heavy", Cost: 4.0})
	vftLight, _ := e.ComputeVFT(&Task{TenantID: "light", Cost: 4.0})

	// heavy: 0 + 4.0/4.0 = 1.0; light: 0 + 4.0/1.0 = 4.0
	assert.InDelta(t, 1.0, vftHeavy, 0.01, "heavy tenant VFT = cost/weight = 1.0")
	assert.InDelta(t, 4.0, vftLight, 0.01, "light tenant VFT = cost/weight = 4.0")
	assert.Less(t, vftHeavy, vftLight, "higher-weight tenant should get an earlier finish time")
}

// --- Burst ---

func TestComputeVFT_BurstAbsorbed_ReducesVFT(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 0.0})

	vft, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 4.0})
	assert.NoError(t, err)

	// Normal VFT = 4.0/1.0 = 4.0; burst reduces to 4.0 * 0.5 = 2.0
	assert.InDelta(t, 2.0, vft, 0.01, "burst absorbed: VFT should be halved")
}

func TestComputeVFT_BurstExhausted_FallsBackToNormalVFT(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 0.0})
	e.tenants["t1"].burstBucket.Tokens = 0.0 // drain bucket

	vft, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 4.0})
	assert.NoError(t, err)
	assert.InDelta(t, 4.0, vft, 0.01, "exhausted bucket: VFT should be full cost/weight")
}

func TestComputeVFT_BurstInsufficientTokens_FallsBackToNormalVFT(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 0.0})
	e.tenants["t1"].burstBucket.Tokens = 1.0 // not enough for cost=4.0

	vft, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 4.0})
	assert.NoError(t, err)
	assert.InDelta(t, 4.0, vft, 0.01, "insufficient tokens: VFT should be full cost/weight")
}

func TestComputeVFT_BurstLowerVFTThanNoBurst(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "burst-tenant", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 0.0})
	e.RegisterTenant(&tenant.Tenant{ID: "normal-tenant", Weight: 1.0})

	vftBurst, _ := e.ComputeVFT(&Task{TenantID: "burst-tenant", Cost: 4.0})
	vftNormal, _ := e.ComputeVFT(&Task{TenantID: "normal-tenant", Cost: 4.0})

	assert.Less(t, vftBurst, vftNormal,
		"burst tenant should have lower VFT and be scheduled before non-burst tenant")
}

func TestComputeVFT_NoBurstWhenCapacityZero(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0, BurstCapacity: 0.0, BurstRate: 1.0})

	assert.Nil(t, e.tenants["t1"].burstBucket, "burstBucket must be nil when BurstCapacity=0")

	vft, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 4.0})
	assert.NoError(t, err)
	assert.InDelta(t, 4.0, vft, 0.01, "VFT should be full cost/weight with no burst")
}

// --- RegisterTenant ---

func TestRegisterTenant_CreatesNewTenant(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 2.0})

	s, exists := e.tenants["ash"]
	assert.True(t, exists, "tenant should exist after RegisterTenant")
	assert.Equal(t, 2.0, s.weight)
	assert.Nil(t, s.burstBucket, "burst should be nil when burstCapacity=0")
}

func TestRegisterTenant_CreatesNewTenantWithBurst(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 2.0, BurstCapacity: 10.0, BurstRate: 1.0})

	s, exists := e.tenants["ash"]
	assert.True(t, exists)
	assert.Equal(t, 2.0, s.weight)
	assert.NotNil(t, s.burstBucket, "burst bucket must be set when BurstCapacity > 0")
}

func TestRegisterTenant_UpdatesExistingWeightAndBurst(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 1.0})
	_, _ = e.ComputeVFT(&Task{TenantID: "ash", Cost: 1.0}) // put one task in flight

	// Update both weight and enable burst
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 3.0, BurstCapacity: 10.0, BurstRate: 1.0})

	s := e.tenants["ash"]
	assert.Equal(t, 3.0, s.weight, "weight should be updated")
	assert.Equal(t, 3.0, e.vtTracker.vt.ActiveWeight, "activeWeight should adjust for in-flight tasks")
	assert.NotNil(t, s.burstBucket, "burst bucket should be set on update")
}

func TestRegisterTenant_UpdateClearsBurstWhenCapacityZero(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 1.0})
	assert.NotNil(t, e.tenants["ash"].burstBucket)

	// Re-register with BurstCapacity=0 to disable burst.
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 1.0})
	assert.Nil(t, e.tenants["ash"].burstBucket, "burst bucket must be cleared when BurstCapacity=0")
}

func TestRegisterTenant_UpdateDoesNotRecreateBucketIfUnchanged(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 1.0})
	original := e.tenants["ash"].burstBucket

	// Re-register with the same burst params — must reuse the existing bucket.
	e.RegisterTenant(&tenant.Tenant{ID: "ash", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 1.0})
	assert.Same(t, original, e.tenants["ash"].burstBucket,
		"bucket must not be replaced when capacity and rate are unchanged")
}

// --- OnTaskDispatched ---

func TestOnTaskDispatched_RemovesActiveWeight(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})
	_, _ = e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})

	assert.Equal(t, 1.0, e.vtTracker.vt.ActiveWeight, "active weight should be 1.0 after first task")

	e.OnTaskDispatched(&Task{TenantID: "t1"})

	assert.Equal(t, 0.0, e.vtTracker.vt.ActiveWeight, "active weight should be 0 after task dispatched")
}

func TestOnTaskDispatched_Underflow(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})
	_, _ = e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})
	e.OnTaskDispatched(&Task{TenantID: "t1"}) // pendingTasks -> 0

	// Extra dispatch must be a no-op
	e.OnTaskDispatched(&Task{TenantID: "t1"})

	s := e.tenants["t1"]
	assert.Equal(t, int64(0), s.pendingTasks, "pendingTasks must not underflow")
	assert.Equal(t, int64(1), s.totalDispatched, "totalDispatched must not increment on no-op")
	assert.Equal(t, 0.0, e.vtTracker.vt.ActiveWeight, "activeWeight must not go negative")
}

func TestOnTaskDispatched_UnknownTenant(t *testing.T) {
	e := NewVFTEngine()

	// Must not panic or create a tenant
	e.OnTaskDispatched(&Task{TenantID: "ghost"})

	_, exists := e.tenants["ghost"]
	assert.False(t, exists, "unknown tenant must not be created")
}

// --- Concurrency ---

func TestConcurrentComputeVFT_SameTenant(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})

	const n = 200
	vfts := make([]float64, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(i int) {
			defer wg.Done()
			vft, err := e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})
			assert.NoError(t, err)
			vfts[i] = vft
		}(i)
	}
	wg.Wait()

	// All VFTs must be unique and positive (no two tasks get the same slot)
	seen := make(map[float64]bool, n)
	for _, v := range vfts {
		assert.False(t, seen[v], "duplicate VFT: %v", v)
		seen[v] = true
		assert.Greater(t, v, 0.0)
	}
}

func TestConcurrentComputeVFT_MultiTenant(t *testing.T) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "a", Weight: 1.0})

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			_, _ = e.ComputeVFT(&Task{TenantID: "a", Cost: 1.0})
		}(i)
		go func(i int) {
			defer wg.Done()
			e.OnTaskDispatched(&Task{TenantID: "a"})
		}(i)
	}
	wg.Wait()

	// No data race — race detector will catch issues
	assert.GreaterOrEqual(t, e.vtTracker.vt.ActiveWeight, 0.0)
}

// --- Benchmarks ---

// BenchmarkComputeVFT_SingleTenant-4   	 1264330	       934.9 ns/op	       0 B/op	       0 allocs/op
func BenchmarkComputeVFT_SingleTenant(b *testing.B) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})

	for b.Loop() {
		_, _ = e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})
	}
}

// BenchmarkComputeVFT_MultiTenant-4   	 1254723	       939.7 ns/op	       0 B/op	       0 allocs/op
func BenchmarkComputeVFT_MultiTenant(b *testing.B) {
	e := NewVFTEngine()
	tenants := []string{"a", "b", "c", "d"}
	for _, id := range tenants {
		e.RegisterTenant(&tenant.Tenant{ID: id, Weight: 1.0})
	}

	for b.Loop() {
		_, _ = e.ComputeVFT(&Task{TenantID: tenants[b.N%len(tenants)], Cost: 1.0})
	}
}

// BenchmarkComputeVFT_Concurrent-4   	  833314	      1425 ns/op	       0 B/op	       0 allocs/op
func BenchmarkComputeVFT_Concurrent(b *testing.B) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})
		}
	})
}

// BenchmarkOnTaskDispatched-4   	  599037	      2001 ns/op	       0 B/op	       0 allocs/op
func BenchmarkOnTaskDispatched(b *testing.B) {
	e := NewVFTEngine()
	e.RegisterTenant(&tenant.Tenant{ID: "t1", Weight: 1.0})

	for b.Loop() {
		_, _ = e.ComputeVFT(&Task{TenantID: "t1", Cost: 1.0})
		e.OnTaskDispatched(&Task{TenantID: "t1"})
	}
}
