package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- ComputeVFT ---

func TestComputeVFT_AutoCreatesTenant(t *testing.T) {
	e := NewVFTEngine()
	task := &Task{TenantId: "new-tenant", Cost: 1.0}

	e.ComputeVFT(task)

	tenant, exists := e.tenants["new-tenant"]
	assert.True(t, exists, "tenant should be auto-created")
	assert.Equal(t, 1.0, tenant.Weight, "default weight should be 1.0")
}

func TestComputeVFT_Formula(t *testing.T) {
	e := NewVFTEngine()

	// With activeWeight=0 virtual time stays at 0, so VFT = 0 + cost/weight
	vft1 := e.ComputeVFT(&Task{TenantId: "t1", Cost: 2.0})
	assert.Equal(t, 2.0, vft1, "first VFT = 0 + 2.0/1.0")

	// Second task: lastVFT=2.0, vt≈0 → vst=max(0,2.0)=2.0, VFT=2.0+3.0=5.0
	vft2 := e.ComputeVFT(&Task{TenantId: "t1", Cost: 3.0})
	assert.Equal(t, 5.0, vft2, "second VFT = max(vt, 2.0) + 3.0/1.0")

	assert.Greater(t, vft2, vft1, "VFTs must be monotonically increasing")
}

func TestComputeVFT_VTAhead(t *testing.T) {
	e := NewVFTEngine()

	// Manually push virtual time ahead by adding active weight and advancing
	e.vtTracker.UpdateActiveWeight(1.0)
	e.vtTracker.Advance()

	// Force lastVFT to a value behind current virtual time
	e.ComputeVFT(&Task{TenantId: "t1", Cost: 1.0}) // establishes lastVFT
	e.OnTaskDispatched(&Task{TenantId: "t1"})      // clear pending so activeWeight resets

	// Set lastVFT=0 directly to simulate vt > lastVFT
	e.tenants["t1"].LastVFT = 0

	vt := e.vtTracker.Get()
	vft := e.ComputeVFT(&Task{TenantId: "t1", Cost: 1.0})

	// vst = max(vt, 0) = vt; VFT = vt + 1.0
	assert.InDelta(t, vt+1.0, vft, 0.01, "when vt > lastVFT, vt is used as base")
}

func TestComputeVFT_MultiTenant(t *testing.T) {
	e := NewVFTEngine()

	vftA := e.ComputeVFT(&Task{TenantId: "a", Cost: 1.0})
	vftB := e.ComputeVFT(&Task{TenantId: "b", Cost: 1.0})

	// Both start from the same virtual time with the same weight/cost
	assert.InDelta(t, vftA, vftB, 0.01, "independent tenants should get similar VFTs")

	// Second task for each advances independently
	vftA2 := e.ComputeVFT(&Task{TenantId: "a", Cost: 1.0})
	vftB2 := e.ComputeVFT(&Task{TenantId: "b", Cost: 1.0})
	assert.Greater(t, vftA2, vftA, "tenant a VFT must advance")
	assert.Greater(t, vftB2, vftB, "tenant b VFT must advance")
}

func TestComputeVFT_WeightedOrdering(t *testing.T) {
	e := NewVFTEngine()
	e.SetTenantWeight("heavy", 4.0) // higher weight -> lower cost per unit -> smaller VFT increment
	e.SetTenantWeight("light", 1.0)

	// Same cost, but heavy tenant has 4x the weight -> its VFT advances 4x slower
	vftHeavy := e.ComputeVFT(&Task{TenantId: "heavy", Cost: 4.0})
	vftLight := e.ComputeVFT(&Task{TenantId: "light", Cost: 4.0})

	// heavy: 0 + 4.0/4.0 = 1.0; light: 0 + 4.0/1.0 = 4.0
	assert.InDelta(t, 1.0, vftHeavy, 0.01, "heavy tenant VFT = cost/weight = 1.0")
	assert.InDelta(t, 4.0, vftLight, 0.01, "light tenant VFT = cost/weight = 4.0")
	assert.Less(t, vftHeavy, vftLight, "higher-weight tenant should get an earlier finish time")
}

// --- OnTaskDispatched ---

func TestOnTaskDispatched_RemovesActiveWeight(t *testing.T) {
	e := NewVFTEngine()
	e.ComputeVFT(&Task{TenantId: "t1", Cost: 1.0})

	assert.Equal(t, 1.0, e.vtTracker.vt.ActiveWeight, "active weight should be 1.0 after first task")

	e.OnTaskDispatched(&Task{TenantId: "t1"})

	assert.Equal(t, 0.0, e.vtTracker.vt.ActiveWeight, "active weight should be 0 after task dispatched")
}

func TestOnTaskDispatched_Underflow(t *testing.T) {
	e := NewVFTEngine()
	e.ComputeVFT(&Task{TenantId: "t1", Cost: 1.0})
	e.OnTaskDispatched(&Task{TenantId: "t1"}) // pendingTasks → 0

	// Extra dispatch must be a no-op
	e.OnTaskDispatched(&Task{TenantId: "t1"})

	tenant := e.tenants["t1"]
	assert.Equal(t, int64(0), tenant.PendingTasks, "pendingTasks must not underflow")
	assert.Equal(t, int64(1), tenant.TotalDispatched, "totalDispatched must not increment on no-op")
	assert.Equal(t, 0.0, e.vtTracker.vt.ActiveWeight, "activeWeight must not go negative")
}

func TestOnTaskDispatched_UnknownTenant(t *testing.T) {
	e := NewVFTEngine()

	// Must not panic or create a tenant
	e.OnTaskDispatched(&Task{TenantId: "ghost"})

	_, exists := e.tenants["ghost"]
	assert.False(t, exists, "unknown tenant must not be created")
}

// --- SetTenantWeight ---

func TestSetTenantWeight_Clamped(t *testing.T) {
	e := NewVFTEngine()

	e.SetTenantWeight("t1", 0.0)
	assert.Equal(t, 0.001, e.tenants["t1"].Weight, "below-min weight should clamp to 0.001")

	e.SetTenantWeight("t2", 9999.0)
	assert.Equal(t, 1000.0, e.tenants["t2"].Weight, "above-max weight should clamp to 1000.0")
}

func TestSetTenantWeight_AdjustsActiveWeight(t *testing.T) {
	e := NewVFTEngine()
	e.ComputeVFT(&Task{TenantId: "t1", Cost: 1.0}) // weight=1.0, activeWeight=1.0

	e.SetTenantWeight("t1", 3.0)

	assert.Equal(t, 3.0, e.tenants["t1"].Weight)
	assert.Equal(t, 3.0, e.vtTracker.vt.ActiveWeight, "activeWeight should reflect new weight")
}

func TestSetTenantWeight_IdleTenantNoActiveWeightChange(t *testing.T) {
	e := NewVFTEngine()
	e.SetTenantWeight("t1", 5.0) // tenant has no pending tasks

	assert.Equal(t, 5.0, e.tenants["t1"].Weight)
	assert.Equal(t, 0.0, e.vtTracker.vt.ActiveWeight, "idle tenant must not affect activeWeight")
}

// --- Concurrency ---

func TestConcurrentComputeVFT_SameTenant(t *testing.T) {
	e := NewVFTEngine()

	const n = 200
	vfts := make([]float64, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(i int) {
			defer wg.Done()
			vfts[i] = e.ComputeVFT(&Task{TenantId: "t1", Cost: 1.0})
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

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			e.ComputeVFT(&Task{TenantId: "a", Cost: 1.0})
		}(i)
		go func(i int) {
			defer wg.Done()
			e.OnTaskDispatched(&Task{TenantId: "a"})
		}(i)
	}
	wg.Wait()

	// No data race — race detector will catch issues
	assert.GreaterOrEqual(t, e.vtTracker.vt.ActiveWeight, 0.0)
}
