package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- NewTenant ---

func TestNewTenant_Valid(t *testing.T) {
	cases := []struct {
		id     string
		weight float64
	}{
		{"t1", 0.001},
		{"tenant2", 1.0},
		{"my-tenant", 500.0},
		{"abc", 1000.0},
	}
	for _, tc := range cases {
		tenant, err := NewTenant(tc.id, tc.weight)
		require.NoError(t, err)
		assert.Equal(t, tc.id, tenant.ID)
		assert.Equal(t, tc.weight, tenant.GetWeight())
		assert.Equal(t, int64(0), tenant.GetPendingTasks())
		assert.Equal(t, float64(0), tenant.GetLastVFT())
	}
}

func TestNewTenant_OutOfRange(t *testing.T) {
	for _, weight := range []float64{0.0, 0.0009, 1000.001, -1.0, 2000.0} {
		tenant, err := NewTenant("t1", weight)
		assert.Error(t, err)
		assert.Nil(t, tenant)
	}
}

// --- UpdateWeight ---

func TestUpdateWeight_Valid(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	for _, w := range []float64{0.001, 1.0, 500.0, 1000.0} {
		require.NoError(t, tenant.UpdateWeight(w))
		assert.Equal(t, w, tenant.GetWeight())
	}
}

func TestUpdateWeight_OutOfRange(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	for _, w := range []float64{0.0, 0.0009, 1000.001, -1.0} {
		assert.Error(t, tenant.UpdateWeight(w))
	}
	// Weight must remain unchanged after all rejected updates.
	assert.Equal(t, 1.0, tenant.GetWeight())
}

// --- VFT ---

func TestSetAndGetLastVFT(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	tenant.SetLastVFT(42.5)
	assert.Equal(t, 42.5, tenant.GetLastVFT())
}

// --- NewTask ---

func TestNewTask_IncreasesPendingCount(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	task := tenant.NewTask(2.0)
	require.NotNil(t, task)
	assert.Equal(t, int64(1), tenant.GetPendingTasks())
	assert.Equal(t, "t1", task.tenantID)
	assert.Equal(t, 2.0, task.cost)
	assert.False(t, task.arrived.IsZero())
}

func TestNewTask_IDIsUnique(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	ids := make(map[string]bool)
	for range 10 {
		task := tenant.NewTask(1.0)
		assert.False(t, ids[task.id], "duplicate task id: %s", task.id)
		ids[task.id] = true
	}
}

// --- CompleteTask ---

func TestCompleteTask(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	tenant.NewTask(1.0)
	tenant.NewTask(1.0)
	assert.Equal(t, int64(2), tenant.GetPendingTasks())

	tenant.CompleteTask()
	assert.Equal(t, int64(1), tenant.GetPendingTasks())
	assert.Equal(t, int64(1), tenant.totalDispatched)
}

func TestCompleteTask_NoUnderflow(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	// Completing with no pending tasks must not underflow.
	tenant.CompleteTask()
	assert.Equal(t, int64(0), tenant.GetPendingTasks())
	assert.Equal(t, int64(0), tenant.totalDispatched)
}

// --- Concurrency ---

func TestConcurrentUpdateWeight(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = tenant.UpdateWeight(0.001 + float64(i%1000))
		}(i)
	}
	wg.Wait()

	w := tenant.GetWeight()
	assert.GreaterOrEqual(t, w, 0.001)
	assert.LessOrEqual(t, w, 1000.0)
}

func TestConcurrentSetGetVFT(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				tenant.SetLastVFT(float64(i))
			} else {
				_ = tenant.GetLastVFT()
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrentNewTaskAndComplete(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	const n = 500
	var wg sync.WaitGroup
	wg.Add(n * 2)

	for range n {
		go func() { defer wg.Done(); tenant.NewTask(1.0) }()
	}
	for range n {
		go func() { defer wg.Done(); tenant.CompleteTask() }()
	}
	wg.Wait()

	// pending >= 0 always; exact value depends on scheduling order.
	assert.GreaterOrEqual(t, tenant.GetPendingTasks(), int64(0))
}

func TestConcurrentMixedOps(t *testing.T) {
	tenant, err := NewTenant("t1", 1.0)
	require.NoError(t, err)

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n * 4)
	for i := range n {
		go func(i int) { defer wg.Done(); _ = tenant.UpdateWeight(float64(i%1000) + 0.001) }(i)
		go func(i int) { defer wg.Done(); tenant.SetLastVFT(float64(i)) }(i)
		go func() { defer wg.Done(); tenant.NewTask(1.0) }()
		go func() { defer wg.Done(); tenant.CompleteTask() }()
	}
	wg.Wait()
}
