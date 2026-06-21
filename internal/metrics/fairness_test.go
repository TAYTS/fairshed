package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJainsFairnessIndex_EmptyInput(t *testing.T) {
	assert.Equal(t, 1.0, JainsFairnessIndex(nil))
	assert.Equal(t, 1.0, JainsFairnessIndex(map[string]float64{}))
}

func TestJainsFairnessIndex_AllZeroValues(t *testing.T) {
	// sumSq == 0 guard — no work done yet, treat as perfectly fair
	assert.Equal(t, 1.0, JainsFairnessIndex(map[string]float64{
		"a": 0.0,
		"b": 0.0,
	}))
}

func TestJainsFairnessIndex_SingleTenant(t *testing.T) {
	// J = x² / (1 · x²) = 1.0 always
	assert.Equal(t, 1.0, JainsFairnessIndex(map[string]float64{"a": 42.0}))
}

func TestJainsFairnessIndex_PerfectlyFair(t *testing.T) {
	// Equal allocation across all tenants gives J = 1.0
	assert.InDelta(t, 1.0, JainsFairnessIndex(map[string]float64{
		"a": 10.0,
		"b": 10.0,
		"c": 10.0,
	}), 1e-9)
}

func TestJainsFairnessIndex_TotallyUnfair(t *testing.T) {
	// One tenant gets everything, n-1 get nothing: J approaches 1/n
	// With n=3: J = 100² / (3 · 100²) = 1/3
	result := JainsFairnessIndex(map[string]float64{
		"hog":    100.0,
		"victim": 0.0,
		"idle":   0.0,
	})
	assert.InDelta(t, 1.0/3.0, result, 1e-9)
}

func TestJainsFairnessIndex_TwoTenantsUnequal(t *testing.T) {
	// J = (a+b)² / (2·(a²+b²)); must be in (0.5, 1.0)
	result := JainsFairnessIndex(map[string]float64{
		"a": 90.0,
		"b": 10.0,
	})
	assert.Greater(t, result, 0.5)
	assert.Less(t, result, 1.0)
}
