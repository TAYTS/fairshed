package tenant

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newTenant is a test helper that builds a minimal valid Tenant.
func newTenant(id string) Tenant {
	return Tenant{
		ID:     id,
		Weight: 1.0,
	}
}

// ---- Create ----------------------------------------------------------------

func TestCreate_Success(t *testing.T) {
	r := NewRegistry()
	err := r.Create(newTenant("ash"))
	assert.NoError(t, err, "expect no error when creating a new tenant")

	got, err := r.Get("ash")
	assert.NoError(t, err)
	assert.Equal(t, "ash", got.ID)
	assert.False(t, got.CreatedAt.IsZero(), "expect CreatedAt to be set")
}

func TestCreate_SetsCreatedAt(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))

	got, _ := r.Get("ash")
	assert.False(t, got.CreatedAt.IsZero(), "expect CreatedAt to be populated by registry")
}

func TestCreate_Duplicate(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))

	err := r.Create(newTenant("ash"))
	assert.ErrorIs(t, err, ErrTenantAlreadyExists, "expect ErrTenantAlreadyExists on duplicate create")
}

func TestCreate_ZeroWeightUsesDefault(t *testing.T) {
	// Weight=0 is treated as "unset" and defaults to DefaultWeight.
	r := NewRegistry()
	err := r.Create(Tenant{ID: "ash", Weight: 0})
	assert.NoError(t, err, "zero weight should be accepted and defaulted")

	got, _ := r.Get("ash")
	assert.Equal(t, DefaultWeight, got.Weight, "zero weight should be stored as DefaultWeight")
}

func TestCreate_NegativeFields_ReturnsErrInvalidValue(t *testing.T) {
	r := NewRegistry()

	err := r.Create(Tenant{ID: "ash", Weight: -1.0})
	assert.ErrorIs(t, err, ErrInvalidValue, "negative weight must return ErrInvalidValue")

	err = r.Create(Tenant{ID: "ash2", MaxConcurrency: -1})
	assert.ErrorIs(t, err, ErrInvalidValue, "negative MaxConcurrency must return ErrInvalidValue")

	err = r.Create(Tenant{ID: "ash3", BurstCapacity: -1.0})
	assert.ErrorIs(t, err, ErrInvalidValue, "negative BurstCapacity must return ErrInvalidValue")

	err = r.Create(Tenant{ID: "ash4", BurstRate: -1.0})
	assert.ErrorIs(t, err, ErrInvalidValue, "negative BurstRate must return ErrInvalidValue")
}

func TestCreate_InvalidWeight_TooLarge(t *testing.T) {
	r := NewRegistry()
	err := r.Create(Tenant{ID: "ash", Weight: 1001.0})
	assert.ErrorIs(t, err, ErrInvalidWeight, "expect ErrInvalidWeight for weight > 1000")
}

func TestCreate_BoundaryWeights(t *testing.T) {
	r := NewRegistry()

	err := r.Create(Tenant{ID: "min", Weight: MinWeight})
	assert.NoError(t, err, "expect no error at minimum boundary weight")

	err = r.Create(Tenant{ID: "max", Weight: MaxWeight})
	assert.NoError(t, err, "expect no error at maximum boundary weight")
}

func TestCreate_NaNWeight_ReturnsErrInvalidWeight(t *testing.T) {
	r := NewRegistry()
	err := r.Create(Tenant{ID: "ash", Weight: math.NaN()})
	assert.ErrorIs(t, err, ErrInvalidWeight, "NaN weight must return ErrInvalidWeight")
}

func TestCreate_ZeroMaxConcurrencyUsesDefault(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0, MaxConcurrency: 0})

	got, _ := r.Get("ash")
	assert.Equal(t, DefaultConcurrency, got.MaxConcurrency,
		"zero MaxConcurrency should be stored as DefaultConcurrency")
}

func TestCreate_ZeroBurstCapacityUsesNoBurst(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0, BurstCapacity: 0})

	got, _ := r.Get("ash")
	assert.Equal(t, 0.0, got.BurstCapacity)
	assert.Equal(t, 0.0, got.BurstRate, "BurstRate must be 0 when burst is disabled")
}

func TestCreate_PositiveBurstCapacityZeroRateUsesDefault(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0, BurstCapacity: 10.0, BurstRate: 0})

	got, _ := r.Get("ash")
	assert.Equal(t, 10.0, got.BurstCapacity)
	assert.Equal(t, DefaultBurstRate, got.BurstRate, "zero BurstRate with positive BurstCapacity should use DefaultBurstRate")
}

func TestCreate_ExplicitMaxConcurrencyPreserved(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0, MaxConcurrency: 50})

	got, _ := r.Get("ash")
	assert.Equal(t, 50, got.MaxConcurrency,
		"explicit positive MaxConcurrency should be preserved as-is")
}

// ---- Get -------------------------------------------------------------------

func TestGet_Success(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 2.0, MaxConcurrency: 10})

	got, err := r.Get("ash")
	assert.NoError(t, err, "expect no error for existing tenant")
	assert.Equal(t, "ash", got.ID)
	assert.Equal(t, 2.0, got.Weight)
	assert.Equal(t, 10, got.MaxConcurrency)
}

func TestGet_NotFound(t *testing.T) {
	r := NewRegistry()
	_, err := r.Get("nonexistent")
	assert.ErrorIs(t, err, ErrTenantNotFound, "expect ErrTenantNotFound for unknown tenant")
}

func TestGet_ReturnsCopy(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0})

	got, _ := r.Get("ash")
	got.Weight = 999.0 // mutate the returned copy

	// Registry must not be affected
	original, _ := r.Get("ash")
	assert.Equal(t, DefaultWeight, original.Weight, "expect Get to return a copy, not a reference")
}

// ---- UpdateWeight ----------------------------------------------------------

func TestUpdateWeight_Success(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0})

	err := r.UpdateWeight("ash", 3.0)
	assert.NoError(t, err, "expect no error when updating weight")

	got, _ := r.Get("ash")
	assert.Equal(t, 3.0, got.Weight, "expect weight to be updated")
}

func TestUpdateWeight_NotFound(t *testing.T) {
	r := NewRegistry()
	err := r.UpdateWeight("nonexistent", 2.0)
	assert.ErrorIs(t, err, ErrTenantNotFound, "expect ErrTenantNotFound for nonexistent tenant")
}

func TestUpdateWeight_InvalidWeight_Zero(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))

	err := r.UpdateWeight("ash", 0)
	assert.ErrorIs(t, err, ErrInvalidWeight, "expect ErrInvalidWeight for weight=0")
}

func TestUpdateWeight_NegativeWeight(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))

	err := r.UpdateWeight("ash", -5.0)
	assert.ErrorIs(t, err, ErrInvalidWeight, "expect ErrInvalidWeight for negative weight")
}

func TestUpdateWeight_InvalidWeight_TooLarge(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))

	err := r.UpdateWeight("ash", 1001.0)
	assert.ErrorIs(t, err, ErrInvalidWeight, "expect ErrInvalidWeight for weight > 1000")
}

func TestUpdateWeight_ValidationRunsBeforeLookup(t *testing.T) {
	// A nonexistent tenant with an invalid weight — ErrInvalidWeight wins
	// because we validate before we look up to avoid leaking tenant existence.
	r := NewRegistry()
	err := r.UpdateWeight("nonexistent", -1.0)
	assert.ErrorIs(t, err, ErrInvalidWeight, "expect weight validation to run before tenant lookup")
}

func TestUpdateWeight_BoundaryWeights(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))

	assert.NoError(t, r.UpdateWeight("ash", MinWeight), "expect no error at minimum boundary")
	assert.NoError(t, r.UpdateWeight("ash", MaxWeight), "expect no error at maximum boundary")
}

// ---- List ------------------------------------------------------------------

func TestList_Empty(t *testing.T) {
	r := NewRegistry()
	result := r.List()
	assert.Empty(t, result, "expect empty list from empty registry")
}

func TestList_ReturnsAllTenants(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(newTenant("ash"))
	_ = r.Create(newTenant("misty"))
	_ = r.Create(newTenant("brock"))

	result := r.List()
	assert.Len(t, result, 3, "expect all three tenants to be listed")
}

func TestList_ReturnsCopies(t *testing.T) {
	r := NewRegistry()
	_ = r.Create(Tenant{ID: "ash", Weight: 1.0})

	list := r.List()
	list[0].Weight = 999.0 // mutate the returned copy

	// Registry must not be affected
	got, _ := r.Get("ash")
	assert.Equal(t, DefaultWeight, got.Weight, "expect List to return copies, not references")
}

// ---- orDefault (internal utility) -----------------------------------------

func TestOrDefault(t *testing.T) {
	t.Run("float64", func(t *testing.T) {
		tests := []struct {
			name       string
			v          float64
			defaultVal float64
			want       float64
		}{
			{"positive returns v", 0.5, 1.0, 0.5},
			{"zero returns defaultVal", 0.0, 1.0, 1.0},
			{"negative returns defaultVal", -3.0, 1.0, 1.0},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.want, orDefault(tc.v, tc.defaultVal))
			})
		}
	})

	t.Run("int", func(t *testing.T) {
		tests := []struct {
			name       string
			v          int
			defaultVal int
			want       int
		}{
			{"positive returns v", 50, 100, 50},
			{"zero returns defaultVal", 0, 100, 100},
			{"negative returns defaultVal", -1, 100, 100},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.want, orDefault(tc.v, tc.defaultVal))
			})
		}
	})
}
