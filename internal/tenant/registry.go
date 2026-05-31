package tenant

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrTenantNotFound      = errors.New("tenant not found")
	ErrTenantAlreadyExists = errors.New("tenant already exists")
	ErrInvalidWeight       = errors.New("weight must be in range [0.001, 1000]")
	ErrInvalidValue        = errors.New("invalid configuration value")
)

const (
	MinWeight          float64 = 0.001
	MaxWeight          float64 = 1000.0
	DefaultConcurrency int     = 100
	DefaultWeight      float64 = 1.0
	DefaultBurstRate   float64 = 1.0
)

// Tenant holds the domain configuration for a single tenant.
// This is the canonical representation used by the HTTP API, registry, and dispatcher.
type Tenant struct {
	ID             string
	Weight         float64 // Fairness weight; range [0.001, 1000], default 1.0
	MaxConcurrency int     // Max in-flight tasks (default 100)
	BurstCapacity  float64 // Token bucket capacity; 0 disables burst
	BurstRate      float64 // Token refill rate per second
	CreatedAt      time.Time
}

// Registry is a thread-safe in-memory store of tenant configurations.
type Registry struct {
	mu      sync.RWMutex
	tenants map[string]*Tenant // tenantID -> Tenant
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		tenants: make(map[string]*Tenant),
	}
}

// Create registers a new tenant.
// Returns ErrTenantAlreadyExists if a tenant with the same ID has registered.
// Returns ErrInvalidWeight if the weight is outside [0.001, 1000].
// Returns ErrInvalidValue if any configuration value is negative.
func (r *Registry) Create(t Tenant) error {
	if t.Weight < 0 || t.MaxConcurrency < 0 || t.BurstCapacity < 0 || t.BurstRate < 0 {
		return fmt.Errorf("tenant %s: %w", t.ID, ErrInvalidValue)
	}

	t.Weight = orDefault(t.Weight, DefaultWeight)
	t.MaxConcurrency = orDefault(t.MaxConcurrency, DefaultConcurrency)
	if t.BurstCapacity > 0 {
		t.BurstRate = orDefault(t.BurstRate, DefaultBurstRate)
	}

	if err := validateWeight(t.Weight); err != nil {
		return fmt.Errorf("tenant %s: %w", t.ID, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.tenants[t.ID]; ok {
		return fmt.Errorf("tenant %s: %w", t.ID, ErrTenantAlreadyExists)
	}

	t.CreatedAt = time.Now()
	r.tenants[t.ID] = &t
	return nil
}

// Get retrieves a tenant by ID.
// Returns ErrTenantNotFound if the tenant does not exist.
func (r *Registry) Get(id string) (*Tenant, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.tenants[id]
	if !ok {
		return nil, fmt.Errorf("tenant %s: %w", id, ErrTenantNotFound)
	}

	// Return a copy to prevent callers from mutating registry state.
	cp := *t
	return &cp, nil
}

// UpdateWeight changes a tenant's fairness weight.
// Returns ErrTenantNotFound if the tenant does not exist.
// Returns ErrInvalidWeight if the weight is outside [0.001, 1000].
func (r *Registry) UpdateWeight(id string, weight float64) error {
	if err := validateWeight(weight); err != nil {
		return fmt.Errorf("tenant %s: %w", id, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.tenants[id]
	if !ok {
		return fmt.Errorf("tenant %s: %w", id, ErrTenantNotFound)
	}

	t.Weight = weight
	return nil
}

// List returns a snapshot of all registered tenants.
// The returned slice is a copy — callers may not modify registry state through it.
func (r *Registry) List() []*Tenant {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Tenant, 0, len(r.tenants))
	for _, t := range r.tenants {
		cp := *t
		result = append(result, &cp)
	}
	return result
}

// validateWeight returns ErrInvalidWeight if weight is outside [MinWeight, MaxWeight].
func validateWeight(weight float64) error {
	if math.IsNaN(weight) || weight < MinWeight || weight > MaxWeight {
		return ErrInvalidWeight
	}
	return nil
}

// orDefault returns v if v is positive, otherwise returns def.
// Used to apply sentinel defaults for zero-valued config fields.
func orDefault[T cmp.Ordered](v, def T) T {
	var zero T
	if v <= zero {
		return def
	}
	return v
}
