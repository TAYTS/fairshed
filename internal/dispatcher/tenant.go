package dispatcher

import "cmp"

const (
	defaultWeight    float64 = 1.0
	defaultLimit     int     = 100
	defaultBurstRate float64 = 1.0
)

type TenantConfig struct {
	Weight        float64 // fairness weight (default 1.0)
	Limit         int     // max in-flight tasks (default 100)
	BurstCapacity float64 // token bucket capacity; 0 disables burst
	BurstRate     float64 // token refill rate per second (default 1.0)
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
