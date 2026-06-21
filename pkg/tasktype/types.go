package tasktype

const (
	Snorlax  = "snorlax"  // cost 5.0s — the noisy neighbor
	Pikachu  = "pikachu"  // cost 0.5s — the victim small tenant
	Eevee    = "eevee"    // cost 1.0s — steady mid-weight tenant
	Magikarp = "magikarp" // cost 0.1s — trivial high-volume tasks
)

var DefaultCost = map[string]float64{
	Snorlax:  5.0,
	Pikachu:  0.5,
	Eevee:    1.0,
	Magikarp: 0.1,
}

// BenchmarkCost maps each task type to a 10x scaled-down cost for use in
// benchmarks and e2e tests, keeping overall runtime under a few seconds.
var BenchmarkCost = map[string]float64{
	Snorlax:  0.5,
	Pikachu:  0.05,
	Eevee:    0.1,
	Magikarp: 0.01,
}
