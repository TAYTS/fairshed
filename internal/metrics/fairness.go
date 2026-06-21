package metrics

// JainsFairnessIndex computes Jain's fairness index over per-tenant worker-seconds.
// J = (Σ xᵢ)² / (n · Σ xᵢ²)
//
// where:
//
//	xᵢ = (tasks dispatched for tenant i) / (weight of tenant i)
//	n  = number of tenants
//
// Range [1/n, 1.0] — 1.0 is perfectly fair.
func JainsFairnessIndex(workerSeconds map[string]float64) float64 {
	if len(workerSeconds) == 0 {
		return 1.0
	}

	var sum, sumSq float64
	n := float64(len(workerSeconds))
	for _, x := range workerSeconds {
		sum += x
		sumSq += x * x
	}

	if sumSq == 0 {
		return 1.0
	}

	return (sum * sum) / (n * sumSq)
}
