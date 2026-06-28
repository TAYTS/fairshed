package api

import (
	"net/http"

	"github.com/tayts/fairsched/internal/dispatcher"
	"github.com/tayts/fairsched/internal/metrics"
	"github.com/tayts/fairsched/internal/store"
)

type StatsHandler struct {
	dispatcher *dispatcher.Dispatcher
	store      *store.TaskStore
}

func NewStatsHandler(
	dispatcher *dispatcher.Dispatcher,
	store *store.TaskStore,
) HandlerRegistry {
	h := &StatsHandler{
		dispatcher: dispatcher,
		store:      store,
	}
	return h.getRegistry()
}

func (h *StatsHandler) getRegistry() HandlerRegistry {
	return func(mux *http.ServeMux) {
		mux.HandleFunc("GET /v1/stats", h.getStats)
	}
}

// StatsResponse is the JSON body returned by GET /v1/stats.
type StatsResponse struct {
	QueueDepth         int                    `json:"queue_depth"`
	GlobalVirtualTime  float64                `json:"global_virtual_time"`
	JainsFairnessIndex float64                `json:"jains_fairness_index"`
	Tenants            map[string]TenantStats `json:"tenants"`
}

// TenantStats is per-tenant scheduling state included in StatsResponse.
type TenantStats struct {
	Weight          float64 `json:"weight"`
	Pending         int     `json:"pending"`          // tasks currently in queue
	Active          int     `json:"active"`           // tasks dispatched but not yet completed
	DispatchedTotal int64   `json:"dispatched_total"` // lifetime dispatch count
	LastVFT         float64 `json:"last_vft"`
	WorkerSeconds   float64 `json:"worker_seconds"` // total CPU-seconds consumed (completed + failed)
}

func (h *StatsHandler) getStats(w http.ResponseWriter, r *http.Request) {
	engineStats := h.dispatcher.Stats()
	storeStats := h.store.TenantStats()

	tenants := make(map[string]TenantStats, len(engineStats.Tenants))

	// Weight-normalised worker-seconds for Jain's index.
	// Dividing by weight means a tenant with weight 2.0 that receives 2x throughput
	// scores the same as a weight 1.0 tenant. Jain's = 1.0 means the scheduler
	// is allocating capacity in exact proportion to weights.
	normalised := make(map[string]float64, len(engineStats.Tenants))
	for _, ts := range engineStats.Tenants {
		st := storeStats[ts.TenantID]
		tenants[ts.TenantID] = TenantStats{
			Weight:          ts.Weight,
			Pending:         st.PendingCount,
			Active:          st.ActiveCount,
			DispatchedTotal: st.DispatchedTotal,
			LastVFT:         ts.LastVFT,
			WorkerSeconds:   st.WorkerSeconds,
		}
		if ts.Weight > 0 {
			normalised[ts.TenantID] = st.WorkerSeconds / ts.Weight
		}
	}

	writeJSON(w, http.StatusOK, StatsResponse{
		QueueDepth:         h.dispatcher.Len(),
		GlobalVirtualTime:  engineStats.VirtualTime,
		JainsFairnessIndex: metrics.JainsFairnessIndex(normalised),
		Tenants:            tenants,
	})
}
