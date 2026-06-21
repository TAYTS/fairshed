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

	// Build the per-tenant worker-seconds map for the Jain's index calculation.
	workerSeconds := make(map[string]float64, len(storeStats))
	for id, st := range storeStats {
		workerSeconds[id] = st.WorkerSeconds
	}

	tenants := make(map[string]TenantStats, len(engineStats.Tenants))
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
	}

	writeJSON(w, http.StatusOK, StatsResponse{
		QueueDepth:         h.dispatcher.Len(),
		GlobalVirtualTime:  engineStats.VirtualTime,
		JainsFairnessIndex: metrics.JainsFairnessIndex(workerSeconds),
		Tenants:            tenants,
	})
}
