// Package metrics defines the Prometheus collectors exposed by the lbsync
// agent and a small facade used by the engine and modules to report activity.
package metrics

import "github.com/prometheus/client_golang/prometheus"

const namespace = "lbsync"

// Metrics holds every collector exposed by the agent.
type Metrics struct {
	ClusterMembers prometheus.Gauge
	ModuleEnabled  *prometheus.GaugeVec
	AppliedTS      *prometheus.GaugeVec
	NotAfter       *prometheus.GaugeVec
	PublishTotal   *prometheus.CounterVec
	ApplyTotal     *prometheus.CounterVec
	VerifyErrors   *prometheus.CounterVec
	ReloadErrors   *prometheus.CounterVec
	RollbackTotal  *prometheus.CounterVec
	ReconcileDur   *prometheus.HistogramVec
}

// New builds the metric set and registers it with reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ClusterMembers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_members",
			Help:      "Number of members currently in the Olric cluster.",
		}),
		ModuleEnabled: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "module_enabled",
			Help:      "1 if a module is enabled on this node, else 0.",
		}, []string{"module"}),
		AppliedTS: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "applied_timestamp_seconds",
			Help:      "Unix timestamp of the last successful apply for a resource.",
		}, []string{"kind", "key"}),
		NotAfter: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "not_after_seconds",
			Help:      "Certificate NotAfter (Unix seconds) of the served certificate, for cert resources.",
		}, []string{"kind", "key"}),
		PublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_total",
			Help:      "Number of times this node published a newer resource to the cluster.",
		}, []string{"kind"}),
		ApplyTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "apply_total",
			Help:      "Number of times this node applied a resource locally.",
		}, []string{"kind"}),
		VerifyErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "verify_errors_total",
			Help:      "Number of failed config verify commands before reload.",
		}, []string{"kind"}),
		ReloadErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "reload_errors_total",
			Help:      "Number of failed reload commands.",
		}, []string{"kind"}),
		RollbackTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rollback_total",
			Help:      "Number of times applied files were rolled back after a verify failure.",
		}, []string{"kind"}),
		ReconcileDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "reconcile_duration_seconds",
			Help:      "Duration of a single reconcile tick per kind.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"kind"}),
	}

	reg.MustRegister(
		m.ClusterMembers,
		m.ModuleEnabled,
		m.AppliedTS,
		m.NotAfter,
		m.PublishTotal,
		m.ApplyTotal,
		m.VerifyErrors,
		m.ReloadErrors,
		m.RollbackTotal,
		m.ReconcileDur,
	)
	return m
}
