package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ControlPlaneMetrics holds all control plane Prometheus metrics.
type ControlPlaneMetrics struct {
	RequestsTotal       *prometheus.CounterVec
	RequestDuration     *prometheus.HistogramVec
	SchemasTotal        *prometheus.GaugeVec
	SchemaVersionsTotal *prometheus.GaugeVec
	StreamsTotal        *prometheus.GaugeVec
	PipelinesTotal      *prometheus.GaugeVec
	DBConnectionsActive prometheus.Gauge
	DBConnectionsIdle   prometheus.Gauge
	CacheHitsTotal      *prometheus.CounterVec
	CacheMissesTotal    *prometheus.CounterVec
	ConfigReloadsTotal  prometheus.Counter
}

// NewControlPlaneMetrics registers and returns all control plane metrics.
func NewControlPlaneMetrics() *ControlPlaneMetrics {
	return &ControlPlaneMetrics{
		RequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_control_requests_total",
			Help: "Total control plane API requests",
		}, []string{"endpoint", "method", "status"}),

		RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_control_request_duration_seconds",
			Help:    "Control plane request latency",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		}, []string{"endpoint", "method"}),

		SchemasTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_schemas_total",
			Help: "Total schemas by namespace",
		}, []string{"namespace"}),

		SchemaVersionsTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_schema_versions_total",
			Help: "Versions per schema",
		}, []string{"schema_id"}),

		StreamsTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_streams_total",
			Help: "Total streams by status",
		}, []string{"status"}),

		PipelinesTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_pipelines_total",
			Help: "Total pipelines by status",
		}, []string{"status"}),

		DBConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_db_connections_active",
			Help: "Active database connections",
		}),

		DBConnectionsIdle: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_db_connections_idle",
			Help: "Idle database connections",
		}),

		CacheHitsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_cache_hits_total",
			Help: "Cache hits by type",
		}, []string{"cache_type"}),

		CacheMissesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_cache_misses_total",
			Help: "Cache misses by type",
		}, []string{"cache_type"}),

		ConfigReloadsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "ttdust_config_reloads_total",
			Help: "Configuration reload events",
		}),
	}
}
