package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Histogram bucket presets per observability.md §2.2
var (
	LatencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}
	QueryBuckets   = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0}
	RecordBuckets  = []float64{1, 10, 100, 500, 1000, 5000, 10000}
)

// DataPlaneMetrics holds all data plane Prometheus metrics per observability.md §2.
type DataPlaneMetrics struct {
	// --- Ingestion metrics (§2.2) ---
	IngestionRequestsTotal        *prometheus.CounterVec
	IngestionRecordsTotal         *prometheus.CounterVec
	IngestionBytesTotal           *prometheus.CounterVec
	IngestionLatency              *prometheus.HistogramVec
	IngestionValidationErrorTotal *prometheus.CounterVec
	IngestionBufferSizeBytes      *prometheus.GaugeVec
	IngestionBufferRecords        *prometheus.GaugeVec
	IngestionBackpressureTotal    *prometheus.CounterVec

	// --- Processing metrics (§2.3) ---
	ProcessingRecordsTotal      *prometheus.CounterVec
	ProcessingLatency           *prometheus.HistogramVec
	ProcessingStageLatency      *prometheus.HistogramVec
	ProcessingDLQRecordsTotal   *prometheus.CounterVec
	ProcessingErrorsTotal       *prometheus.CounterVec

	// --- Storage metrics (§2.4) ---
	WALWritesTotal          *prometheus.CounterVec
	WALBytesTotal           *prometheus.CounterVec
	WALSegmentsActive       *prometheus.GaugeVec
	WALFlushLatency         *prometheus.HistogramVec
	S3OperationsTotal       *prometheus.CounterVec
	S3Latency               *prometheus.HistogramVec
	S3BytesTotal            *prometheus.CounterVec
	StorageFilesTotal       *prometheus.GaugeVec
	StorageBytesTotal       *prometheus.GaugeVec
	CompactionRunsTotal     *prometheus.CounterVec

	// --- Query metrics (§2.5) ---
	QueryRequestsTotal       *prometheus.CounterVec
	QueryLatency             *prometheus.HistogramVec
	QueryRecordsReturned     *prometheus.HistogramVec
	QueryPartitionsScanned   *prometheus.HistogramVec
	QueryCacheHitsTotal      *prometheus.CounterVec
	QueryCacheMissesTotal    *prometheus.CounterVec

	// --- Infrastructure metrics (§2.6) ---
	PGConnectionsActive     prometheus.Gauge
	PGConnectionsIdle       prometheus.Gauge
	PGQueryLatency          *prometheus.HistogramVec
	RedisConnectionsActive  prometheus.Gauge
	RedisLatency            *prometheus.HistogramVec
	GRPCConnectionsActive   prometheus.Gauge
	HTTPConnectionsActive   prometheus.Gauge
}

// NewDataPlaneMetrics registers and returns all data plane metrics.
func NewDataPlaneMetrics() *DataPlaneMetrics {
	return &DataPlaneMetrics{
		// Ingestion
		IngestionRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_ingestion_requests_total",
			Help: "Total ingestion requests",
		}, []string{"stream", "status"}),

		IngestionRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_ingestion_records_total",
			Help: "Total records ingested",
		}, []string{"stream", "status"}),

		IngestionBytesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_ingestion_bytes_total",
			Help: "Total bytes ingested",
		}, []string{"stream"}),

		IngestionLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_ingestion_latency_seconds",
			Help:    "End-to-end ingestion latency",
			Buckets: LatencyBuckets,
		}, []string{"stream"}),

		IngestionValidationErrorTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_ingestion_validation_errors_total",
			Help: "Validation failures",
		}, []string{"stream", "error_type"}),

		IngestionBufferSizeBytes: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_ingestion_buffer_size_bytes",
			Help: "Current buffer size in bytes",
		}, []string{"stream"}),

		IngestionBufferRecords: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_ingestion_buffer_records",
			Help: "Records in buffer",
		}, []string{"stream"}),

		IngestionBackpressureTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_ingestion_backpressure_total",
			Help: "Backpressure events",
		}, []string{"stream"}),

		// Processing
		ProcessingRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_processing_records_total",
			Help: "Records processed",
		}, []string{"pipeline", "stage", "status"}),

		ProcessingLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_processing_latency_seconds",
			Help:    "Pipeline execution time",
			Buckets: LatencyBuckets,
		}, []string{"pipeline"}),

		ProcessingStageLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_processing_stage_latency_seconds",
			Help:    "Per-stage latency",
			Buckets: LatencyBuckets,
		}, []string{"pipeline", "stage"}),

		ProcessingDLQRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_processing_dlq_records_total",
			Help: "Records sent to DLQ",
		}, []string{"stream"}),

		ProcessingErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_processing_errors_total",
			Help: "Processing errors",
		}, []string{"pipeline", "stage", "error_type"}),

		// Storage
		WALWritesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_wal_writes_total",
			Help: "WAL write operations",
		}, []string{"stream"}),

		WALBytesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_wal_bytes_total",
			Help: "WAL bytes written",
		}, []string{"stream"}),

		WALSegmentsActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_wal_segments_active",
			Help: "Active WAL segments",
		}, []string{"stream"}),

		WALFlushLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_wal_flush_latency_seconds",
			Help:    "WAL flush time",
			Buckets: LatencyBuckets,
		}, []string{"stream"}),

		S3OperationsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_s3_operations_total",
			Help: "S3 operations",
		}, []string{"operation", "status"}),

		S3Latency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_s3_latency_seconds",
			Help:    "S3 operation latency",
			Buckets: LatencyBuckets,
		}, []string{"operation"}),

		S3BytesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_s3_bytes_total",
			Help: "S3 bytes transferred",
		}, []string{"operation"}),

		StorageFilesTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_storage_files_total",
			Help: "Total Parquet files",
		}, []string{"stream"}),

		StorageBytesTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ttdust_storage_bytes_total",
			Help: "Total storage bytes",
		}, []string{"stream"}),

		CompactionRunsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_compaction_runs_total",
			Help: "Compaction executions",
		}, []string{"stream", "status"}),

		// Query
		QueryRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_query_requests_total",
			Help: "Query requests",
		}, []string{"stream", "type", "status"}),

		QueryLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_query_latency_seconds",
			Help:    "Query latency",
			Buckets: QueryBuckets,
		}, []string{"stream", "type"}),

		QueryRecordsReturned: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_query_records_returned",
			Help:    "Records per query",
			Buckets: RecordBuckets,
		}, []string{"stream", "type"}),

		QueryPartitionsScanned: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_query_partitions_scanned",
			Help:    "Partitions scanned per query",
			Buckets: []float64{1, 5, 10, 25, 50, 100},
		}, []string{"stream"}),

		QueryCacheHitsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_query_cache_hits_total",
			Help: "Cache hits",
		}, []string{"cache_level"}),

		QueryCacheMissesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_query_cache_misses_total",
			Help: "Cache misses",
		}, []string{"cache_level"}),

		// Infrastructure
		PGConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_pg_connections_active",
			Help: "Active PostgreSQL connections",
		}),

		PGConnectionsIdle: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_pg_connections_idle",
			Help: "Idle PostgreSQL connections",
		}),

		PGQueryLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_pg_query_latency_seconds",
			Help:    "PostgreSQL query latency",
			Buckets: LatencyBuckets,
		}, []string{"query_type"}),

		RedisConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_redis_connections_active",
			Help: "Active Redis connections",
		}),

		RedisLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ttdust_redis_latency_seconds",
			Help:    "Redis command latency",
			Buckets: LatencyBuckets,
		}, []string{"command"}),

		GRPCConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_grpc_connections_active",
			Help: "Active gRPC connections",
		}),

		HTTPConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_http_connections_active",
			Help: "Active HTTP connections",
		}),
	}
}
