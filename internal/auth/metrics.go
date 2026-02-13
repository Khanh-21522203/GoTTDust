package auth

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SecurityMetrics holds all security-related Prometheus metrics.
type SecurityMetrics struct {
	AuthAttemptsTotal      *prometheus.CounterVec
	AuthzDenialsTotal      *prometheus.CounterVec
	RateLimitHitsTotal     *prometheus.CounterVec
	SuspiciousRequestTotal prometheus.Counter
	APIKeyUsage            prometheus.Gauge
}

// NewSecurityMetrics registers and returns security metrics.
func NewSecurityMetrics() *SecurityMetrics {
	return &SecurityMetrics{
		AuthAttemptsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_auth_attempts_total",
			Help: "Authentication attempts by outcome",
		}, []string{"method", "outcome"}),

		AuthzDenialsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_authz_denials_total",
			Help: "Authorization denials by resource type",
		}, []string{"resource_type", "action"}),

		RateLimitHitsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "ttdust_rate_limit_hits_total",
			Help: "Rate limit triggers by category",
		}, []string{"category", "client_id"}),

		SuspiciousRequestTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "ttdust_suspicious_requests_total",
			Help: "Requests flagged as suspicious",
		}),

		APIKeyUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "ttdust_api_key_usage",
			Help: "Active API key count",
		}),
	}
}
