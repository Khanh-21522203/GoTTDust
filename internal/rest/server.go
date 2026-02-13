package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsRecorder records per-request metrics for the control plane.
type MetricsRecorder func(endpoint, method string, statusCode int, duration time.Duration)

// Server wraps the HTTP server for admin/query REST endpoints.
type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux
}

// HealthStatus represents the health check response.
type HealthStatus struct {
	Status     string                     `json:"status"`
	Components map[string]ComponentStatus `json:"components,omitempty"`
	Timestamp  string                     `json:"timestamp,omitempty"`
}

// ComponentStatus is the health status of a single component.
type ComponentStatus struct {
	Status    string `json:"status"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ComponentCheck is the result of a health check for a single component.
type ComponentCheck struct {
	Healthy   bool
	LatencyMs int64
	Error     string
}

// NewServer creates a new REST server.
func NewServer(address string) *Server {
	mux := http.NewServeMux()

	s := &Server{
		httpServer: &http.Server{
			Addr:         address,
			Handler:      mux,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		mux: mux,
	}

	// Health endpoints
	mux.HandleFunc("/health/live", s.handleLiveness)
	mux.HandleFunc("/health/ready", s.handleReadiness)

	// Metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	return s
}

// SetReadinessChecker sets the function used to check component readiness.
// The checker returns per-component health with latency.
func (s *Server) SetReadinessChecker(checker func() map[string]ComponentCheck) {
	s.mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		checks := checker()
		status := "ready"
		components := make(map[string]ComponentStatus)

		for name, check := range checks {
			cs := ComponentStatus{
				LatencyMs: check.LatencyMs,
			}
			if check.Healthy {
				cs.Status = "healthy"
			} else {
				cs.Status = "unhealthy"
				cs.Error = check.Error
				status = "not_ready"
			}
			components[name] = cs
		}

		resp := HealthStatus{
			Status:     status,
			Components: components,
		}

		w.Header().Set("Content-Type", "application/json")
		if status != "ready" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(resp)
	})
}

// AuthMiddleware is an interface for HTTP authentication middleware.
type AuthMiddleware interface {
	Handler(next http.Handler) http.Handler
}

// SetAuthMiddleware wraps the server's handler with authentication middleware.
// Health and metrics endpoints are skipped by the middleware itself.
func (s *Server) SetAuthMiddleware(m AuthMiddleware) {
	s.httpServer.Handler = m.Handler(s.mux)
}

// RegisterAPI registers the control plane API routes.
func (s *Server) RegisterAPI(api *APIHandler) {
	api.RegisterRoutes(s.mux)
}

// RegisterAdmin registers the admin API routes.
func (s *Server) RegisterAdmin(admin *AdminHandler) {
	admin.RegisterRoutes(s.mux)
}

// RegisterQueryREST registers the REST query routes.
func (s *Server) RegisterQueryREST(qh *QueryRESTHandler) {
	qh.RegisterRoutes(s.mux)
}

// SetMetricsRecorder wraps the server handler with per-request metrics recording.
// Must be called before SetAuthMiddleware (metrics wraps outermost).
func (s *Server) SetMetricsRecorder(recorder MetricsRecorder) {
	inner := s.httpServer.Handler
	s.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		inner.ServeHTTP(rw, r)
		recorder(r.URL.Path, r.Method, rw.statusCode, time.Since(start))
	})
}

// statusRecorder wraps http.ResponseWriter to capture the status code.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

// Mux returns the underlying ServeMux for additional route registration.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Serve starts the REST server.
func (s *Server) Serve() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the REST server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// handleLiveness returns 200 if the process is running.
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	resp := HealthStatus{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleReadiness returns 200 if all dependencies are connected.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	resp := HealthStatus{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
