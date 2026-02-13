package health

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"GoTTDust/internal/common"
	redisadapter "GoTTDust/internal/storage/redis"
	s3adapter "GoTTDust/internal/storage/s3"
)

// Checker checks the health of a single component.
type Checker func(ctx context.Context) (string, error)

// Tracker manages health checks for all system components.
type Tracker struct {
	mu       sync.RWMutex
	db       *sql.DB
	nodeID   string
	checkers map[string]Checker
	statuses map[string]*common.ComponentHealth
}

// NewTracker creates a new health tracker.
func NewTracker(db *sql.DB, nodeID string) *Tracker {
	return &Tracker{
		db:       db,
		nodeID:   nodeID,
		checkers: make(map[string]Checker),
		statuses: make(map[string]*common.ComponentHealth),
	}
}

// RegisterChecker registers a health checker for a component.
func (t *Tracker) RegisterChecker(componentType string, checker Checker) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checkers[componentType] = checker
}

// RegisterPostgres registers a PostgreSQL health checker.
func (t *Tracker) RegisterPostgres(db *sql.DB) {
	t.RegisterChecker("postgresql", func(ctx context.Context) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return common.HealthStatusUnhealthy, fmt.Errorf("ping failed: %w", err)
		}
		return common.HealthStatusHealthy, nil
	})
}

// RegisterRedis registers a Redis health checker.
func (t *Tracker) RegisterRedis(redis *redisadapter.Adapter) {
	t.RegisterChecker("redis", func(ctx context.Context) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := redis.Ping(ctx); err != nil {
			return common.HealthStatusDegraded, fmt.Errorf("ping failed: %w", err)
		}
		return common.HealthStatusHealthy, nil
	})
}

// RegisterS3 registers an S3 health checker.
func (t *Tracker) RegisterS3(s3 *s3adapter.Adapter, bucket string) {
	t.RegisterChecker("s3", func(ctx context.Context) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		_, err := s3.ListObjects(ctx, "_system/healthcheck")
		if err != nil {
			return common.HealthStatusDegraded, fmt.Errorf("list failed: %w", err)
		}
		return common.HealthStatusHealthy, nil
	})
}

// RegisterIngestionBuffer registers an ingestion buffer health checker.
func (t *Tracker) RegisterIngestionBuffer(utilizationFn func() float64) {
	t.RegisterChecker("ingestion", func(ctx context.Context) (string, error) {
		util := utilizationFn()
		if util >= 0.90 {
			return common.HealthStatusUnhealthy, fmt.Errorf("buffer utilization %.0f%%", util*100)
		}
		if util >= 0.70 {
			return common.HealthStatusDegraded, fmt.Errorf("buffer utilization %.0f%%", util*100)
		}
		return common.HealthStatusHealthy, nil
	})
}

// CheckAll runs all registered health checks and returns component statuses.
func (t *Tracker) CheckAll(ctx context.Context) map[string]*common.ComponentHealth {
	t.mu.RLock()
	checkers := make(map[string]Checker, len(t.checkers))
	for k, v := range t.checkers {
		checkers[k] = v
	}
	t.mu.RUnlock()

	results := make(map[string]*common.ComponentHealth, len(checkers))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for compType, checker := range checkers {
		wg.Add(1)
		go func(ct string, ch Checker) {
			defer wg.Done()
			start := time.Now()
			status, err := ch(ctx)
			latency := time.Since(start)

			health := &common.ComponentHealth{
				ComponentID:   fmt.Sprintf("%s:%s", t.nodeID, ct),
				NodeID:        t.nodeID,
				ComponentType: ct,
				Status:        status,
				LastHeartbeat: time.Now(),
				LatencyMs:     latency.Milliseconds(),
			}

			if err != nil {
				health.Message = err.Error()
				details, _ := json.Marshal(map[string]string{"error": err.Error()})
				health.Details = details
			}

			mu.Lock()
			results[ct] = health
			mu.Unlock()
		}(compType, checker)
	}

	wg.Wait()

	// Update in-memory cache
	t.mu.Lock()
	t.statuses = results
	t.mu.Unlock()

	return results
}

// AggregateStatus returns the overall system health status.
func (t *Tracker) AggregateStatus() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	hasUnhealthy := false
	hasDegraded := false

	for _, h := range t.statuses {
		switch h.Status {
		case common.HealthStatusUnhealthy:
			hasUnhealthy = true
		case common.HealthStatusDegraded:
			hasDegraded = true
		}
	}

	if hasUnhealthy {
		return common.HealthStatusUnhealthy
	}
	if hasDegraded {
		return common.HealthStatusDegraded
	}
	return common.HealthStatusHealthy
}

// GetStatuses returns the current cached component statuses.
func (t *Tracker) GetStatuses() map[string]*common.ComponentHealth {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]*common.ComponentHealth, len(t.statuses))
	for k, v := range t.statuses {
		result[k] = v
	}
	return result
}

// PersistStatuses writes current health statuses to PostgreSQL.
func (t *Tracker) PersistStatuses(ctx context.Context) error {
	t.mu.RLock()
	statuses := make(map[string]*common.ComponentHealth, len(t.statuses))
	for k, v := range t.statuses {
		statuses[k] = v
	}
	t.mu.RUnlock()

	for _, h := range statuses {
		_, err := t.db.ExecContext(ctx,
			`INSERT INTO component_health (component_id, node_id, component_type, status, last_heartbeat, details)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (component_id)
			 DO UPDATE SET status = $4, last_heartbeat = $5, details = $6`,
			h.ComponentID, h.NodeID, h.ComponentType, h.Status, h.LastHeartbeat, h.Details)
		if err != nil {
			return fmt.Errorf("persist health %s: %w", h.ComponentID, err)
		}
	}
	return nil
}

// RunLoop starts a periodic health check loop.
func (t *Tracker) RunLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial check
	t.CheckAll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.CheckAll(ctx)
			_ = t.PersistStatuses(ctx)
		}
	}
}
