package ratelimit

import (
	"context"
	"fmt"
	"time"

	redisadapter "GoTTDust/internal/storage/redis"
)

// Category defines rate limit categories.
type Category string

const (
	CategoryIngestion Category = "ingestion"
	CategoryQuery     Category = "query"
	CategoryAdmin     Category = "admin"
)

// Config holds rate limit configuration per category.
type Config struct {
	Limit int           // Requests per window
	Burst int           // Burst allowance
	Window time.Duration // Window duration
}

// DefaultConfigs returns the default rate limit configs per api-spec.md §4.1.
func DefaultConfigs() map[Category]Config {
	return map[Category]Config{
		CategoryIngestion: {Limit: 10000, Burst: 15000, Window: time.Second},
		CategoryQuery:     {Limit: 1000, Burst: 2000, Window: time.Second},
		CategoryAdmin:     {Limit: 100, Burst: 200, Window: time.Second},
	}
}

// Result holds the outcome of a rate limit check.
type Result struct {
	Allowed   bool
	Limit     int
	Remaining int
	ResetAt   time.Time
	RetryAfter time.Duration
}

// Limiter implements Redis-based sliding window rate limiting.
type Limiter struct {
	redis   *redisadapter.Adapter
	configs map[Category]Config
}

// NewLimiter creates a new rate limiter.
func NewLimiter(redis *redisadapter.Adapter, configs map[Category]Config) *Limiter {
	if configs == nil {
		configs = DefaultConfigs()
	}
	return &Limiter{
		redis:   redis,
		configs: configs,
	}
}

// Allow checks if a request is allowed for the given client and category.
func (l *Limiter) Allow(ctx context.Context, clientID string, category Category) Result {
	cfg, ok := l.configs[category]
	if !ok {
		return Result{Allowed: true, Limit: 0, Remaining: 0}
	}

	now := time.Now()
	windowKey := now.Truncate(cfg.Window)
	key := fmt.Sprintf("ttdust:rate:%s:%s:%d", clientID, category, windowKey.UnixMilli())

	// Increment counter
	data, found := l.redis.Get(ctx, key)
	var count int
	if found {
		for _, b := range data {
			count = count*256 + int(b)
		}
	}

	count++
	remaining := cfg.Burst - count
	if remaining < 0 {
		remaining = 0
	}

	resetAt := windowKey.Add(cfg.Window)

	if count > cfg.Burst {
		return Result{
			Allowed:    false,
			Limit:      cfg.Limit,
			Remaining:  0,
			ResetAt:    resetAt,
			RetryAfter: time.Until(resetAt),
		}
	}

	// Store incremented count
	countBytes := []byte(fmt.Sprintf("%d", count))
	l.redis.Set(ctx, key, countBytes, cfg.Window+time.Second)

	return Result{
		Allowed:   true,
		Limit:     cfg.Limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}
}

// UpdateConfigs replaces the rate limit configs (hot-reload on SIGHUP).
func (l *Limiter) UpdateConfigs(configs map[Category]Config) {
	l.configs = configs
}

// GetConfig returns the config for a category.
func (l *Limiter) GetConfig(category Category) Config {
	return l.configs[category]
}
