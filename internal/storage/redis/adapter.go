package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"GoTTDust/internal/common"
	pb "GoTTDust/internal/genproto/ingestionpb"

	"github.com/go-redis/redis/v8"
)

// Config holds Redis adapter configuration.
type Config struct {
	Address  string
	Password string
	DB       int
	PoolSize int
}

// Adapter provides operations against Redis.
type Adapter struct {
	client *redis.Client
}

// NewAdapter creates a new Redis adapter.
func NewAdapter(cfg Config) (*Adapter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	return &Adapter{client: client}, nil
}

// Ping checks the Redis connection.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.client.Ping(ctx).Err()
}

// Close closes the Redis connection.
func (a *Adapter) Close() error {
	return a.client.Close()
}

// --- IdempotencyChecker implementation ---

const idempotencyTTL = 24 * time.Hour

// Check looks up an idempotency key in Redis.
// Returns the cached response and whether it was found.
func (a *Adapter) Check(ctx context.Context, streamID common.StreamID, key string) (*pb.IngestResponse, bool, error) {
	redisKey := fmt.Sprintf("idem:%s:%s", streamID, key)

	data, err := a.client.Get(ctx, redisKey).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis get idempotency key: %w", err)
	}

	var resp pb.IngestResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false, fmt.Errorf("unmarshal cached response: %w", err)
	}

	return &resp, true, nil
}

// Store saves an idempotency key and response in Redis with TTL.
func (a *Adapter) Store(ctx context.Context, streamID common.StreamID, key string, resp *pb.IngestResponse) error {
	redisKey := fmt.Sprintf("idem:%s:%s", streamID, key)

	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	return a.client.Set(ctx, redisKey, data, idempotencyTTL).Err()
}

// --- Cache implementation for Query subsystem ---

// Get retrieves a value from Redis cache.
func (a *Adapter) Get(ctx context.Context, key string) ([]byte, bool) {
	data, err := a.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	return data, true
}

// Set stores a value in Redis cache with TTL.
func (a *Adapter) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	_ = a.client.Set(ctx, key, value, ttl).Err()
}

// Delete removes a key from Redis.
func (a *Adapter) Delete(ctx context.Context, key string) {
	_ = a.client.Del(ctx, key).Err()
}

// Publish publishes a message to a Redis pub/sub channel.
func (a *Adapter) Publish(ctx context.Context, channel string, message []byte) error {
	return a.client.Publish(ctx, channel, message).Err()
}

// Subscribe subscribes to a Redis pub/sub channel.
func (a *Adapter) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return a.client.Subscribe(ctx, channel)
}

// --- Record Cache (ttdust:record:{stream_id}:{record_id}, TTL 5min) ---

const recordCacheTTL = 5 * time.Minute

// CacheRecord stores a record in the Redis record cache.
func (a *Adapter) CacheRecord(ctx context.Context, streamID, recordID string, data []byte) {
	key := fmt.Sprintf("ttdust:record:%s:%s", streamID, recordID)
	_ = a.client.Set(ctx, key, data, recordCacheTTL).Err()
}

// GetCachedRecord retrieves a record from the Redis record cache.
func (a *Adapter) GetCachedRecord(ctx context.Context, streamID, recordID string) ([]byte, bool) {
	key := fmt.Sprintf("ttdust:record:%s:%s", streamID, recordID)
	data, err := a.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	return data, true
}

// --- Rate Limiting (ttdust:rate:{client_id}:{window}) ---

// IncrRateLimit increments a rate limit counter and returns the new count.
func (a *Adapter) IncrRateLimit(ctx context.Context, clientID string, window time.Time, ttl time.Duration) (int64, error) {
	key := fmt.Sprintf("ttdust:rate:%s:%d", clientID, window.UnixMilli())
	pipe := a.client.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("rate limit incr: %w", err)
	}
	return incr.Val(), nil
}
