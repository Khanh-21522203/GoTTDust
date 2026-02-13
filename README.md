# TTDust

A high-performance, real-time data ingestion, processing, and query platform built in Go. TTDust is designed as a modular monolith that handles streaming data pipelines with schema validation, durable storage, and flexible querying.

## Features

### Data Ingestion
- **gRPC streaming ingestion** — bidirectional streaming for high-throughput record ingestion (≥10k records/s)
- **Batch ingestion** — async import from S3 files (JSON Lines, Parquet, CSV)
- **At-least-once delivery** with idempotency keys (24h dedup window, scoped per stream)
- **Write-Ahead Log (WAL)** — fsync before ack for durability, <10ms ack target
- **Backpressure** — configurable buffer thresholds to prevent overload

### Schema Registry
- **JSON Schema draft-07** validation on every ingested record
- **Schema versioning** with compatibility modes (backward, forward, full)
- **Namespace support** for multi-tenant schema organization
- **Fingerprint-based dedup** to prevent duplicate schema versions

### Stream Management
- **Stream lifecycle** with state machine (creating → active → paused → deleting → deleted)
- **Configurable retention policies** (time-based or size-based)
- **Time-based partitioning** — hourly partitions: `{stream}/{year}/{month}/{day}/{hour}/`
- **Per-stream statistics** tracking (records ingested, bytes, queries, storage)

### Processing Pipelines
- **Field projection** — select/rename/reorder fields
- **Timestamp normalization** — parse and standardize timestamp formats
- **Type coercion** — convert field types with configurable error behavior
- **Dead Letter Queue (DLQ)** — failed records routed to a separate stream (7-day retention)

### Query Engine
- **Time-range queries** with partition pruning and cursor-based pagination
- **Key-based lookup** for point queries on specific fields
- **gRPC streaming read** — live tail with historical replay
- **Multi-layer caching** — L1 in-process LRU → L2 Redis → L3 S3

### Storage
- **Parquet files** on S3-compatible storage for efficient columnar reads
- **Background compaction** — merges small files into 128MB targets
- **WAL archival** — sealed WAL segments archived to S3 for disaster recovery
- **Checkpoint management** — per-stream flush progress tracked in S3
- **Orphan file cleanup** — periodic scan for unreferenced data files

### Security
- **API key authentication** (data plane) with SHA-256 hashed storage
- **JWT Bearer authentication** (admin plane) with RSA public key validation
- **RBAC authorization** — 5 roles (admin, operator, schema_admin, writer, reader)
- **Per-request rate limiting** — Redis-based sliding window per client/category
- **Optional mTLS** with TLS 1.3

### Observability
- **Prometheus metrics** — 40+ metrics with `ttdust_` prefix (ingestion, processing, storage, query, infra)
- **Structured JSON logging** via logrus with component tagging and trace ID correlation
- **OpenTelemetry tracing** — OTLP export to Jaeger/Tempo
- **Health endpoints** — `/health/live` and `/health/ready` with per-component latency
- **Alerting rules** — 8 pre-configured Prometheus alerts (critical/warning/info)

### Operations
- **Hot config reload** via `SIGHUP` signal (rate limits, log level)
- **Graceful shutdown** with in-flight request draining
- **Docker Compose** dev stack with all dependencies
- **Database migrations** via golang-migrate
- **CI/CD** — GitHub Actions for build, test, lint, security scan, Docker push

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     API Gateway                          │
│  ┌──────────────────┐       ┌─────────────────────────┐ │
│  │  gRPC Server     │       │  REST Server            │ │
│  │  :50051          │       │  :8080                  │ │
│  │  (data plane)    │       │  (control plane + query)│ │
│  └────────┬─────────┘       └────────┬────────────────┘ │
│           │  Auth Interceptor        │  Auth Middleware  │
│           │  Rate Limiter            │  Rate Limiter     │
│           │  RBAC Authorizer         │  RBAC Authorizer  │
└───────────┼──────────────────────────┼──────────────────┘
            │                          │
┌───────────┼──────────────────────────┼──────────────────┐
│           ▼          Services        ▼                   │
│  ┌────────────────┐  ┌───────────┐  ┌────────────────┐  │
│  │  Ingestion     │  │  Query    │  │  Admin (REST)  │  │
│  │  Handler       │  │  Handler  │  │  Schema/Stream │  │
│  │  + Batch Exec  │  │  + Cache  │  │  Pipeline CRUD │  │
│  └───────┬────────┘  └─────┬─────┘  └───────┬────────┘  │
│          │                 │                 │            │
│  ┌───────▼────────┐  ┌────▼──────┐  ┌──────▼─────────┐  │
│  │  Processing    │  │  Planner  │  │  Schema Store  │  │
│  │  Pipeline Mgr  │  │  Executor │  │  Stream Mgr    │  │
│  └───────┬────────┘  └─────┬─────┘  └───────┬────────┘  │
└──────────┼─────────────────┼─────────────────┼──────────┘
           │                 │                 │
┌──────────┼─────────────────┼─────────────────┼──────────┐
│          ▼  Infrastructure ▼                 ▼           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  S3 Adapter  │  │  Redis       │  │  PostgreSQL  │   │
│  │  (data files)│  │  (cache/rate │  │  (metadata/  │   │
│  │  WAL Manager │  │   limit/idem)│  │   control)   │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

**Write path:**
```
Producer → gRPC → Validate (schema) → Buffer → WAL (fsync) → Ack
                                                    ↓ (async)
                                          Transform → Batch → S3 → Update Index
```

**Read path:**
```
Client → Auth → Query Plan → Cache Check (LRU → Redis → S3) → Response
```

## Project Structure

```
GoTTDust/
├── cmd/server/main.go          # Application entry point
├── proto/                      # Protobuf definitions
│   ├── ingestion.proto         #   Ingestion service (stream, unary, batch)
│   ├── query.proto             #   Query service (time-range, key lookup, streaming)
│   └── admin.proto             #   Admin service (stream CRUD)
├── internal/
│   ├── config/                 # Application configuration
│   ├── common/                 # Shared types, models, errors
│   ├── domain/                 # Domain model (entities, events, value objects)
│   ├── auth/                   # Authentication & authorization
│   │   ├── middleware.go       #   REST auth middleware
│   │   ├── grpc_interceptor.go #   gRPC auth interceptors
│   │   ├── authorizer.go       #   RBAC authorizer
│   │   ├── jwt.go              #   JWT validator
│   │   └── metrics.go          #   Security metrics
│   ├── schema/                 # Schema registry (validation, versioning)
│   ├── stream/                 # Stream lifecycle management
│   ├── ingestion/              # Ingestion pipeline
│   │   ├── handler.go          #   gRPC handler (stream + batch)
│   │   ├── wal.go              #   Write-Ahead Log manager
│   │   ├── buffer.go           #   In-memory record buffer
│   │   ├── flush.go            #   Buffer → S3 flush coordinator
│   │   ├── batch.go            #   Batch import executor
│   │   └── subscriber.go       #   Live tail subscription manager
│   ├── processing/             # Data transformation pipelines
│   ├── pipeline/               # Pipeline configuration manager
│   ├── query/                  # Query planning & execution
│   ├── storage/                # Storage layer
│   │   ├── manager.go          #   S3 file management & indexing
│   │   ├── parquet.go          #   Parquet batch serialization
│   │   ├── compaction.go       #   Background file compaction
│   │   ├── checkpoint.go       #   Checkpoint, WAL archival, compaction log
│   │   ├── manifest.go         #   Partition manifest management
│   │   ├── postgres/           #   PostgreSQL adapter
│   │   ├── redis/              #   Redis adapter (cache, rate limit, idempotency)
│   │   └── s3/                 #   S3 adapter
│   ├── health/                 # Health check tracker
│   ├── ratelimit/              # Redis-based sliding window rate limiter
│   ├── observability/          # Metrics, logging, tracing
│   ├── audit/                  # Audit log subscriber
│   ├── rest/                   # REST API handlers
│   │   ├── server.go           #   HTTP server setup
│   │   ├── api.go              #   Schema/Stream/Pipeline CRUD
│   │   ├── admin.go            #   Config & API key management
│   │   └── query.go            #   REST query endpoints
│   └── grpc/                   # gRPC server setup
├── scripts/init-db.sql         # Database bootstrap schema
├── migrations/                 # Database migrations
├── config/                     # Prometheus & alerting config
├── tests/
│   ├── fixtures/               # Test helpers & sample data
│   └── benchmarks/             # Performance benchmarks
├── docker-compose.yml          # Full dev stack
├── Dockerfile                  # Multi-stage production build
├── Makefile                    # Build, test, lint targets
└── .github/workflows/          # CI/CD pipelines
```

## Getting Started

### Prerequisites

- **Go 1.21+**
- **Docker & Docker Compose** (for local development)
- **protoc** (optional, for regenerating protobuf code)

### Quick Start with Docker Compose

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-org/GoTTDust.git
   cd GoTTDust
   ```

2. **Copy the environment file:**
   ```bash
   cp .env.example .env
   ```

3. **Start all dependencies (PostgreSQL, Redis, MinIO):**
   ```bash
   make docker-deps
   ```

4. **Run database migrations:**
   ```bash
   make migrate
   ```

5. **Build and run the server:**
   ```bash
   make build
   ./bin/ttdust
   ```

   Or run everything with Docker Compose:
   ```bash
   make docker-up
   ```

6. **Verify the server is running:**
   ```bash
   curl http://localhost:8080/health/live
   # {"status":"ok"}

   curl http://localhost:8080/health/ready
   # {"status":"ok","components":{...}}
   ```

### Local Development (without Docker)

If you have PostgreSQL, Redis, and MinIO running locally:

```bash
# Set environment variables (or use .env)
export TTDUST_PG_HOST=localhost
export TTDUST_REDIS_URL=redis://localhost:6379
export TTDUST_S3_ENDPOINT=http://localhost:9000

# Build and run
make build
./bin/ttdust
```

## API Reference

### REST API (`:8080`)

#### Schemas

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/schemas` | Create a schema with initial version |
| `GET` | `/api/v1/schemas` | List all schemas |
| `GET` | `/api/v1/schemas/{id}` | Get schema by ID |
| `POST` | `/api/v1/schemas/{id}/versions` | Add a new schema version |
| `GET` | `/api/v1/schemas/{id}/versions` | List schema versions |

#### Streams

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/streams` | Create a stream |
| `GET` | `/api/v1/streams` | List streams (filter by `?status=`) |
| `GET` | `/api/v1/streams/{id}` | Get stream by ID |
| `PATCH` | `/api/v1/streams/{id}` | Update stream (description, retention, status) |
| `DELETE` | `/api/v1/streams/{id}` | Delete a stream |

#### Query

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/streams/{id}/records?start=&end=&limit=&cursor=` | Time-range query |
| `GET` | `/api/v1/streams/{id}/records?key_field=&key_value=` | Key-based lookup |

#### Pipelines

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipelines` | Create a pipeline |
| `GET` | `/api/v1/pipelines` | List pipelines |
| `GET` | `/api/v1/pipelines/{id}` | Get pipeline by ID |

#### Admin

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/config` | Get system configuration |
| `PATCH` | `/api/v1/config` | Update system configuration |
| `POST` | `/api/v1/api-keys` | Create an API key |
| `GET` | `/api/v1/api-keys` | List API keys |
| `DELETE` | `/api/v1/api-keys/{id}` | Revoke an API key |

#### Health & Metrics

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe (checks all components) |
| `GET` | `/metrics` | Prometheus metrics |

### gRPC API (`:50051`)

#### IngestionService
- `IngestStream` — bidirectional streaming ingestion
- `Ingest` — unary single-record ingestion
- `IngestBatch` — submit async batch import job
- `GetBatchJob` — check batch job status

#### QueryService
- `QueryTimeRange` — paginated time-range query
- `LookupByKey` — key-based point lookup
- `StreamingRead` — server-streaming live tail with historical replay

## Usage Examples

### Create a Schema

```bash
curl -X POST http://localhost:8080/api/v1/schemas \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <API_KEY>" \
  -d '{
    "name": "user_events",
    "namespace": "analytics",
    "description": "User interaction events",
    "definition": {
      "type": "object",
      "required": ["user_id", "event_type", "timestamp"],
      "properties": {
        "user_id": {"type": "string"},
        "event_type": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "metadata": {"type": "object"}
      }
    }
  }'
```

### Create a Stream

```bash
curl -X POST http://localhost:8080/api/v1/streams \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <API_KEY>" \
  -d '{
    "name": "user-clicks",
    "description": "User click events stream",
    "schema_id": "sch_<uuid>",
    "retention_policy": {
      "type": "time",
      "retention_days": 30
    }
  }'
```

### Ingest Records (gRPC)

```go
conn, _ := grpc.Dial("localhost:50051", grpc.WithInsecure())
client := ingestionpb.NewIngestionServiceClient(conn)

// Single record
resp, _ := client.Ingest(ctx, &ingestionpb.IngestRequest{
    StreamId: "str_<uuid>",
    SchemaId: "sch_<uuid>",
    Payload:  []byte(`{"user_id":"u123","event_type":"click","timestamp":"2025-01-15T10:30:00Z"}`),
    IdempotencyKey: proto.String("click-u123-1705312200"),
})
fmt.Printf("Record ID: %s, Sequence: %d\n", resp.RecordId, resp.SequenceNumber)
```

### Query Records

```bash
# Time-range query
curl "http://localhost:8080/api/v1/streams/str_<uuid>/records?\
start=2025-01-15T00:00:00Z&end=2025-01-16T00:00:00Z&limit=100" \
  -H "Authorization: Bearer <API_KEY>"

# Key lookup
curl "http://localhost:8080/api/v1/streams/str_<uuid>/records?\
key_field=_record_id&key_value=rec_<uuid>" \
  -H "Authorization: Bearer <API_KEY>"
```

### Create an API Key

```bash
curl -X POST http://localhost:8080/api/v1/api-keys \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <ADMIN_JWT>" \
  -d '{
    "name": "producer-service",
    "role": "writer",
    "stream_permissions": ["str_<uuid>"]
  }'
# Returns: {"key_id":"key_...","api_key":"ttd_live_..."}
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TTDUST_BIND_ADDRESS` | `0.0.0.0:8080` | REST server bind address |
| `TTDUST_GRPC_ADDRESS` | `0.0.0.0:50051` | gRPC server bind address |
| `TTDUST_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `TTDUST_LOG_FORMAT` | `json` | Log format (json, text) |
| `TTDUST_PG_HOST` | `localhost` | PostgreSQL host |
| `TTDUST_PG_PORT` | `5432` | PostgreSQL port |
| `TTDUST_PG_DATABASE` | `ttdust` | PostgreSQL database |
| `TTDUST_PG_USER` | `ttdust` | PostgreSQL user |
| `TTDUST_PG_PASSWORD` | — | PostgreSQL password |
| `TTDUST_REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `TTDUST_S3_ENDPOINT` | — | S3-compatible endpoint |
| `TTDUST_S3_BUCKET` | `ttdust-data` | S3 bucket name |
| `TTDUST_S3_ACCESS_KEY` | — | S3 access key |
| `TTDUST_S3_SECRET_KEY` | — | S3 secret key |
| `TTDUST_S3_REGION` | `us-east-1` | S3 region |
| `TTDUST_WAL_DIRECTORY` | `/var/lib/ttdust/wal` | WAL segment directory |
| `TTDUST_SHUTDOWN_TIMEOUT` | `30` | Graceful shutdown timeout (seconds) |
| `TTDUST_JWT_PUBLIC_KEY_PATH` | — | Path to RSA public key for JWT validation |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OpenTelemetry OTLP endpoint (enables tracing) |

### Hot Reload

Send `SIGHUP` to reload configuration without downtime:

```bash
kill -HUP $(pgrep ttdust)
```

Reloadable settings: rate limit configs, log level.

## Observability

### Enable the Full Observability Stack

```bash
make docker-observability
```

This starts Prometheus (`:9091`), Grafana (`:3000`), and Jaeger (`:16686`) alongside the core services.

### Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9091 | — |
| Jaeger UI | http://localhost:16686 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `ttdust_ingestion_records_received_total` | Counter | Records received by stream |
| `ttdust_ingestion_records_validated_total` | Counter | Records passing validation |
| `ttdust_ingestion_ack_latency_seconds` | Histogram | End-to-end ack latency |
| `ttdust_storage_flush_duration_seconds` | Histogram | Buffer-to-S3 flush time |
| `ttdust_query_execution_duration_seconds` | Histogram | Query execution time |
| `ttdust_cp_requests_total` | Counter | Control plane requests by endpoint |
| `ttdust_auth_attempts_total` | Counter | Auth attempts by method/result |
| `ttdust_rate_limit_hits_total` | Counter | Rate limit rejections |

## Development

### Make Targets

```bash
make build            # Build binary to bin/ttdust
make test             # Run all tests with race detector
make test-unit        # Run unit tests only
make test-bench       # Run benchmarks
make test-coverage    # Generate HTML coverage report
make lint             # Run golangci-lint
make fmt              # Format code
make vet              # Run go vet
make check            # fmt + vet + lint + test-unit
make proto            # Regenerate protobuf code
make docker           # Build Docker image
make docker-up        # Start full stack
make docker-deps      # Start dependencies only
make docker-clean     # Stop and remove all volumes
make migrate          # Run database migrations
```

### Running Tests

```bash
# All tests
make test

# Unit tests only (fast)
make test-unit

# Benchmarks
make test-bench

# Coverage report
make test-coverage
open coverage.html
```

## Benchmark Results

Benchmarks run on AMD Ryzen 7 PRO 8840HS, 16 threads, Linux. Run with:

```bash
make test-bench
# or: go test -bench=. -benchmem -benchtime=3s ./tests/benchmarks/
```

### Record Buffer (in-memory ingestion buffer)

| Benchmark | ops/sec | ns/op | allocs/op |
|-----------|---------|-------|-----------|
| BufferAdd 100B | 421M | 237 | 2 |
| BufferAdd 1KB | 423M | 236 | 2 |
| BufferAdd 10KB | 493M | 203 | 2 |
| BufferAdd Concurrent (16 writers) | 428M | 233 | 2 |
| BufferFlush 1,000 records | 4.3K | 232µs | 3 |
| BufferFlush 10,000 records | 2.2K | 458µs | 3 |
| BackpressureCheck | 629M | 6.6 | 0 |
| BufferManager GetOrCreate (single) | 333M | 10.5 | 0 |
| BufferManager GetOrCreate (100 streams) | 233M | 15.7 | 0 |
| BufferManager FlushAll (10×1000) | 744 | 5.6ms | 55 |

### WAL (Write-Ahead Log)

| Benchmark | ops/sec | throughput | ns/op | allocs/op |
|-----------|---------|------------|-------|-----------|
| WAL Write 100B (no fsync) | 993K | 27.9 MB/s | 3,590 | 1 |
| WAL Write 1KB (no fsync) | 662K | 192.6 MB/s | 5,317 | 1 |
| WAL Write 10KB (no fsync) | 187K | 459.7 MB/s | 22,276 | 1 |
| WAL Write 1KB (fsync) | 7.0K | 2.4 MB/s | 428,359 | 1 |
| WAL WriteBatch 100×1KB (no fsync) | 6.9K | 202.2 MB/s | 506µs | 100 |
| WAL WriteBatch 1000×1KB (no fsync) | 782 | 205.4 MB/s | 5.0ms | 1,000 |
| WAL WriteBatch 100×1KB (fsync) | 3.1K | 92.8 MB/s | 1.1ms | 100 |
| WAL Write 10 streams 1KB | 559K | 188.7 MB/s | 5,428 | 1 |
| WAL SealSegment | 191M | — | 18.3 | 0 |

### Schema Validation (JSON Schema draft-07)

| Benchmark | ops/sec | throughput | ns/op | allocs/op |
|-----------|---------|------------|-------|-----------|
| Schema Compile | 126K | — | 31,958 | 277 |
| Validate valid payload (168B) | 331K | 13.7 MB/s | 12,278 | 172 |
| Validate invalid payload | 271K | 4.1 MB/s | 13,545 | 161 |
| Validate 50-field payload (1.2KB) | 71K | 24.8 MB/s | 47,179 | 508 |
| Validate 200-field payload (4.7KB) | 19K | 25.3 MB/s | 184,337 | 1,718 |
| Validate concurrent (16 threads) | 807K | 38.2 MB/s | 4,401 | 172 |
| Registry Get (cached) | 271M | — | 11.7 | 0 |
| Registry Get concurrent | 100M | — | 30.2 | 0 |

### Parquet Batch Serialization

| Benchmark | ops/sec | throughput | ns/op | allocs/op |
|-----------|---------|------------|-------|-----------|
| Batch Add 100 records | 59K | — | 51,807 | 310 |
| Batch Add 1,000 records | 4.9K | — | 1.0ms | 3,015 |
| Batch Add 10,000 records | 490 | — | 8.0ms | 30,032 |
| MarshalJSON 100 records | 31K | 718.7 MB/s | 128µs | 2 |
| MarshalJSON 1,000 records | 3.2K | 897.1 MB/s | 1.0ms | 3 |
| UnmarshalJSON 100 records | 6.4K | 157.9 MB/s | 584µs | 416 |
| UnmarshalJSON 1,000 records | 625 | 171.1 MB/s | 5.4ms | 4,020 |

### EventBus

| Benchmark | ops/sec | ns/op | allocs/op |
|-----------|---------|-------|-----------|
| Publish (no subscribers) | 57M | 63.4 | 1 |
| Publish (1 subscriber) | 51M | 85.4 | 1 |
| Publish (10 subscribers) | 25M | 139.1 | 1 |
| Publish (SubscribeAll) | 31M | 120.0 | 1 |
| Publish concurrent (16 threads) | 56M | 63.9 | 1 |

### JSON Record Serialization

| Benchmark | ops/sec | ns/op | allocs/op |
|-----------|---------|-------|-----------|
| Record Serialize | 5.1M | 1,160 | 15 |
| Record Deserialize | 5.3M | 722 | 28 |
| Batch Serialize (100 records) | 44K | 120µs | 702 |

### Key Takeaways

- **Buffer throughput**: ~4.2M adds/sec per thread, lock contention minimal even at 16 concurrent writers
- **WAL write**: 192 MB/s without fsync, 2.4 MB/s with fsync (disk-bound); batch writes amortize fsync cost (92 MB/s for 100-record batches)
- **Schema validation**: ~81K validations/sec single-threaded, scales linearly to ~807K/sec at 16 threads
- **Parquet serialization**: ~900 MB/s marshal, ~170 MB/s unmarshal — JSON intermediate format is the bottleneck
- **EventBus**: ~51M events/sec with subscriber dispatch, negligible overhead

## Performance Targets

| Metric | Target |
|--------|--------|
| Ingestion throughput | ≥10,000 records/s, ≥10 MB/s |
| Ingestion latency (p50) | ≤10ms |
| Ingestion latency (p99) | ≤100ms |
| Key lookup (p50) | ≤50ms |
| Key lookup (p99) | ≤200ms |
| Time-range query (p50) | ≤500ms |
| Time-range query (p99) | ≤2s |
| Memory (normal) | ≤2 GB heap |
| Memory (peak) | ≤4 GB heap |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.21+ |
| gRPC | google.golang.org/grpc |
| Database | PostgreSQL 15+ (pgx driver) |
| Cache | Redis 7+ (go-redis) |
| Object Storage | S3-compatible (aws-sdk-go-v2) |
| Schema Validation | gojsonschema (JSON Schema draft-07) |
| Metrics | Prometheus (client_golang) |
| Logging | logrus (structured JSON) |
| Tracing | OpenTelemetry (OTLP) |
| Auth | golang-jwt/jwt/v5 |
| Container | Distroless (production), Alpine (debug) |

## License

This project is for educational and personal use.
