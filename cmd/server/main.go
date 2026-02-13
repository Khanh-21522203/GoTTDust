package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"GoTTDust/internal/audit"
	"GoTTDust/internal/auth"
	"GoTTDust/internal/common"
	"GoTTDust/internal/config"
	"GoTTDust/internal/domain"
	grpcserver "GoTTDust/internal/grpc"
	"GoTTDust/internal/health"
	"GoTTDust/internal/ingestion"
	"GoTTDust/internal/observability"
	pipelinemgr "GoTTDust/internal/pipeline"
	"GoTTDust/internal/processing"
	"GoTTDust/internal/query"
	"GoTTDust/internal/ratelimit"
	"GoTTDust/internal/rest"
	"GoTTDust/internal/schema"
	"GoTTDust/internal/storage"
	"GoTTDust/internal/storage/postgres"
	redisadapter "GoTTDust/internal/storage/redis"
	s3adapter "GoTTDust/internal/storage/s3"
	streammgr "GoTTDust/internal/stream"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.DefaultConfig()
	const nodeID = "node01"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Observability ---
	dpMetrics := observability.NewDataPlaneMetrics()
	cpMetrics := observability.NewControlPlaneMetrics()

	// Structured logger
	logger := observability.DefaultLogger()
	logger.Info("Starting TTDust server")

	// OpenTelemetry tracing (optional, based on env)
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint != "" {
		tp, err := observability.InitTracing(ctx, observability.TracingConfig{
			Enabled:      true,
			ServiceName:  "ttdust",
			Exporter:     "otlp",
			OTLPEndpoint: otlpEndpoint,
			SampleRatio:  0.1,
		})
		if err != nil {
			logger.WithError(err).Warn("Failed to init tracing, continuing without")
		} else if tp != nil {
			defer func() { _ = tp.Shutdown(context.Background()) }()
			logger.Info("OpenTelemetry tracing enabled")
		}
	}

	// Security metrics
	securityMetrics := auth.NewSecurityMetrics()

	// --- Infrastructure adapters ---

	// PostgreSQL
	pgAdapter, err := postgres.NewAdapter(postgres.Config{
		DSN:             cfg.Storage.Postgres.DSN,
		MaxOpenConns:    cfg.Storage.Postgres.MaxOpenConns,
		MaxIdleConns:    cfg.Storage.Postgres.MaxIdleConns,
		ConnMaxLifetime: cfg.Storage.Postgres.ConnMaxLifetime,
	})
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	_ = pgAdapter.Close()
	db := pgAdapter.DB()

	// Redis
	redisAdapter, err := redisadapter.NewAdapter(redisadapter.Config{
		Address:  cfg.Storage.Redis.Address,
		Password: cfg.Storage.Redis.Password,
		DB:       cfg.Storage.Redis.DB,
		PoolSize: cfg.Storage.Redis.PoolSize,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	_ = redisAdapter.Close()

	// S3
	s3Adapter, err := s3adapter.NewAdapter(ctx, s3adapter.Config{
		Endpoint:        cfg.Storage.S3.Endpoint,
		Bucket:          cfg.Storage.S3.Bucket,
		Region:          cfg.Storage.S3.Region,
		AccessKeyID:     cfg.Storage.S3.AccessKeyID,
		SecretAccessKey: cfg.Storage.S3.SecretAccessKey,
		UsePathStyle:    cfg.Storage.S3.UsePathStyle,
	})
	if err != nil {
		log.Fatalf("Failed to create S3 adapter: %v", err)
	}

	// --- Control plane services ---

	// Schema store (PostgreSQL-backed with L1/L2/L3 caching)
	schemaStore := schema.NewStore(db, redisAdapter)

	// In-memory schema registry (for data plane fast-path validation)
	schemaRegistry := schema.NewRegistry()

	// Stream manager
	streamManager := streammgr.NewManager(db)

	// Pipeline manager (control plane)
	pipelineMgr := pipelinemgr.NewManager(db)

	// Audit logger
	auditLogger := audit.NewLogger(db)

	// Domain event bus with audit subscriber
	eventBus := domain.NewEventBus()
	eventBus.SubscribeAll(func(event domain.Event) {
		entry := common.AuditLog{
			Timestamp: event.OccurredAt(),
			Action:    event.EventType(),
		}
		// Extract resource info from known event types
		switch e := event.(type) {
		case domain.SchemaCreated:
			entry.Actor = string(e.Actor)
			entry.ResourceType = "schema"
			entry.ResourceID = e.SchemaID
		case domain.StreamCreated:
			entry.Actor = string(e.Actor)
			entry.ResourceType = "stream"
			entry.ResourceID = e.StreamID
		case domain.StreamStatusChanged:
			entry.Actor = string(e.Actor)
			entry.ResourceType = "stream"
			entry.ResourceID = e.StreamID
		default:
			entry.Actor = "system"
		}
		if err := auditLogger.Log(context.Background(), entry); err != nil {
			log.Printf("Audit log error: %v", err)
		}
	})

	// Rate limiter (Redis-backed sliding window)
	rateLimiter := ratelimit.NewLimiter(redisAdapter, ratelimit.DefaultConfigs())

	// RBAC authorizer
	authorizer := auth.NewAuthorizer()

	// --- Data plane services ---

	// WAL manager
	walConfig := ingestion.WALConfig{
		Dir:               cfg.Storage.WAL.Dir,
		SegmentSizeBytes:  cfg.Storage.WAL.SegmentSizeBytes,
		SegmentAge:        cfg.Storage.WAL.SegmentAge,
		SyncMode:          cfg.Storage.WAL.SyncMode,
		RetentionSegments: cfg.Storage.WAL.RetentionSegments,
	}
	walManager, err := ingestion.NewWALManager(walConfig)
	if err != nil {
		log.Fatalf("Failed to create WAL manager: %v", err)
	}
	_ = walManager.Close()

	// WAL recovery on startup
	recovered, err := walManager.Recover()
	if err != nil {
		log.Printf("WAL recovery warning: %v", err)
	} else if len(recovered) > 0 {
		var totalRecords int
		for _, records := range recovered {
			totalRecords += len(records)
		}
		log.Printf("WAL recovery: replayed %d records from %d streams", totalRecords, len(recovered))
	}

	// Storage manager
	storageManager := storage.NewManager(s3Adapter, nodeID)

	// Compactor
	compactor := storage.NewCompactor(storage.DefaultCompactionConfig(), storageManager)
	go compactor.RunLoop(ctx)

	// Processing pipeline manager (data plane)
	processingManager := processing.NewPipelineManager()

	// Checkpoint manager (per-stream flush progress in S3)
	checkpointMgr := storage.NewCheckpointManager(s3Adapter, nodeID)

	// WAL archiver (sealed WAL segments to S3 for disaster recovery)
	walArchiver := storage.NewWALArchiver(s3Adapter, nodeID)

	// Ingestion handler
	ingestionHandler := ingestion.NewIngestionHandler(
		schemaRegistry,
		walManager,
		redisAdapter,
		ingestion.IngestionHandlerConfig{
			MaxMessageSizeBytes: cfg.Ingestion.MaxMessageSizeBytes,
			MaxBatchSize:        cfg.Ingestion.MaxBatchSize,
			MaxBufferBytes:      int64(cfg.Ingestion.BackpressureThreshold) * 1024,
			FlushInterval:       cfg.Ingestion.FlushInterval,
		},
	)

	// Wire data plane metrics into ingestion handler
	ingestionHandler.OnReceived = func(streamID string, status string) {
		dpMetrics.IngestionRecordsTotal.WithLabelValues(streamID, status).Inc()
		dpMetrics.IngestionRequestsTotal.WithLabelValues(streamID, status).Inc()
	}
	ingestionHandler.OnValidated = func(streamID string, status string) {
		if status == "rejected" {
			dpMetrics.IngestionValidationErrorTotal.WithLabelValues(streamID, "schema").Inc()
		}
	}
	ingestionHandler.OnBuffered = func(streamID string) {
		dpMetrics.IngestionBufferRecords.WithLabelValues(streamID).Inc()
	}
	ingestionHandler.OnWALWrite = func(streamID string) {
		dpMetrics.WALWritesTotal.WithLabelValues(streamID).Inc()
	}

	// Batch executor for async batch import jobs
	batchExecutor := ingestion.NewBatchExecutor(
		db,
		schemaRegistry,
		walManager,
		ingestionHandler.GetBufferManager(),
		func(ctx context.Context, uri string) (io.ReadCloser, error) {
			data, err := s3Adapter.GetObject(ctx, uri)
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	)
	ingestionHandler.SetBatchExecutor(batchExecutor)

	// Flush coordinator (buffer → processing → S3 → checkpoint)
	flushCoordinator := ingestion.NewFlushCoordinator(
		ingestionHandler.GetBufferManager(),
		walManager,
		func(ctx context.Context, streamID common.StreamID, records []*common.ValidatedRecord) error {
			if err := flushRecords(ctx, streamID, records, processingManager, storageManager); err != nil {
				return err
			}
			// Update checkpoint after successful flush
			if len(records) > 0 {
				lastSeq := int64(records[len(records)-1].SequenceNumber)
				if err := checkpointMgr.UpdateStreamCheckpoint(ctx, streamID, 0, lastSeq); err != nil {
					logger.ForComponent("checkpoint").WithError(err).Warn("Failed to update checkpoint")
				}
			}
			return nil
		},
		cfg.Ingestion.FlushInterval,
	)
	flushCoordinator.OnPostFlush = func(ctx context.Context, streamID common.StreamID, segmentPath string) {
		data, err := os.ReadFile(segmentPath)
		if err != nil {
			logger.ForComponent("wal_archiver").WithError(err).Warn("Failed to read sealed WAL segment for archival")
			return
		}
		if err := walArchiver.ArchiveSegment(ctx, streamID, 0, data); err != nil {
			logger.ForComponent("wal_archiver").WithError(err).Warn("Failed to archive WAL segment to S3")
		}
	}
	go flushCoordinator.RunLoop(ctx)

	// Subscription manager for live tail
	subManager := ingestion.NewSubscriptionManager()

	// Query planner and handler
	queryPlanner := query.NewPlanner(storageManager)
	queryHandler := query.NewQueryHandler(queryPlanner, storageManager, redisAdapter, subManager)

	// --- Health tracking ---
	healthTracker := health.NewTracker(db, nodeID)
	healthTracker.RegisterPostgres(db)
	healthTracker.RegisterRedis(redisAdapter)
	healthTracker.RegisterS3(s3Adapter, cfg.Storage.S3.Bucket)
	healthTracker.RegisterIngestionBuffer(func() float64 {
		// Buffer utilization is approximated; individual stream buffers
		// report BackpressureLevel but there's no aggregate API yet.
		return 0.0
	})
	go healthTracker.RunLoop(ctx, 30*time.Second)

	// --- Authentication ---

	// JWT validator (optional)
	jwtValidator, _ := auth.NewJWTValidator(auth.JWTConfig{
		PublicKeyPath: os.Getenv("TTDUST_JWT_PUBLIC_KEY_PATH"),
		Issuer:        "ttdust",
		Audience:      "ttdust-api",
	})

	// Rate limit checker adapter (bridges ratelimit.Limiter → auth.RateLimitChecker)
	rateLimitCheck := auth.RateLimitChecker(func(ctx context.Context, clientID string, category string) (bool, time.Duration) {
		result := rateLimiter.Allow(ctx, clientID, ratelimit.Category(category))
		return result.Allowed, result.RetryAfter
	})

	// Authz checker adapter (bridges auth.Authorizer → auth.AuthzChecker)
	authzCheck := auth.AuthzChecker(func(ctx context.Context, principal *auth.Principal, resourceType, resourceID, action string) error {
		return authorizer.Authorize(ctx, principal, auth.Resource{Type: resourceType, ID: resourceID}, auth.Action(action))
	})

	// gRPC auth interceptor with rate limiting and authorization
	grpcAuth := auth.NewGRPCInterceptor(db, jwtValidator)
	grpcAuth.SetRateLimiter(rateLimitCheck)
	grpcAuth.SetAuthorizer(authzCheck)
	grpcAuth.SetAuthFailureCallback(func(reason, keyPrefix, ip, ua string) {
		securityMetrics.AuthAttemptsTotal.WithLabelValues("api_key", "failure").Inc()
		logger.ForComponent("security::auth").WithField("reason", reason).Warn("gRPC auth failure")
	})

	// REST auth middleware with rate limiting and authorization
	restAuth := auth.NewMiddleware(db, jwtValidator)
	restAuth.SetRateLimiter(rateLimitCheck)
	restAuth.SetAuthorizer(authzCheck)
	restAuth.SetAuthFailureCallback(func(reason, keyPrefix, ip, ua string) {
		securityMetrics.AuthAttemptsTotal.WithLabelValues("api_key", "failure").Inc()
		logger.ForComponent("security::auth").WithField("reason", reason).WithField("ip", ip).Warn("REST auth failure")
	})

	// --- Servers ---

	// gRPC server (data plane) with auth interceptors
	grpcSrv := grpcserver.NewServer(
		cfg.Server.GRPCAddress,
		ingestionHandler,
		queryHandler,
		grpc.ChainUnaryInterceptor(grpcAuth.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(grpcAuth.StreamInterceptor()),
	)
	go func() {
		logger.ForComponent("grpc").Infof("gRPC server listening on %s", cfg.Server.GRPCAddress)
		if err := grpcSrv.Serve(); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// REST server (control plane + health + metrics) with auth middleware
	restSrv := rest.NewServer(cfg.Server.RESTAddress)
	restSrv.SetAuthMiddleware(restAuth)
	restSrv.SetMetricsRecorder(func(endpoint, method string, statusCode int, duration time.Duration) {
		status := fmt.Sprintf("%d", statusCode)
		cpMetrics.RequestsTotal.WithLabelValues(endpoint, method, status).Inc()
		cpMetrics.RequestDuration.WithLabelValues(endpoint, method).Observe(duration.Seconds())
	})

	// Register control plane API
	apiHandler := rest.NewAPIHandler(schemaStore, streamManager, pipelineMgr, healthTracker)
	apiHandler.SetEventCallback(func(eventType, resourceType, resourceID, actor string) {
		switch eventType {
		case "SchemaCreated":
			eventBus.Publish(domain.SchemaCreated{
				SchemaID:  resourceID,
				Actor:     domain.ActorID(actor),
				Timestamp: time.Now(),
			})
		case "StreamCreated":
			eventBus.Publish(domain.StreamCreated{
				StreamID:  resourceID,
				Actor:     domain.ActorID(actor),
				Timestamp: time.Now(),
			})
		}
	})

	// Register REST query endpoints and wire into API handler for /streams/{id}/records routing
	queryRESTHandler := rest.NewQueryRESTHandler(queryPlanner, storageManager)
	apiHandler.SetQueryHandler(queryRESTHandler)

	restSrv.RegisterAPI(apiHandler)

	// Register admin API (config, API keys)
	adminHandler := rest.NewAdminHandler(db)
	restSrv.RegisterAdmin(adminHandler)

	// Wire readiness checker from health tracker with latency
	restSrv.SetReadinessChecker(func() map[string]rest.ComponentCheck {
		statuses := healthTracker.GetStatuses()
		result := make(map[string]rest.ComponentCheck)
		for name, h := range statuses {
			check := rest.ComponentCheck{
				Healthy:   h.Status == common.HealthStatusHealthy,
				LatencyMs: h.LatencyMs,
			}
			if !check.Healthy {
				check.Error = h.Message
			}
			result[name] = check
		}
		return result
	})

	go func() {
		log.Printf("REST server listening on %s", cfg.Server.RESTAddress)
		if err := restSrv.Serve(); err != nil && err.Error() != "http: Server closed" {
			log.Fatalf("REST server error: %v", err)
		}
	}()

	logger.Info("TTDust server started")

	// Wait for shutdown signal or SIGHUP for config reload
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	for {
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				logger.Info("Received SIGHUP, reloading configuration...")
				newCfg := config.DefaultConfig()
				// Hot-reload rate limiter configs
				rateLimiter.UpdateConfigs(ratelimit.DefaultConfigs())
				// Hot-reload log level
				if newCfg.Observability.LogLevel != "" {
					if lvl, err := logrus.ParseLevel(newCfg.Observability.LogLevel); err == nil {
						logger.SetLevel(lvl)
					}
				}
				cpMetrics.ConfigReloadsTotal.Inc()
				logger.Info("Configuration reloaded")
				continue
			}
			logger.Infof("Received signal %v, shutting down...", sig)
			cancel()
		case <-ctx.Done():
		}
		break
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	grpcSrv.GracefulStop()
	if err := restSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("REST server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}

// flushRecords processes and writes records to S3 for a single stream.
func flushRecords(
	ctx context.Context,
	streamID common.StreamID,
	records []*common.ValidatedRecord,
	processingManager *processing.PipelineManager,
	storageManager *storage.Manager,
) error {
	// Run through processing pipeline
	results := processingManager.ProcessRecords(streamID, records)

	// Partition records by time
	batches := make(map[string]*storage.ParquetBatch)
	for _, result := range results {
		if result.Failed {
			continue
		}

		partKey := storage.PartitionKeyFromRecord(result.Record)
		partPath := partKey.PartitionPath()

		batch, ok := batches[partPath]
		if !ok {
			batch = storage.NewParquetBatch(streamID, result.Record.SchemaID, partKey)
			batches[partPath] = batch
		}
		batch.Add(result.Record, result.Payload)
	}

	// Flush each batch to S3
	for _, batch := range batches {
		if err := storageManager.FlushBatch(ctx, batch); err != nil {
			return fmt.Errorf("flush batch for stream %s: %w", streamID, err)
		}
	}

	return nil
}
