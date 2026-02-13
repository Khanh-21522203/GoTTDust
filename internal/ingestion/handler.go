package ingestion

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"GoTTDust/internal/common"
	pb "GoTTDust/internal/genproto/ingestionpb"
	"GoTTDust/internal/schema"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IngestionHandler implements the gRPC IngestionService.
type IngestionHandler struct {
	pb.UnimplementedIngestionServiceServer

	registry      *schema.Registry
	bufferManager *BufferManager
	walManager    *WALManager
	idempotency   IdempotencyChecker
	seqCounters   sync.Map // map[common.StreamID]*atomic.Int64
	maxMsgSize    int
	batchExecutor *BatchExecutor

	// Metrics callbacks (set by observability layer)
	OnReceived  func(streamID string, status string)
	OnValidated func(streamID string, status string)
	OnBuffered  func(streamID string)
	OnWALWrite  func(streamID string)
}

// IdempotencyChecker checks and stores idempotency keys.
type IdempotencyChecker interface {
	Check(ctx context.Context, streamID common.StreamID, key string) (*pb.IngestResponse, bool, error)
	Store(ctx context.Context, streamID common.StreamID, key string, resp *pb.IngestResponse) error
}

// IngestionHandlerConfig holds configuration for the handler.
type IngestionHandlerConfig struct {
	MaxMessageSizeBytes int
	MaxBatchSize        int
	MaxBufferBytes      int64
	FlushInterval       time.Duration
}

// NewIngestionHandler creates a new ingestion handler.
func NewIngestionHandler(
	registry *schema.Registry,
	walManager *WALManager,
	idempotency IdempotencyChecker,
	config IngestionHandlerConfig,
) *IngestionHandler {
	return &IngestionHandler{
		registry:      registry,
		bufferManager: NewBufferManager(config.MaxBatchSize, config.MaxBufferBytes),
		walManager:    walManager,
		idempotency:   idempotency,
		maxMsgSize:    config.MaxMessageSizeBytes,
		OnReceived:    func(string, string) {},
		OnValidated:   func(string, string) {},
		OnBuffered:    func(string) {},
		OnWALWrite:    func(string) {},
	}
}

// IngestStream handles bidirectional streaming ingestion.
func (h *IngestionHandler) IngestStream(stream pb.IngestionService_IngestStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive error: %v", err)
		}

		resp := h.processRecord(stream.Context(), req)

		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "send error: %v", err)
		}
	}
}

// SetBatchExecutor sets the batch executor for async batch ingestion.
func (h *IngestionHandler) SetBatchExecutor(be *BatchExecutor) {
	h.batchExecutor = be
}

// IngestBatch handles batch ingestion from a file source.
func (h *IngestionHandler) IngestBatch(ctx context.Context, req *pb.IngestBatchRequest) (*pb.IngestBatchResponse, error) {
	if req.StreamId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "stream_id is required")
	}
	if req.SchemaId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "schema_id is required")
	}
	if req.SourceUri == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_uri is required")
	}

	if h.batchExecutor == nil {
		return nil, status.Errorf(codes.Unimplemented, "batch ingestion not configured")
	}

	job, err := h.batchExecutor.SubmitJob(ctx, req.StreamId, req.SourceUri, "jsonl")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit batch job: %v", err)
	}

	return &pb.IngestBatchResponse{
		JobId:  job.JobID,
		Status: pb.JobStatus_JOB_STATUS_PENDING,
	}, nil
}

// processRecord handles a single ingestion request through all stages.
func (h *IngestionHandler) processRecord(ctx context.Context, req *pb.IngestRequest) *pb.IngestResponse {
	streamID := common.StreamID(req.StreamId)
	now := time.Now()

	// Stage 1: Receive - validate message
	h.OnReceived(req.StreamId, "received")

	if err := h.validateMessage(req); err != nil {
		h.OnReceived(req.StreamId, "rejected")
		return &pb.IngestResponse{
			Status:       pb.Status_STATUS_VALIDATION_ERROR,
			ErrorMessage: strPtr(err.Error()),
		}
	}

	// Stage 2: Schema Validation
	schemaID := common.SchemaID(req.SchemaId)
	if err := h.registry.Validate(schemaID, req.Payload); err != nil {
		h.OnValidated(req.StreamId, "rejected")
		return &pb.IngestResponse{
			Status:       pb.Status_STATUS_VALIDATION_ERROR,
			ErrorMessage: strPtr(err.Error()),
		}
	}
	h.OnValidated(req.StreamId, "validated")

	// Stage 3: Idempotency Check
	if req.IdempotencyKey != nil && *req.IdempotencyKey != "" {
		if h.idempotency != nil {
			cached, found, err := h.idempotency.Check(ctx, streamID, *req.IdempotencyKey)
			if err == nil && found {
				return cached
			}
		}
	}

	// Assign record ID and sequence number
	recordID := common.NewRecordID()
	seqNum := h.nextSequence(streamID)

	record := &common.ValidatedRecord{
		RecordID:       recordID,
		StreamID:       streamID,
		SchemaID:       schemaID,
		SequenceNumber: seqNum,
		Payload:        req.Payload,
		Metadata:       req.Metadata,
		IngestedAt:     now,
	}
	if req.IdempotencyKey != nil {
		record.IdempotencyKey = *req.IdempotencyKey
	}

	// Stage 4: Buffer
	buf := h.bufferManager.GetOrCreate(streamID)
	level := buf.BackpressureLevel()

	if level == BackpressureCritical {
		return &pb.IngestResponse{
			Status:       pb.Status_STATUS_RATE_LIMITED,
			ErrorMessage: strPtr("server is overloaded, please retry with backoff"),
		}
	}
	if level == BackpressureOverload {
		return &pb.IngestResponse{
			Status:       pb.Status_STATUS_RATE_LIMITED,
			ErrorMessage: strPtr("buffer full, connection should be dropped"),
		}
	}

	if err := buf.Add(record); err != nil {
		return &pb.IngestResponse{
			Status:       pb.Status_STATUS_RATE_LIMITED,
			ErrorMessage: strPtr(err.Error()),
		}
	}
	h.OnBuffered(req.StreamId)

	// Stage 5: WAL Write (fsync for durability)
	if err := h.walManager.Write(streamID, record); err != nil {
		return &pb.IngestResponse{
			Status:       pb.Status_STATUS_INTERNAL_ERROR,
			ErrorMessage: strPtr(fmt.Sprintf("WAL write failed: %v", err)),
		}
	}
	h.OnWALWrite(req.StreamId)

	// Build response
	resp := &pb.IngestResponse{
		RecordId:           string(recordID),
		SequenceNumber:     int64(seqNum),
		TimestampUnixMicros: now.UnixMicro(),
		Status:             pb.Status_STATUS_OK,
	}

	// Store idempotency key
	if req.IdempotencyKey != nil && *req.IdempotencyKey != "" && h.idempotency != nil {
		_ = h.idempotency.Store(ctx, streamID, *req.IdempotencyKey, resp)
	}

	return resp
}

// validateMessage performs Stage 1 validation on the raw message.
func (h *IngestionHandler) validateMessage(req *pb.IngestRequest) error {
	if req.StreamId == "" {
		return fmt.Errorf("stream_id is required")
	}
	if req.SchemaId == "" {
		return fmt.Errorf("schema_id is required")
	}
	if len(req.Payload) == 0 {
		return fmt.Errorf("payload is required")
	}
	if h.maxMsgSize > 0 && len(req.Payload) > h.maxMsgSize {
		return fmt.Errorf("payload exceeds maximum size of %d bytes", h.maxMsgSize)
	}
	return nil
}

// nextSequence returns the next sequence number for a stream.
func (h *IngestionHandler) nextSequence(streamID common.StreamID) common.SequenceNumber {
	val, _ := h.seqCounters.LoadOrStore(streamID, &atomic.Int64{})
	counter := val.(*atomic.Int64)
	return common.SequenceNumber(counter.Add(1))
}

// GetBufferManager returns the buffer manager for external flush coordination.
func (h *IngestionHandler) GetBufferManager() *BufferManager {
	return h.bufferManager
}

func strPtr(s string) *string {
	return &s
}
