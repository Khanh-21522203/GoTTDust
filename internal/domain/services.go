package domain

import (
	"context"
	"encoding/json"
	"time"
)

// --- Domain Service Interfaces ---

// SchemaService manages schema lifecycle.
type SchemaService interface {
	CreateSchema(ctx context.Context, name SchemaName, namespace Namespace, definition json.RawMessage, actor ActorID) (*SchemaAggregate, error)
	AddVersion(ctx context.Context, schemaID string, definition json.RawMessage, compat CompatibilityMode, actor ActorID) (*SchemaVersionEntity, error)
	DeprecateVersion(ctx context.Context, schemaID string, version int, actor ActorID) error
	DeleteSchema(ctx context.Context, schemaID string, actor ActorID) error
	ValidateRecord(ctx context.Context, schemaID string, version int, payload []byte) error
	CheckCompatibility(ctx context.Context, schemaID string, newDef json.RawMessage, mode CompatibilityMode) error
}

// StreamService manages stream lifecycle.
type StreamService interface {
	CreateStream(ctx context.Context, name StreamName, schemaID string, config StreamConfig, actor ActorID) (*StreamAggregate, error)
	UpdateStatus(ctx context.Context, streamID string, newStatus StreamStatus, actor ActorID) (*StreamAggregate, error)
	GetStream(ctx context.Context, streamID string) (*StreamAggregate, error)
	ListStreams(ctx context.Context, filter StreamFilter, pagination Pagination) (*Page, error)
	DeleteStream(ctx context.Context, streamID string, actor ActorID) error
}

// IngestionService handles record ingestion.
type IngestionService interface {
	IngestRecord(ctx context.Context, streamID string, schemaID string, payload json.RawMessage, meta IngestMetadata) (*IngestResult, error)
	CheckIdempotency(ctx context.Context, streamID string, key string) (*IngestResult, bool, error)
}

// ProcessingService executes pipelines on records.
type ProcessingService interface {
	ProcessRecord(ctx context.Context, pipelineID string, record *RecordEntity) (*ProcessingResult, error)
}

// --- Supporting Types ---

// StreamConfig holds stream creation configuration.
type StreamConfig struct {
	Description     string
	RetentionPolicy RetentionPolicyValue
	PartitionConfig PartitionConfigValue
}

// RetentionPolicyValue is the domain value for retention.
type RetentionPolicyValue struct {
	PolicyType   RetentionType `json:"policy_type"`
	DurationDays *int          `json:"duration_days,omitempty"`
	MaxBytes     *int64        `json:"max_bytes,omitempty"`
	MinRecords   int64         `json:"min_records"`
}

// Validate checks retention policy invariants.
func (r RetentionPolicyValue) Validate() error {
	errs := &ValidationErrors{}
	if r.DurationDays != nil && *r.DurationDays > 3650 {
		errs.Add("retention_policy.duration_days", "must be <= 3650 (10 years)")
	}
	if r.DurationDays != nil && *r.DurationDays < 1 {
		errs.Add("retention_policy.duration_days", "must be >= 1")
	}
	return errs.OrNil()
}

// PartitionConfigValue is the domain value for partitioning.
type PartitionConfigValue struct {
	PartitionBy PartitionStrategy `json:"partition_by"`
	Granularity TimeGranularity   `json:"granularity"`
	KeyField    *string           `json:"key_field,omitempty"`
}

// IngestMetadata holds optional ingestion metadata.
type IngestMetadata struct {
	IdempotencyKey string
	ProducerID     string
	TraceID        string
}

// IngestResult is the result of a successful ingestion.
type IngestResult struct {
	RecordID   string
	Sequence   int64
	IngestedAt time.Time
}

// ProcessingResult is the outcome of pipeline processing.
type ProcessingResult struct {
	Success  bool
	Record   *RecordEntity
	Error    *ProcessingError
	StageIdx int
}

// StreamFilter for listing streams.
type StreamFilter struct {
	Status   *StreamStatus
	SchemaID *string
}

// Pagination parameters.
type Pagination struct {
	Limit  int
	Cursor string
}

// DefaultPagination returns defaults for a resource type.
func DefaultPagination(resourceType string) Pagination {
	switch resourceType {
	case "records":
		return Pagination{Limit: 1000}
	default:
		return Pagination{Limit: 20}
	}
}

// MaxLimit returns the maximum limit for a resource type.
func MaxLimit(resourceType string) int {
	switch resourceType {
	case "records":
		return 10000
	default:
		return 100
	}
}

// ClampLimit clamps a limit to the valid range for a resource type.
func ClampLimit(limit int, resourceType string) int {
	def := DefaultPagination(resourceType)
	max := MaxLimit(resourceType)
	if limit <= 0 {
		return def.Limit
	}
	if limit > max {
		return max
	}
	return limit
}

// Page is a paginated result.
type Page struct {
	Items      interface{} `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
	TotalCount int64       `json:"total_count"`
}

// --- DLQ Config ---

// DLQConfigValue is the domain value for dead letter queue configuration.
type DLQConfigValue struct {
	Enabled       bool   `json:"enabled"`
	StreamID      string `json:"stream_id,omitempty"`
	RetentionDays int    `json:"retention_days"`
}
