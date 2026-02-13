package domain

import (
	"encoding/json"
	"time"
)

// --- Schema Aggregate ---

// SchemaAggregate is the aggregate root for schemas.
type SchemaAggregate struct {
	ID          string               `json:"id"`
	Name        string               `json:"name"`
	Namespace   string               `json:"namespace"`
	Description *string              `json:"description,omitempty"`
	Versions    []SchemaVersionEntity `json:"versions"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
	CreatedBy   ActorID              `json:"created_by"`
	DeletedAt   *time.Time           `json:"deleted_at,omitempty"`
}

// Validate checks schema aggregate invariants.
func (s *SchemaAggregate) Validate() error {
	errs := &ValidationErrors{}

	if _, err := NewSchemaName(s.Name); err != nil {
		errs.Add("name", err.Error())
	}
	if _, err := NewNamespace(s.Namespace); err != nil {
		errs.Add("namespace", err.Error())
	}
	if len(s.Versions) == 0 {
		errs.Add("versions", "schema must have at least one version")
	}

	activeCount := 0
	for _, v := range s.Versions {
		if v.Status == VersionActive {
			activeCount++
		}
	}
	if activeCount == 0 && len(s.Versions) > 0 {
		errs.Add("versions", "schema must have at least one active version")
	}

	return errs.OrNil()
}

// LatestVersion returns the latest active version.
func (s *SchemaAggregate) LatestVersion() *SchemaVersionEntity {
	var latest *SchemaVersionEntity
	for i := range s.Versions {
		v := &s.Versions[i]
		if v.Status == VersionActive {
			if latest == nil || v.Version > latest.Version {
				latest = v
			}
		}
	}
	return latest
}

// IsDeleted returns true if the schema is soft-deleted.
func (s *SchemaAggregate) IsDeleted() bool {
	return s.DeletedAt != nil
}

// MarkDeleted soft-deletes the schema.
func (s *SchemaAggregate) MarkDeleted(ts time.Time) {
	s.DeletedAt = &ts
}

// SchemaVersionEntity is a specific version of a schema.
type SchemaVersionEntity struct {
	ID                string            `json:"id"`
	SchemaID          string            `json:"schema_id"`
	Version           int               `json:"version"`
	Definition        json.RawMessage   `json:"definition"`
	Fingerprint       string            `json:"fingerprint"`
	CompatibilityMode CompatibilityMode `json:"compatibility_mode"`
	Status            VersionStatus     `json:"status"`
	CreatedAt         time.Time         `json:"created_at"`
	CreatedBy         ActorID           `json:"created_by"`
}

// --- Stream Aggregate ---

// StreamAggregate is the aggregate root for streams.
type StreamAggregate struct {
	ID              string               `json:"id"`
	Name            string               `json:"name"`
	Description     *string              `json:"description,omitempty"`
	SchemaID        string               `json:"schema_id"`
	Status          StreamStatus         `json:"status"`
	RetentionPolicy RetentionPolicyValue `json:"retention_policy"`
	PartitionConfig PartitionConfigValue `json:"partition_config"`
	Pipeline        *PipelineAggregate   `json:"pipeline,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
	CreatedBy       ActorID              `json:"created_by"`
	DeletedAt       *time.Time           `json:"deleted_at,omitempty"`
}

// Validate checks stream aggregate invariants.
func (s *StreamAggregate) Validate() error {
	errs := &ValidationErrors{}

	if _, err := NewStreamName(s.Name); err != nil {
		errs.Add("name", err.Error())
	}
	if s.SchemaID == "" {
		errs.Add("schema_id", "schema_id is required")
	}
	if err := s.RetentionPolicy.Validate(); err != nil {
		if ve, ok := err.(*ValidationErrors); ok {
			for _, e := range ve.Errors {
				errs.Errors = append(errs.Errors, e)
			}
		}
	}

	return errs.OrNil()
}

// IsDeleted returns true if the stream is soft-deleted.
func (s *StreamAggregate) IsDeleted() bool {
	return s.DeletedAt != nil
}

// MarkDeleted soft-deletes the stream.
func (s *StreamAggregate) MarkDeleted(ts time.Time) {
	s.DeletedAt = &ts
}

// TransitionStatus validates and applies a status transition.
func (s *StreamAggregate) TransitionStatus(target StreamStatus) error {
	newStatus, err := s.Status.TransitionTo(target)
	if err != nil {
		return err
	}
	s.Status = newStatus
	s.UpdatedAt = time.Now()
	return nil
}

// --- Record Entity ---

// RecordEntity represents a single data record.
type RecordEntity struct {
	ID           string         `json:"id"`
	StreamID     string         `json:"stream_id"`
	Sequence     int64          `json:"sequence"`
	PartitionKey string         `json:"partition_key"`
	Payload      RecordPayload  `json:"payload"`
	Metadata     RecordMetadata `json:"metadata"`
	IngestedAt   time.Time      `json:"ingested_at"`
}

// RecordPayload is the validated JSON payload.
type RecordPayload struct {
	Data      json.RawMessage `json:"data"`
	SizeBytes int             `json:"size_bytes"`
}

// RecordMetadata holds record context.
type RecordMetadata struct {
	SchemaVersion  int    `json:"schema_version"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	ProducerID     string `json:"producer_id,omitempty"`
	TraceID        string `json:"trace_id,omitempty"`
}

// Validate checks record invariants.
func (r *RecordEntity) Validate() error {
	errs := &ValidationErrors{}
	if r.StreamID == "" {
		errs.Add("stream_id", "stream_id is required")
	}
	if r.Payload.SizeBytes > 1048576 {
		errs.Add("payload", "payload must be <= 1MB")
	}
	return errs.OrNil()
}

// --- Pipeline Aggregate ---

// PipelineAggregate represents a processing pipeline.
type PipelineAggregate struct {
	ID        string               `json:"id"`
	Name      string               `json:"name"`
	StreamID  string               `json:"stream_id"`
	Status    PipelineStatus       `json:"status"`
	Stages    []PipelineStageEntity `json:"stages"`
	DLQConfig *DLQConfigValue      `json:"dlq_config,omitempty"`
	CreatedAt time.Time            `json:"created_at"`
	UpdatedAt time.Time            `json:"updated_at"`
}

// Validate checks pipeline invariants.
func (p *PipelineAggregate) Validate() error {
	errs := &ValidationErrors{}
	if len(p.Stages) == 0 {
		errs.Add("stages", "pipeline must have at least one stage")
	}
	if len(p.Stages) > 10 {
		errs.Add("stages", "pipeline must have at most 10 stages")
	}

	seen := make(map[int]bool)
	for i, stage := range p.Stages {
		if !stage.StageType.IsValid() {
			errs.Add("stages", "unsupported stage type: "+string(stage.StageType))
		}
		expectedOrder := i + 1
		if stage.Order != expectedOrder {
			errs.Add("stages", "stage order must be contiguous starting from 1")
		}
		if seen[stage.Order] {
			errs.Add("stages", "duplicate stage order")
		}
		seen[stage.Order] = true
	}

	return errs.OrNil()
}

// PipelineStageEntity is a single stage in a pipeline.
type PipelineStageEntity struct {
	ID         string          `json:"id"`
	PipelineID string          `json:"pipeline_id"`
	Order      int             `json:"order"`
	StageType  StageType       `json:"stage_type"`
	Config     json.RawMessage `json:"config"`
	Enabled    bool            `json:"enabled"`
}

// --- Stage Config Types ---

// FieldProjectionConfig for field projection stages.
type FieldProjectionConfig struct {
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
}

// TimestampNormalizationConfig for timestamp normalization stages.
type TimestampNormalizationConfig struct {
	SourceField   string           `json:"source_field"`
	SourceFormats []TimestampFormat `json:"source_formats"`
	TargetFormat  TimestampFormat   `json:"target_format"`
}

// TypeCoercionConfig for type coercion stages.
type TypeCoercionConfig struct {
	Rules []CoercionRule `json:"rules"`
}

// CoercionRule defines a single type coercion.
type CoercionRule struct {
	Field      string        `json:"field"`
	TargetType DataType      `json:"target_type"`
	OnError    ErrorBehavior `json:"on_error"`
}

// --- Soft Delete Interface ---

// SoftDeletable is implemented by entities that support soft deletion.
type SoftDeletable interface {
	IsDeleted() bool
	MarkDeleted(ts time.Time)
}
