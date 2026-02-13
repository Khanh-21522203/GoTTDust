package common

import (
	"encoding/json"
	"time"
)

// --- Schema entities ---

// Schema represents a schema definition in the control plane.
type Schema struct {
	SchemaID    string    `json:"schema_id" db:"schema_id"`
	Name        string    `json:"name" db:"name"`
	Namespace   string    `json:"namespace" db:"namespace"`
	Description string    `json:"description,omitempty" db:"description"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
	CreatedBy   string    `json:"created_by" db:"created_by"`
}

// SchemaVersion represents a versioned schema definition.
type SchemaVersion struct {
	VersionID         string          `json:"version_id" db:"version_id"`
	SchemaID          string          `json:"schema_id" db:"schema_id"`
	Version           int             `json:"version" db:"version"`
	Definition        json.RawMessage `json:"definition" db:"definition"`
	Fingerprint       string          `json:"fingerprint" db:"fingerprint"`
	CompatibilityMode string          `json:"compatibility_mode" db:"compatibility_mode"`
	Status            string          `json:"status" db:"status"`
	CreatedAt         time.Time       `json:"created_at" db:"created_at"`
	CreatedBy         string          `json:"created_by" db:"created_by"`
}

// Schema version statuses
const (
	SchemaStatusActive     = "active"
	SchemaStatusDeprecated = "deprecated"
	SchemaStatusDeleted    = "deleted"
)

// Compatibility modes
const (
	CompatBackward = "BACKWARD"
	CompatForward  = "FORWARD"
	CompatFull     = "FULL"
	CompatNone     = "NONE"
)

// --- Stream entities ---

// Stream represents a data stream in the control plane.
type Stream struct {
	StreamID        string          `json:"stream_id" db:"stream_id"`
	Name            string          `json:"name" db:"name"`
	Description     string          `json:"description,omitempty" db:"description"`
	SchemaID        string          `json:"schema_id" db:"schema_id"`
	Status          string          `json:"status" db:"status"`
	RetentionPolicy json.RawMessage `json:"retention_policy" db:"retention_policy"`
	PartitionConfig json.RawMessage `json:"partition_config" db:"partition_config"`
	CreatedAt       time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at" db:"updated_at"`
	CreatedBy       string          `json:"created_by" db:"created_by"`
}

// Stream statuses (state machine)
const (
	StreamStatusCreating = "creating"
	StreamStatusActive   = "active"
	StreamStatusPaused   = "paused"
	StreamStatusDeleting = "deleting"
	StreamStatusDeleted  = "deleted"
)

// RetentionPolicy defines data retention for a stream.
type RetentionPolicy struct {
	Type         string `json:"type"`
	DurationDays int    `json:"duration_days"`
	MinRecords   int64  `json:"min_records,omitempty"`
	MaxBytes     int64  `json:"max_bytes,omitempty"`
}

// PartitionConfig defines partitioning for a stream.
type PartitionConfig struct {
	PartitionBy string `json:"partition_by"`
	Granularity string `json:"granularity"`
	KeyField    string `json:"key_field,omitempty"`
}

// StreamStats holds daily statistics for a stream.
type StreamStats struct {
	StreamID        string    `json:"stream_id" db:"stream_id"`
	StatDate        time.Time `json:"stat_date" db:"stat_date"`
	RecordsIngested int64     `json:"records_ingested" db:"records_ingested"`
	BytesIngested   int64     `json:"bytes_ingested" db:"bytes_ingested"`
	RecordsQueried  int64     `json:"records_queried" db:"records_queried"`
	StorageBytes    int64     `json:"storage_bytes" db:"storage_bytes"`
}

// --- Pipeline entities ---

// Pipeline represents a processing pipeline in the control plane.
type Pipeline struct {
	PipelineID  string    `json:"pipeline_id" db:"pipeline_id"`
	Name        string    `json:"name" db:"name"`
	StreamID    string    `json:"stream_id" db:"stream_id"`
	Status      string    `json:"status" db:"status"`
	DLQStreamID string    `json:"dlq_stream_id,omitempty" db:"dlq_stream_id"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// Pipeline statuses
const (
	PipelineStatusActive   = "active"
	PipelineStatusDisabled = "disabled"
)

// PipelineStage represents a single stage in a pipeline.
type PipelineStage struct {
	StageID    string          `json:"stage_id" db:"stage_id"`
	PipelineID string          `json:"pipeline_id" db:"pipeline_id"`
	StageOrder int             `json:"stage_order" db:"stage_order"`
	StageType  string          `json:"stage_type" db:"stage_type"`
	Config     json.RawMessage `json:"config" db:"config"`
	Enabled    bool            `json:"enabled" db:"enabled"`
}

// --- System entities ---

// ComponentHealth tracks health of system components.
type ComponentHealth struct {
	ComponentID   string          `json:"component_id" db:"component_id"`
	NodeID        string          `json:"node_id" db:"node_id"`
	ComponentType string          `json:"component_type" db:"component_type"`
	Status        string          `json:"status" db:"status"`
	LastHeartbeat time.Time       `json:"last_heartbeat" db:"last_heartbeat"`
	Details       json.RawMessage `json:"details,omitempty" db:"details"`
	LatencyMs     int64           `json:"latency_ms"`
	Message       string          `json:"message,omitempty"`
}

// Health statuses
const (
	HealthStatusHealthy   = "healthy"
	HealthStatusDegraded  = "degraded"
	HealthStatusUnhealthy = "unhealthy"
)

// SystemConfig represents a dynamic configuration entry.
type SystemConfig struct {
	ConfigKey   string          `json:"config_key" db:"config_key"`
	ConfigValue json.RawMessage `json:"config_value" db:"config_value"`
	Description string          `json:"description,omitempty" db:"description"`
	UpdatedAt   time.Time       `json:"updated_at" db:"updated_at"`
	UpdatedBy   string          `json:"updated_by" db:"updated_by"`
}

// ApiKey represents an API key for authentication.
type ApiKey struct {
	KeyID             string          `json:"key_id" db:"key_id"`
	KeyHash           string          `json:"-" db:"key_hash"`
	Name              string          `json:"name" db:"name"`
	Role              string          `json:"role" db:"role"`
	StreamPermissions json.RawMessage `json:"stream_permissions" db:"stream_permissions"`
	CreatedAt         time.Time       `json:"created_at" db:"created_at"`
	ExpiresAt         *time.Time      `json:"expires_at,omitempty" db:"expires_at"`
	LastUsedAt        *time.Time      `json:"last_used_at,omitempty" db:"last_used_at"`
	CreatedBy         string          `json:"created_by" db:"created_by"`
}

// RBAC roles
const (
	RoleAdmin  = "admin"
	RoleWriter = "writer"
	RoleReader = "reader"
)

// AuditLog represents an audit log entry.
type AuditLog struct {
	LogID        string          `json:"log_id" db:"log_id"`
	Timestamp    time.Time       `json:"timestamp" db:"timestamp"`
	Actor        string          `json:"actor" db:"actor"`
	Action       string          `json:"action" db:"action"`
	ResourceType string          `json:"resource_type" db:"resource_type"`
	ResourceID   string          `json:"resource_id,omitempty" db:"resource_id"`
	Details      json.RawMessage `json:"details,omitempty" db:"details"`
	IPAddress    string          `json:"ip_address,omitempty" db:"ip_address"`
	TraceID      string          `json:"trace_id,omitempty" db:"trace_id"`
}
