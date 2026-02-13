package domain

import (
	"fmt"
	"regexp"
	"strings"
)

// --- Validated Name Types ---

var (
	namePattern      = regexp.MustCompile(`^[a-z][a-z0-9-]{2,62}$`)
	namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
)

// SchemaName is a validated schema name.
type SchemaName struct {
	value string
}

// NewSchemaName creates a validated schema name.
func NewSchemaName(name string) (SchemaName, error) {
	if !namePattern.MatchString(name) {
		return SchemaName{}, &ValidationError{
			Field:      "name",
			Message:    "must match pattern ^[a-z][a-z0-9-]{2,62}$",
			Constraint: "pattern",
		}
	}
	return SchemaName{value: name}, nil
}

// Value returns the underlying string.
func (n SchemaName) Value() string { return n.value }

// String implements fmt.Stringer.
func (n SchemaName) String() string { return n.value }

// StreamName is a validated stream name.
type StreamName struct {
	value string
}

// NewStreamName creates a validated stream name.
func NewStreamName(name string) (StreamName, error) {
	if !namePattern.MatchString(name) {
		return StreamName{}, &ValidationError{
			Field:      "name",
			Message:    "must match pattern ^[a-z][a-z0-9-]{2,62}$",
			Constraint: "pattern",
		}
	}
	return StreamName{value: name}, nil
}

// Value returns the underlying string.
func (n StreamName) Value() string { return n.value }

// String implements fmt.Stringer.
func (n StreamName) String() string { return n.value }

// Namespace is a validated namespace for grouping schemas.
type Namespace struct {
	value string
}

// NewNamespace creates a validated namespace.
func NewNamespace(ns string) (Namespace, error) {
	if ns == "" {
		return Namespace{value: "default"}, nil
	}
	if !namespacePattern.MatchString(ns) {
		return Namespace{}, &ValidationError{
			Field:      "namespace",
			Message:    "must match pattern ^[a-z][a-z0-9-]{0,62}$",
			Constraint: "pattern",
		}
	}
	return Namespace{value: ns}, nil
}

// Value returns the underlying string.
func (n Namespace) Value() string { return n.value }

// String implements fmt.Stringer.
func (n Namespace) String() string { return n.value }

// ActorID identifies the actor performing an action.
type ActorID string

// TraceID is a distributed tracing identifier.
type TraceID string

// FieldPath represents a dot-separated path to a nested field.
type FieldPath struct {
	parts []string
}

// ParseFieldPath parses a dot-separated field path.
func ParseFieldPath(path string) (FieldPath, error) {
	if path == "" {
		return FieldPath{}, &ValidationError{
			Field:   "field_path",
			Message: "field path cannot be empty",
		}
	}
	parts := strings.Split(path, ".")
	for _, p := range parts {
		if p == "" {
			return FieldPath{}, &ValidationError{
				Field:   "field_path",
				Message: fmt.Sprintf("invalid field path: %q contains empty segment", path),
			}
		}
	}
	return FieldPath{parts: parts}, nil
}

// Parts returns the path segments.
func (f FieldPath) Parts() []string { return f.parts }

// String returns the dot-separated path.
func (f FieldPath) String() string { return strings.Join(f.parts, ".") }

// --- Enums ---

// CompatibilityMode for schema evolution.
type CompatibilityMode string

const (
	CompatBackward CompatibilityMode = "backward"
	CompatForward  CompatibilityMode = "forward"
	CompatFull     CompatibilityMode = "full"
	CompatNone     CompatibilityMode = "none"
)

// VersionStatus is the lifecycle status of a schema version.
type VersionStatus string

const (
	VersionActive     VersionStatus = "active"
	VersionDeprecated VersionStatus = "deprecated"
	VersionDeleted    VersionStatus = "deleted"
)

// StreamStatus is the lifecycle status of a stream.
type StreamStatus string

const (
	StreamStatusCreating StreamStatus = "creating"
	StreamStatusActive   StreamStatus = "active"
	StreamStatusPaused   StreamStatus = "paused"
	StreamStatusDeleting StreamStatus = "deleting"
	StreamStatusDeleted  StreamStatus = "deleted"
)

// CanTransitionTo checks if a stream status transition is valid.
func (s StreamStatus) CanTransitionTo(target StreamStatus) bool {
	switch s {
	case StreamStatusCreating:
		return target == StreamStatusActive
	case StreamStatusActive:
		return target == StreamStatusPaused || target == StreamStatusDeleting
	case StreamStatusPaused:
		return target == StreamStatusActive || target == StreamStatusDeleting
	case StreamStatusDeleting:
		return target == StreamStatusDeleted
	default:
		return false
	}
}

// TransitionTo validates and returns the new status.
func (s StreamStatus) TransitionTo(target StreamStatus) (StreamStatus, error) {
	if s.CanTransitionTo(target) {
		return target, nil
	}
	return s, &DomainError{
		Kind:    ErrInvalidStateTransition,
		Message: fmt.Sprintf("cannot transition from %s to %s", s, target),
	}
}

// PipelineStatus is the status of a processing pipeline.
type PipelineStatus string

const (
	PipelineActive   PipelineStatus = "active"
	PipelineDisabled PipelineStatus = "disabled"
)

// RetentionType defines how data retention is measured.
type RetentionType string

const (
	RetentionTimeBased RetentionType = "time_based"
	RetentionSizeBased RetentionType = "size_based"
)

// PartitionStrategy defines how records are partitioned.
type PartitionStrategy string

const (
	PartitionByTime PartitionStrategy = "time"
	PartitionByKey  PartitionStrategy = "key"
)

// TimeGranularity defines partition time granularity.
type TimeGranularity string

const (
	GranularityHourly TimeGranularity = "hourly"
	GranularityDaily  TimeGranularity = "daily"
)

// StageType defines the type of a pipeline stage.
type StageType string

const (
	StageFieldProjection        StageType = "field_projection"
	StageTimestampNormalization StageType = "timestamp_normalization"
	StageTypeCoercion           StageType = "type_coercion"
)

// IsValid returns true if the stage type is supported.
func (st StageType) IsValid() bool {
	switch st {
	case StageFieldProjection, StageTimestampNormalization, StageTypeCoercion:
		return true
	default:
		return false
	}
}

// DataType for type coercion.
type DataType string

const (
	DataTypeString    DataType = "string"
	DataTypeInt64     DataType = "int64"
	DataTypeFloat64   DataType = "float64"
	DataTypeBool      DataType = "bool"
	DataTypeTimestamp DataType = "timestamp"
)

// ErrorBehavior defines what to do when a processing error occurs.
type ErrorBehavior string

const (
	ErrorBehaviorFail       ErrorBehavior = "fail"
	ErrorBehaviorUseDefault ErrorBehavior = "use_default"
	ErrorBehaviorUseNull    ErrorBehavior = "use_null"
)

// TimestampFormat defines supported timestamp formats.
type TimestampFormat string

const (
	TimestampUnixSeconds   TimestampFormat = "unix_seconds"
	TimestampUnixMillis    TimestampFormat = "unix_millis"
	TimestampUnixMicros    TimestampFormat = "unix_micros"
	TimestampISO8601       TimestampFormat = "iso8601"
	TimestampRFC3339       TimestampFormat = "rfc3339"
	TimestampRFC3339Micros TimestampFormat = "rfc3339_micros"
)

// AuditAction defines auditable actions.
type AuditAction string

const (
	AuditCreate AuditAction = "create"
	AuditUpdate AuditAction = "update"
	AuditDelete AuditAction = "delete"
	AuditRead   AuditAction = "read"
)

// ResourceType defines auditable resource types.
type ResourceType string

const (
	ResourceSchema        ResourceType = "schema"
	ResourceSchemaVersion ResourceType = "schema_version"
	ResourceStream        ResourceType = "stream"
	ResourcePipeline      ResourceType = "pipeline"
	ResourceApiKey        ResourceType = "api_key"
)
