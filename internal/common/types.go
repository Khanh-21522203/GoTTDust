package common

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// StreamID uniquely identifies a data stream.
type StreamID string

// RecordID uniquely identifies a record.
type RecordID string

// SchemaID uniquely identifies a schema.
type SchemaID string

// SequenceNumber is a per-stream monotonically increasing number.
type SequenceNumber int64

// NewRecordID generates a new unique record ID.
func NewRecordID() RecordID {
	return RecordID(uuid.New().String())
}

// ValidatedRecord is a record that has passed schema validation.
type ValidatedRecord struct {
	RecordID       RecordID
	StreamID       StreamID
	SchemaID       SchemaID
	SequenceNumber SequenceNumber
	Payload        []byte
	Metadata       map[string]string
	IdempotencyKey string
	IngestedAt     time.Time
}

// DLQRecord represents a record that failed processing.
type DLQRecord struct {
	OriginalRecord ValidatedRecord
	Error          DLQError
	Metadata       DLQMetadata
}

// DLQError describes why a record failed processing.
type DLQError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// DLQMetadata contains context about the DLQ entry.
type DLQMetadata struct {
	SourceStream      StreamID  `json:"source_stream"`
	OriginalTimestamp time.Time `json:"original_timestamp"`
	FailureTimestamp  time.Time `json:"failure_timestamp"`
	RetryCount        int       `json:"retry_count"`
}

// PipelineConfig defines a processing pipeline for a stream.
type PipelineConfig struct {
	PipelineID string        `json:"pipeline_id"`
	StreamID   StreamID      `json:"stream_id"`
	Stages     []StageConfig `json:"stages"`
	DLQConfig  DLQConfig     `json:"dlq_config"`
}

// StageConfig defines a single processing stage.
type StageConfig struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

// DLQConfig configures the dead letter queue for a pipeline.
type DLQConfig struct {
	Enabled       bool     `json:"enabled"`
	StreamID      StreamID `json:"stream_id"`
	RetentionDays int      `json:"retention_days"`
}

// PartitionKey represents a time-based partition.
type PartitionKey struct {
	StreamID StreamID
	Year     int
	Month    int
	Day      int
	Hour     int
}

// PartitionPath returns the S3 path for this partition.
func (p PartitionKey) PartitionPath() string {
	return fmt.Sprintf("streams/%s/partitions/year=%04d/month=%02d/day=%02d/hour=%02d",
		p.StreamID, p.Year, p.Month, p.Day, p.Hour)
}

// ParsePartitionPath parses a partition path string back into a PartitionKey.
// Expected format: streams/{stream}/partitions/year=YYYY/month=MM/day=DD/hour=HH
func ParsePartitionPath(path string) (PartitionKey, error) {
	var pk PartitionKey
	var streamID string
	_, err := fmt.Sscanf(path, "streams/%s", &streamID)
	if err != nil {
		return pk, fmt.Errorf("invalid partition path: %s", path)
	}
	// Manual parse since Sscanf stops at /
	parts := make(map[string]string)
	for _, seg := range splitPath(path) {
		if len(seg) > 5 && seg[4] == '=' {
			parts[seg[:4]] = seg[5:]
		}
		if len(seg) > 6 && seg[5] == '=' {
			parts[seg[:5]] = seg[6:]
		}
	}
	// Extract stream ID (second segment)
	segs := splitPath(path)
	if len(segs) >= 2 {
		pk.StreamID = StreamID(segs[1])
	}
	fmt.Sscanf(parts["year"], "%d", &pk.Year)
	fmt.Sscanf(parts["month"], "%d", &pk.Month)
	fmt.Sscanf(parts["day"], "%d", &pk.Day)
	fmt.Sscanf(parts["hour"], "%d", &pk.Hour)
	return pk, nil
}

func splitPath(path string) []string {
	var parts []string
	current := ""
	for _, c := range path {
		if c == '/' {
			if current != "" {
				parts = append(parts, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// PartitionKeyFromTime creates a PartitionKey from a timestamp.
func PartitionKeyFromTime(streamID StreamID, t time.Time) PartitionKey {
	utc := t.UTC()
	return PartitionKey{
		StreamID: streamID,
		Year:     utc.Year(),
		Month:    int(utc.Month()),
		Day:      utc.Day(),
		Hour:     utc.Hour(),
	}
}
