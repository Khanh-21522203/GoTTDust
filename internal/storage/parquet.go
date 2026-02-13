package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"GoTTDust/internal/common"
)

// ParquetRecord represents a single record in the Parquet file schema.
// Columns: _record_id, _stream_id, _sequence, _ingested_at, _partition_key, payload
type ParquetRecord struct {
	RecordID     string `json:"_record_id"`
	StreamID     string `json:"_stream_id"`
	Sequence     int64  `json:"_sequence"`
	IngestedAt   int64  `json:"_ingested_at"` // TIMESTAMP_MICROS
	PartitionKey string `json:"_partition_key"`
	Payload      []byte `json:"payload"` // JSON bytes
}

// ParquetFileMetadata holds Parquet file-level metadata.
type ParquetFileMetadata struct {
	SchemaID      string `json:"ttdust.schema_id"`
	SchemaVersion int    `json:"ttdust.schema_version"`
	MinSequence   int64  `json:"ttdust.min_sequence"`
	MaxSequence   int64  `json:"ttdust.max_sequence"`
	RecordCount   int64  `json:"ttdust.record_count"`
	CreatedAt     string `json:"ttdust.created_at"` // ISO8601
}

// ParquetBatch collects records for writing to a single Parquet file.
type ParquetBatch struct {
	StreamID  common.StreamID
	SchemaID  common.SchemaID
	Records   []ParquetRecord
	Metadata  ParquetFileMetadata
	Partition common.PartitionKey
}

// NewParquetBatch creates a new batch for a stream partition.
func NewParquetBatch(streamID common.StreamID, schemaID common.SchemaID, partition common.PartitionKey) *ParquetBatch {
	return &ParquetBatch{
		StreamID:  streamID,
		SchemaID:  schemaID,
		Records:   make([]ParquetRecord, 0),
		Partition: partition,
		Metadata: ParquetFileMetadata{
			SchemaID:  string(schemaID),
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}
}

// Add adds a validated record to the batch.
func (b *ParquetBatch) Add(record *common.ValidatedRecord, payload []byte) {
	partKey := common.PartitionKeyFromTime(record.StreamID, record.IngestedAt)

	pr := ParquetRecord{
		RecordID:     string(record.RecordID),
		StreamID:     string(record.StreamID),
		Sequence:     int64(record.SequenceNumber),
		IngestedAt:   record.IngestedAt.UnixMicro(),
		PartitionKey: partKey.PartitionPath(),
		Payload:      payload,
	}

	b.Records = append(b.Records, pr)

	seq := int64(record.SequenceNumber)
	if b.Metadata.MinSequence == 0 || seq < b.Metadata.MinSequence {
		b.Metadata.MinSequence = seq
	}
	if seq > b.Metadata.MaxSequence {
		b.Metadata.MaxSequence = seq
	}
	b.Metadata.RecordCount++
}

// Len returns the number of records in the batch.
func (b *ParquetBatch) Len() int {
	return len(b.Records)
}

// MarshalJSON serializes the batch to JSON (used as intermediate format before Parquet).
// In a production system, this would use a proper Parquet library.
func (b *ParquetBatch) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Records  []ParquetRecord     `json:"records"`
		Metadata ParquetFileMetadata `json:"metadata"`
	}{
		Records:  b.Records,
		Metadata: b.Metadata,
	})
}

// FileKey returns the S3 object key for this batch.
func (b *ParquetBatch) FileKey(fileID string) string {
	return fmt.Sprintf("%s/%s.parquet", b.Partition.PartitionPath(), fileID)
}

// ManifestKey returns the S3 object key for the partition manifest.
func (b *ParquetBatch) ManifestKey() string {
	return fmt.Sprintf("%s/_manifest.json", b.Partition.PartitionPath())
}
