package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"GoTTDust/internal/common"
	s3adapter "GoTTDust/internal/storage/s3"
)

// Checkpoint tracks the last flushed position per stream for crash recovery.
type Checkpoint struct {
	NodeID    string                       `json:"node_id"`
	Streams   map[string]StreamCheckpoint  `json:"streams"`
	UpdatedAt time.Time                    `json:"updated_at"`
}

// StreamCheckpoint tracks flush progress for a single stream.
type StreamCheckpoint struct {
	StreamID        string    `json:"stream_id"`
	LastSegment     int64     `json:"last_segment"`
	LastSequence    int64     `json:"last_sequence"`
	LastFlushedAt   time.Time `json:"last_flushed_at"`
}

// StreamMetaSnapshot is a metadata snapshot stored alongside stream data in S3.
type StreamMetaSnapshot struct {
	StreamID        string          `json:"stream_id"`
	Name            string          `json:"name"`
	SchemaID        string          `json:"schema_id"`
	Status          string          `json:"status"`
	RetentionPolicy json.RawMessage `json:"retention_policy"`
	PartitionConfig json.RawMessage `json:"partition_config"`
	TotalRecords    int64           `json:"total_records"`
	TotalBytes      int64           `json:"total_bytes"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// CheckpointManager manages checkpoints and stream metadata snapshots in S3.
type CheckpointManager struct {
	s3     *s3adapter.Adapter
	nodeID string
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(s3 *s3adapter.Adapter, nodeID string) *CheckpointManager {
	return &CheckpointManager{s3: s3, nodeID: nodeID}
}

// checkpointKey returns the S3 key for a node's checkpoint.
func (cm *CheckpointManager) checkpointKey() string {
	return fmt.Sprintf("_system/checkpoints/%s/checkpoint.json", cm.nodeID)
}

// LoadCheckpoint reads the latest checkpoint from S3.
func (cm *CheckpointManager) LoadCheckpoint(ctx context.Context) (*Checkpoint, error) {
	data, err := cm.s3.GetObject(ctx, cm.checkpointKey())
	if err != nil {
		// No checkpoint yet
		return &Checkpoint{
			NodeID:  cm.nodeID,
			Streams: make(map[string]StreamCheckpoint),
		}, nil
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	return &cp, nil
}

// SaveCheckpoint writes the checkpoint to S3.
func (cm *CheckpointManager) SaveCheckpoint(ctx context.Context, cp *Checkpoint) error {
	cp.UpdatedAt = time.Now()
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	return cm.s3.PutObject(ctx, cm.checkpointKey(), data)
}

// UpdateStreamCheckpoint updates the checkpoint for a single stream after a successful flush.
func (cm *CheckpointManager) UpdateStreamCheckpoint(ctx context.Context, streamID common.StreamID, segment int64, sequence int64) error {
	cp, err := cm.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}

	cp.Streams[string(streamID)] = StreamCheckpoint{
		StreamID:      string(streamID),
		LastSegment:   segment,
		LastSequence:  sequence,
		LastFlushedAt: time.Now(),
	}

	return cm.SaveCheckpoint(ctx, cp)
}

// SaveStreamMeta writes a stream metadata snapshot to S3.
func (cm *CheckpointManager) SaveStreamMeta(ctx context.Context, meta *StreamMetaSnapshot) error {
	key := fmt.Sprintf("streams/%s/_stream_meta.json", meta.StreamID)
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal stream meta: %w", err)
	}
	return cm.s3.PutObject(ctx, key, data)
}

// LoadStreamMeta reads a stream metadata snapshot from S3.
func (cm *CheckpointManager) LoadStreamMeta(ctx context.Context, streamID string) (*StreamMetaSnapshot, error) {
	key := fmt.Sprintf("streams/%s/_stream_meta.json", streamID)
	data, err := cm.s3.GetObject(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load stream meta: %w", err)
	}

	var meta StreamMetaSnapshot
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal stream meta: %w", err)
	}
	return &meta, nil
}

// WALArchiver archives sealed WAL segments to S3 for disaster recovery.
type WALArchiver struct {
	s3     *s3adapter.Adapter
	nodeID string
}

// NewWALArchiver creates a new WAL archiver.
func NewWALArchiver(s3 *s3adapter.Adapter, nodeID string) *WALArchiver {
	return &WALArchiver{s3: s3, nodeID: nodeID}
}

// ArchiveSegment uploads a sealed WAL segment to S3.
func (wa *WALArchiver) ArchiveSegment(ctx context.Context, streamID common.StreamID, segmentID int64, data []byte) error {
	key := fmt.Sprintf("wal-archive/%s/%s/%d.wal", wa.nodeID, streamID, segmentID)
	if err := wa.s3.PutObject(ctx, key, data); err != nil {
		return fmt.Errorf("archive WAL segment: %w", err)
	}
	log.Printf("Archived WAL segment %d for stream %s", segmentID, streamID)
	return nil
}

// ListArchivedSegments lists archived WAL segments for a stream.
func (wa *WALArchiver) ListArchivedSegments(ctx context.Context, streamID common.StreamID) ([]string, error) {
	prefix := fmt.Sprintf("wal-archive/%s/%s/", wa.nodeID, streamID)
	return wa.s3.ListObjects(ctx, prefix)
}

// GetArchivedSegment downloads an archived WAL segment.
func (wa *WALArchiver) GetArchivedSegment(ctx context.Context, streamID common.StreamID, segmentID int64) ([]byte, error) {
	key := fmt.Sprintf("wal-archive/%s/%s/%d.wal", wa.nodeID, streamID, segmentID)
	return wa.s3.GetObject(ctx, key)
}

// CompactionLog tracks compaction state per stream in S3.
type CompactionLog struct {
	StreamID       string            `json:"stream_id"`
	LastCompaction time.Time         `json:"last_compaction"`
	Entries        []CompactionEntry `json:"entries"`
}

// CompactionEntry records a single compaction operation.
type CompactionEntry struct {
	Partition    string    `json:"partition"`
	InputFiles   []string  `json:"input_files"`
	OutputFile   string    `json:"output_file"`
	InputBytes   int64     `json:"input_bytes"`
	OutputBytes  int64     `json:"output_bytes"`
	RecordCount  int64     `json:"record_count"`
	CompactedAt  time.Time `json:"compacted_at"`
}

// SaveCompactionLog writes compaction state to S3.
func SaveCompactionLog(ctx context.Context, s3 *s3adapter.Adapter, log *CompactionLog) error {
	key := fmt.Sprintf("_system/compaction/%s/compaction_log.json", log.StreamID)
	data, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("marshal compaction log: %w", err)
	}
	return s3.PutObject(ctx, key, data)
}

// LoadCompactionLog reads compaction state from S3.
func LoadCompactionLog(ctx context.Context, s3 *s3adapter.Adapter, streamID string) (*CompactionLog, error) {
	key := fmt.Sprintf("_system/compaction/%s/compaction_log.json", streamID)
	data, err := s3.GetObject(ctx, key)
	if err != nil {
		return &CompactionLog{StreamID: streamID}, nil
	}

	var cl CompactionLog
	if err := json.Unmarshal(data, &cl); err != nil {
		return nil, fmt.Errorf("unmarshal compaction log: %w", err)
	}
	return &cl, nil
}
