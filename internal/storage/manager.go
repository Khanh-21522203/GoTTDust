package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"GoTTDust/internal/common"
	s3adapter "GoTTDust/internal/storage/s3"
)

// Manager coordinates writes to WAL and Object Storage,
// manages file lifecycle, maintains partition index, and handles cache coherence.
type Manager struct {
	mu     sync.RWMutex
	s3     *s3adapter.Adapter
	index  *PartitionIndex
	nodeID string
	seqGen uint64
}

// NewManager creates a new storage manager.
func NewManager(s3Adapter *s3adapter.Adapter, nodeID string) *Manager {
	return &Manager{
		s3:     s3Adapter,
		index:  NewPartitionIndex(),
		nodeID: nodeID,
	}
}

// FlushBatch writes a ParquetBatch to S3 and updates the partition index.
func (m *Manager) FlushBatch(ctx context.Context, batch *ParquetBatch) error {
	if batch.Len() == 0 {
		return nil
	}

	m.mu.Lock()
	m.seqGen++
	seq := m.seqGen
	m.mu.Unlock()

	// Generate file ID: {timestamp_epoch_ms}_{sequence}_{node_id}
	fileID := fmt.Sprintf("%d_%06d_%s",
		time.Now().UnixMilli(), seq, m.nodeID)

	// Serialize batch (JSON intermediate; production would use Parquet library)
	data, err := batch.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	// Upload to S3
	key := batch.FileKey(fileID)
	if err := m.s3.PutObject(ctx, key, data); err != nil {
		return fmt.Errorf("upload to S3: %w", err)
	}

	// Update partition index
	m.index.AddFile(batch.Partition, FileEntry{
		Key:         key,
		FileID:      fileID,
		RecordCount: batch.Metadata.RecordCount,
		MinSequence: batch.Metadata.MinSequence,
		MaxSequence: batch.Metadata.MaxSequence,
		CreatedAt:   time.Now(),
		SizeBytes:   int64(len(data)),
	})

	// Update manifest
	if err := m.updateManifest(ctx, batch); err != nil {
		// Non-fatal: manifest can be rebuilt from file listing
		_ = err
	}

	return nil
}

// GetPartitionFiles returns all files for a given partition.
func (m *Manager) GetPartitionFiles(partition common.PartitionKey) []FileEntry {
	return m.index.GetFiles(partition)
}

// ReadFile reads a file from S3.
func (m *Manager) ReadFile(ctx context.Context, key string) ([]byte, error) {
	return m.s3.GetObject(ctx, key)
}

// ListPartitionFiles lists all files in a partition from S3.
func (m *Manager) ListPartitionFiles(ctx context.Context, partition common.PartitionKey) ([]string, error) {
	prefix := partition.PartitionPath() + "/"
	return m.s3.ListObjects(ctx, prefix)
}

// DeleteFile deletes a file from S3 and removes it from the index.
func (m *Manager) DeleteFile(ctx context.Context, partition common.PartitionKey, key string) error {
	if err := m.s3.DeleteObject(ctx, key); err != nil {
		return err
	}
	m.index.RemoveFile(partition, key)
	return nil
}

// updateManifest writes the partition manifest to S3.
func (m *Manager) updateManifest(ctx context.Context, batch *ParquetBatch) error {
	files := m.index.GetFiles(batch.Partition)

	manifest := PartitionManifest{
		Partition: batch.Partition.PartitionPath(),
		Files:     files,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	return m.s3.PutObject(ctx, batch.ManifestKey(), data)
}

// PartitionManifest describes all files in a partition.
type PartitionManifest struct {
	Partition string      `json:"partition"`
	Files     []FileEntry `json:"files"`
	UpdatedAt string      `json:"updated_at"`
}

// FileEntry describes a single data file in a partition.
type FileEntry struct {
	Key         string    `json:"key"`
	FileID      string    `json:"file_id"`
	RecordCount int64     `json:"record_count"`
	MinSequence int64     `json:"min_sequence"`
	MaxSequence int64     `json:"max_sequence"`
	CreatedAt   time.Time `json:"created_at"`
	SizeBytes   int64     `json:"size_bytes"`
}

// PartitionIndex is an in-memory index of files per partition.
type PartitionIndex struct {
	mu    sync.RWMutex
	index map[string][]FileEntry // partition path -> files
}

// NewPartitionIndex creates a new partition index.
func NewPartitionIndex() *PartitionIndex {
	return &PartitionIndex{
		index: make(map[string][]FileEntry),
	}
}

// AddFile adds a file entry to the index.
func (pi *PartitionIndex) AddFile(partition common.PartitionKey, entry FileEntry) {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	key := partition.PartitionPath()
	pi.index[key] = append(pi.index[key], entry)
}

// GetFiles returns all files for a partition.
func (pi *PartitionIndex) GetFiles(partition common.PartitionKey) []FileEntry {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	key := partition.PartitionPath()
	files := pi.index[key]
	result := make([]FileEntry, len(files))
	copy(result, files)
	return result
}

// RemoveFile removes a file from the index.
func (pi *PartitionIndex) RemoveFile(partition common.PartitionKey, fileKey string) {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	key := partition.PartitionPath()
	files := pi.index[key]
	for i, f := range files {
		if f.Key == fileKey {
			pi.index[key] = append(files[:i], files[i+1:]...)
			return
		}
	}
}

// PartitionKeyFromRecord creates a PartitionKey from a validated record.
func PartitionKeyFromRecord(record *common.ValidatedRecord) common.PartitionKey {
	return common.PartitionKeyFromTime(record.StreamID, record.IngestedAt)
}

// GetPartitionsForTimeRange returns partition keys that overlap with a time range.
func GetPartitionsForTimeRange(streamID common.StreamID, start, end time.Time) []common.PartitionKey {
	var partitions []common.PartitionKey

	current := start.UTC().Truncate(time.Hour)
	endTrunc := end.UTC()

	for !current.After(endTrunc) {
		partitions = append(partitions, common.PartitionKeyFromTime(streamID, current))
		current = current.Add(time.Hour)
	}

	return partitions
}
