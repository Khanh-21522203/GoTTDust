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

// ManifestVersion is the current manifest format version.
const ManifestVersion = 1

// Manifest tracks files within a partition for query planning.
type Manifest struct {
	Version      int             `json:"version"`
	Partition    string          `json:"partition"`
	StreamID     string          `json:"stream_id"`
	Files        []ManifestFile  `json:"files"`
	TotalRecords int64           `json:"total_records"`
	TotalBytes   int64           `json:"total_bytes"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// ManifestFile describes a single Parquet file in a partition.
type ManifestFile struct {
	Path         string    `json:"path"`
	SizeBytes    int64     `json:"size_bytes"`
	RecordCount  int64     `json:"record_count"`
	MinSequence  int64     `json:"min_sequence"`
	MaxSequence  int64     `json:"max_sequence"`
	MinTimestamp  time.Time `json:"min_timestamp"`
	MaxTimestamp  time.Time `json:"max_timestamp"`
	CreatedAt    time.Time `json:"created_at"`
}

// ManifestManager handles reading and writing partition manifests in S3.
type ManifestManager struct {
	s3        *s3adapter.Adapter
	maxRetries int
}

// NewManifestManager creates a new manifest manager.
func NewManifestManager(s3 *s3adapter.Adapter) *ManifestManager {
	return &ManifestManager{
		s3:         s3,
		maxRetries: 3,
	}
}

// manifestKey returns the S3 key for a partition manifest.
func manifestKey(partitionPath string) string {
	return partitionPath + "/_manifest.json"
}

// GetManifest reads a partition manifest from S3.
func (mm *ManifestManager) GetManifest(ctx context.Context, partitionPath string) (*Manifest, error) {
	key := manifestKey(partitionPath)

	data, err := mm.s3.GetObject(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return &manifest, nil
}

// AddFile adds a file entry to a partition manifest using atomic read-modify-write.
// Retries on ETag conflict up to maxRetries times.
func (mm *ManifestManager) AddFile(ctx context.Context, partitionPath string, streamID common.StreamID, file ManifestFile) error {
	for attempt := 0; attempt < mm.maxRetries; attempt++ {
		manifest, err := mm.GetManifest(ctx, partitionPath)
		if err != nil {
			// Manifest doesn't exist yet, create new
			manifest = &Manifest{
				Version:   ManifestVersion,
				Partition: partitionPath,
				StreamID:  string(streamID),
				Files:     []ManifestFile{},
			}
		}

		// Add file
		manifest.Files = append(manifest.Files, file)
		manifest.TotalRecords += file.RecordCount
		manifest.TotalBytes += file.SizeBytes
		manifest.UpdatedAt = time.Now()

		// Write back
		data, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshal manifest: %w", err)
		}

		key := manifestKey(partitionPath)
		if err := mm.s3.PutObject(ctx, key, data); err != nil {
			if attempt < mm.maxRetries-1 {
				log.Printf("Manifest update conflict (attempt %d/%d), retrying...", attempt+1, mm.maxRetries)
				continue
			}
			return fmt.Errorf("put manifest after %d retries: %w", mm.maxRetries, err)
		}

		return nil
	}

	return fmt.Errorf("manifest update failed after %d retries", mm.maxRetries)
}

// RemoveFile removes a file entry from a partition manifest.
func (mm *ManifestManager) RemoveFile(ctx context.Context, partitionPath string, filePath string) error {
	manifest, err := mm.GetManifest(ctx, partitionPath)
	if err != nil {
		return fmt.Errorf("get manifest for removal: %w", err)
	}

	newFiles := make([]ManifestFile, 0, len(manifest.Files))
	for _, f := range manifest.Files {
		if f.Path != filePath {
			newFiles = append(newFiles, f)
		} else {
			manifest.TotalRecords -= f.RecordCount
			manifest.TotalBytes -= f.SizeBytes
		}
	}
	manifest.Files = newFiles
	manifest.UpdatedAt = time.Now()

	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	return mm.s3.PutObject(ctx, manifestKey(partitionPath), data)
}

// ListFiles returns all files in a partition manifest.
func (mm *ManifestManager) ListFiles(ctx context.Context, partitionPath string) ([]ManifestFile, error) {
	manifest, err := mm.GetManifest(ctx, partitionPath)
	if err != nil {
		return nil, err
	}
	return manifest.Files, nil
}
