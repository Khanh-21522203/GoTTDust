package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"GoTTDust/internal/common"
)

// CompactionConfig holds compaction configuration.
type CompactionConfig struct {
	TargetFileSizeBytes   int64
	MinFilesForCompaction int
	CompactionInterval    time.Duration
	MaxConcurrentCompact  int
}

// DefaultCompactionConfig returns sensible defaults.
func DefaultCompactionConfig() CompactionConfig {
	return CompactionConfig{
		TargetFileSizeBytes:   134217728, // 128MB
		MinFilesForCompaction: 10,
		CompactionInterval:    time.Hour,
		MaxConcurrentCompact:  2,
	}
}

// CompactionPlan describes a planned compaction operation.
type CompactionPlan struct {
	Partition   common.PartitionKey
	SourceFiles []FileEntry
	TargetSize  int64
	CreatedAt   time.Time
}

// Compactor merges small files into larger ones for query efficiency.
type Compactor struct {
	mu      sync.Mutex
	config  CompactionConfig
	manager *Manager
	running map[string]bool // partition path -> running
}

// NewCompactor creates a new compactor.
func NewCompactor(config CompactionConfig, manager *Manager) *Compactor {
	return &Compactor{
		config:  config,
		manager: manager,
		running: make(map[string]bool),
	}
}

// IdentifyCandidates finds partitions that need compaction.
// Criteria: >10 files OR total size < 64MB with multiple files.
func (c *Compactor) IdentifyCandidates() []CompactionPlan {
	c.mu.Lock()
	defer c.mu.Unlock()

	index := c.manager.index
	index.mu.RLock()
	defer index.mu.RUnlock()

	var plans []CompactionPlan

	for partPath, files := range index.index {
		if c.running[partPath] {
			continue
		}

		if len(files) < c.config.MinFilesForCompaction {
			continue
		}

		// Calculate total size
		var totalSize int64
		for _, f := range files {
			totalSize += f.SizeBytes
		}

		// Group files to reach target size
		plan := CompactionPlan{
			SourceFiles: make([]FileEntry, len(files)),
			TargetSize:  c.config.TargetFileSizeBytes,
			CreatedAt:   time.Now(),
		}
		copy(plan.SourceFiles, files)

		// Parse partition from path (best-effort)
		if pk, err := common.ParsePartitionPath(partPath); err == nil {
			plan.Partition = pk
		}
		plans = append(plans, plan)
	}

	return plans
}

// Execute runs a compaction plan.
// Steps: read source files -> merge -> write merged file -> atomic manifest update -> delete sources.
func (c *Compactor) Execute(ctx context.Context, plan CompactionPlan) error {
	partPath := plan.Partition.PartitionPath()

	c.mu.Lock()
	if c.running[partPath] {
		c.mu.Unlock()
		return fmt.Errorf("compaction already running for partition %s", partPath)
	}
	c.running[partPath] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.running, partPath)
		c.mu.Unlock()
	}()

	// Step 1: Read all source files
	var allRecords []ParquetRecord
	for _, file := range plan.SourceFiles {
		data, err := c.manager.ReadFile(ctx, file.Key)
		if err != nil {
			return fmt.Errorf("read source file %s: %w", file.Key, err)
		}

		var batch struct {
			Records []ParquetRecord `json:"records"`
		}
		if err := json.Unmarshal(data, &batch); err != nil {
			return fmt.Errorf("unmarshal source file %s: %w", file.Key, err)
		}

		allRecords = append(allRecords, batch.Records...)
	}

	if len(allRecords) == 0 {
		return nil
	}

	// Step 2: Build merged batch
	var minSeq, maxSeq int64
	for i, r := range allRecords {
		if i == 0 || r.Sequence < minSeq {
			minSeq = r.Sequence
		}
		if r.Sequence > maxSeq {
			maxSeq = r.Sequence
		}
	}

	merged := struct {
		Records  []ParquetRecord     `json:"records"`
		Metadata ParquetFileMetadata `json:"metadata"`
	}{
		Records: allRecords,
		Metadata: ParquetFileMetadata{
			MinSequence: minSeq,
			MaxSequence: maxSeq,
			RecordCount: int64(len(allRecords)),
			CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		},
	}

	mergedData, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("marshal merged data: %w", err)
	}

	// Step 3: Write merged file to temp location then final
	c.manager.mu.Lock()
	c.manager.seqGen++
	seq := c.manager.seqGen
	c.manager.mu.Unlock()

	fileID := fmt.Sprintf("%d_%06d_%s_compacted",
		time.Now().UnixMilli(), seq, c.manager.nodeID)
	mergedKey := fmt.Sprintf("%s/%s.parquet", partPath, fileID)

	if err := c.manager.s3.PutObject(ctx, mergedKey, mergedData); err != nil {
		return fmt.Errorf("upload merged file: %w", err)
	}

	// Step 4: Atomic index update - add merged, remove sources
	c.manager.index.mu.Lock()
	// Remove source files from index
	files := c.manager.index.index[partPath]
	sourceKeys := make(map[string]bool)
	for _, sf := range plan.SourceFiles {
		sourceKeys[sf.Key] = true
	}
	var remaining []FileEntry
	for _, f := range files {
		if !sourceKeys[f.Key] {
			remaining = append(remaining, f)
		}
	}
	// Add merged file
	remaining = append(remaining, FileEntry{
		Key:         mergedKey,
		FileID:      fileID,
		RecordCount: int64(len(allRecords)),
		MinSequence: minSeq,
		MaxSequence: maxSeq,
		CreatedAt:   time.Now(),
		SizeBytes:   int64(len(mergedData)),
	})
	c.manager.index.index[partPath] = remaining
	c.manager.index.mu.Unlock()

	// Step 5: Delete source files from S3
	for _, sf := range plan.SourceFiles {
		if err := c.manager.s3.DeleteObject(ctx, sf.Key); err != nil {
			// Log but don't fail - orphaned files can be cleaned up later
			_ = err
		}
	}

	return nil
}

// RunLoop starts the compaction loop that runs periodically.
func (c *Compactor) RunLoop(ctx context.Context) {
	ticker := time.NewTicker(c.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			plans := c.IdentifyCandidates()

			// Limit concurrent compactions
			sem := make(chan struct{}, c.config.MaxConcurrentCompact)
			var wg sync.WaitGroup

			for _, plan := range plans {
				sem <- struct{}{}
				wg.Add(1)
				go func(p CompactionPlan) {
					defer wg.Done()
					defer func() { <-sem }()
					_ = c.Execute(ctx, p)
				}(plan)
			}

			wg.Wait()
		}
	}
}
