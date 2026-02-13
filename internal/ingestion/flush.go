package ingestion

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"GoTTDust/internal/common"
)

// FlushHandler is called when records are ready to be flushed to storage.
type FlushHandler func(ctx context.Context, streamID common.StreamID, records []*common.ValidatedRecord) error

// FlushCoordinator manages the async buffer-to-S3 flush process
// with double-buffering, per-stream flush locks, and retry logic.
type FlushCoordinator struct {
	bufferManager *BufferManager
	walManager    *WALManager
	flushHandler  FlushHandler
	flushInterval time.Duration
	maxRetries    int

	// OnPostFlush is called after a successful flush with the stream ID and sealed segment path.
	// Use this to archive WAL segments to S3.
	OnPostFlush func(ctx context.Context, streamID common.StreamID, segmentPath string)

	flushLocks sync.Map // map[common.StreamID]*sync.Mutex
}

// NewFlushCoordinator creates a new flush coordinator.
func NewFlushCoordinator(
	bufferManager *BufferManager,
	walManager *WALManager,
	flushHandler FlushHandler,
	flushInterval time.Duration,
) *FlushCoordinator {
	return &FlushCoordinator{
		bufferManager: bufferManager,
		walManager:    walManager,
		flushHandler:  flushHandler,
		flushInterval: flushInterval,
		maxRetries:    3,
	}
}

// RunLoop starts the periodic flush loop.
func (fc *FlushCoordinator) RunLoop(ctx context.Context) {
	ticker := time.NewTicker(fc.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown
			fc.FlushAll(context.Background())
			return
		case <-ticker.C:
			fc.FlushAll(ctx)
		}
	}
}

// FlushAll flushes all ready buffers.
func (fc *FlushCoordinator) FlushAll(ctx context.Context) {
	flushed := fc.bufferManager.FlushReady(fc.flushInterval)

	var wg sync.WaitGroup
	for streamID, records := range flushed {
		wg.Add(1)
		go func(sid common.StreamID, recs []*common.ValidatedRecord) {
			defer wg.Done()
			fc.flushStream(ctx, sid, recs)
		}(streamID, records)
	}
	wg.Wait()
}

// flushStream flushes records for a single stream with lock and retry.
func (fc *FlushCoordinator) flushStream(ctx context.Context, streamID common.StreamID, records []*common.ValidatedRecord) {
	// Step 1: Acquire per-stream flush lock
	lockVal, _ := fc.flushLocks.LoadOrStore(streamID, &sync.Mutex{})
	lock := lockVal.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	// Step 2: Retry with exponential backoff
	var lastErr error
	for attempt := 0; attempt < fc.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}

		err := fc.flushHandler(ctx, streamID, records)
		if err == nil {
			// Step 3: Mark WAL segments as flushed
			if seg, err := fc.walManager.SealSegment(streamID); err == nil && seg != nil {
				fc.walManager.MarkFlushed(seg)
				// Step 4: Notify post-flush callback (e.g. WAL archival)
				if fc.OnPostFlush != nil {
					fc.OnPostFlush(ctx, streamID, seg.path)
				}
			}
			return
		}

		lastErr = err
		log.Printf("Flush attempt %d/%d for stream %s failed: %v", attempt+1, fc.maxRetries, streamID, lastErr)
	}

	log.Printf("All flush attempts failed for stream %s: %v", streamID, lastErr)
}

// ForceFlush forces an immediate flush for a specific stream.
func (fc *FlushCoordinator) ForceFlush(ctx context.Context, streamID common.StreamID) error {
	buf := fc.bufferManager.GetOrCreate(streamID)
	records := buf.Flush()
	if len(records) == 0 {
		return nil
	}

	lockVal, _ := fc.flushLocks.LoadOrStore(streamID, &sync.Mutex{})
	lock := lockVal.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	return fc.flushHandler(ctx, streamID, records)
}

// OrphanCleanupConfig holds configuration for orphan file cleanup.
type OrphanCleanupConfig struct {
	Interval time.Duration
	MaxAge   time.Duration
}

// DefaultOrphanCleanupConfig returns sensible defaults.
func DefaultOrphanCleanupConfig() OrphanCleanupConfig {
	return OrphanCleanupConfig{
		Interval: time.Hour,
		MaxAge:   24 * time.Hour,
	}
}

// OrphanCleaner removes orphan files from S3 that are not in any manifest.
type OrphanCleaner struct {
	config OrphanCleanupConfig
	// listFiles lists all data files in S3
	listFiles func(ctx context.Context) ([]string, error)
	// isInManifest checks if a file key is referenced by any manifest
	isInManifest func(ctx context.Context, key string) (bool, error)
	// deleteFile deletes a file from S3
	deleteFile func(ctx context.Context, key string) error
}

// NewOrphanCleaner creates a new orphan cleaner.
func NewOrphanCleaner(
	config OrphanCleanupConfig,
	listFiles func(ctx context.Context) ([]string, error),
	isInManifest func(ctx context.Context, key string) (bool, error),
	deleteFile func(ctx context.Context, key string) error,
) *OrphanCleaner {
	return &OrphanCleaner{
		config:       config,
		listFiles:    listFiles,
		isInManifest: isInManifest,
		deleteFile:   deleteFile,
	}
}

// RunLoop starts the periodic orphan cleanup loop.
func (oc *OrphanCleaner) RunLoop(ctx context.Context) {
	ticker := time.NewTicker(oc.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := oc.Cleanup(ctx); err != nil {
				log.Printf("Orphan cleanup error: %v", err)
			}
		}
	}
}

// Cleanup scans for and removes orphan files.
func (oc *OrphanCleaner) Cleanup(ctx context.Context) error {
	files, err := oc.listFiles(ctx)
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}

	for _, key := range files {
		inManifest, err := oc.isInManifest(ctx, key)
		if err != nil {
			continue
		}
		if !inManifest {
			if err := oc.deleteFile(ctx, key); err != nil {
				log.Printf("Failed to delete orphan file %s: %v", key, err)
			}
		}
	}

	return nil
}
