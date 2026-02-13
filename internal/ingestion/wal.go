package ingestion

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"GoTTDust/internal/common"
)

// WAL magic bytes: "TTDWAL" in hex
var walMagic = [6]byte{0x54, 0x54, 0x44, 0x57, 0x41, 0x4C}

const (
	walVersion    uint16 = 1
	walHeaderSize uint16 = 32 // 6 magic + 2 version + 16 streamID UUID + 8 segment number
	walFooterSize uint16 = 12 // 8 checksum + 4 record count
)

// SegmentState represents the lifecycle state of a WAL segment.
type SegmentState int

const (
	SegmentActive   SegmentState = iota // Currently receiving writes
	SegmentSealed                       // No more writes, ready for flush
	SegmentFlushing                     // Being written to S3
	SegmentFlushed                      // Successfully written to S3
	SegmentDeleted                      // Local file removed
)

func (s SegmentState) String() string {
	switch s {
	case SegmentActive:
		return "ACTIVE"
	case SegmentSealed:
		return "SEALED"
	case SegmentFlushing:
		return "FLUSHING"
	case SegmentFlushed:
		return "FLUSHED"
	case SegmentDeleted:
		return "DELETED"
	default:
		return "UNKNOWN"
	}
}

// WALConfig holds WAL configuration.
type WALConfig struct {
	Dir               string
	SegmentSizeBytes  int64
	SegmentAge        time.Duration
	SyncMode          string
	RetentionSegments int
}

// DefaultWALConfig returns sensible defaults.
func DefaultWALConfig() WALConfig {
	return WALConfig{
		Dir:               "/var/lib/ttdust/wal",
		SegmentSizeBytes:  67108864, // 64MB
		SegmentAge:        10 * time.Minute,
		SyncMode:          "fsync",
		RetentionSegments: 10,
	}
}

// WALSegment represents a single WAL segment file.
type WALSegment struct {
	mu            sync.Mutex
	file          *os.File
	streamID      common.StreamID
	segmentNumber uint64
	state         SegmentState
	sizeBytes     int64
	recordCount   uint32
	createdAt     time.Time
	path          string
	checksum      uint64
}

// WALManager manages WAL segments for all streams.
type WALManager struct {
	mu       sync.RWMutex
	config   WALConfig
	segments map[common.StreamID]*WALSegment // active segment per stream
	seqGen   atomic.Uint64
	closed   atomic.Bool
}

// NewWALManager creates a new WAL manager.
func NewWALManager(config WALConfig) (*WALManager, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	wm := &WALManager{
		config:   config,
		segments: make(map[common.StreamID]*WALSegment),
	}

	return wm, nil
}

// Write writes a record to the WAL for the given stream.
// Returns after fsync to guarantee durability.
func (wm *WALManager) Write(streamID common.StreamID, record *common.ValidatedRecord) error {
	if wm.closed.Load() {
		return fmt.Errorf("WAL manager is closed")
	}

	seg, err := wm.getOrCreateSegment(streamID)
	if err != nil {
		return fmt.Errorf("get WAL segment: %w", err)
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.state != SegmentActive {
		seg.mu.Unlock()
		// Segment was sealed, get a new one
		newSeg, err := wm.rotateSegment(streamID)
		if err != nil {
			return fmt.Errorf("rotate WAL segment: %w", err)
		}
		newSeg.mu.Lock()
		defer newSeg.mu.Unlock()
		seg = newSeg
	}

	// Encode the record entry
	payload := record.Payload
	entrySize := 4 + 4 + len(payload) // length(u32) + crc32(u32) + payload
	entry := make([]byte, entrySize)

	binary.BigEndian.PutUint32(entry[0:4], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(entry[4:8], crc)
	copy(entry[8:], payload)

	// Write entry
	n, err := seg.file.Write(entry)
	if err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	// fsync for durability
	if wm.config.SyncMode == "fsync" {
		if err := seg.file.Sync(); err != nil {
			return fmt.Errorf("WAL fsync: %w", err)
		}
	}

	seg.sizeBytes += int64(n)
	seg.recordCount++

	// Check if segment should be sealed
	if seg.sizeBytes >= wm.config.SegmentSizeBytes ||
		time.Since(seg.createdAt) >= wm.config.SegmentAge {
		seg.state = SegmentSealed
	}

	return nil
}

// WriteBatch writes multiple records to the WAL atomically.
func (wm *WALManager) WriteBatch(streamID common.StreamID, records []*common.ValidatedRecord) error {
	if wm.closed.Load() {
		return fmt.Errorf("WAL manager is closed")
	}

	seg, err := wm.getOrCreateSegment(streamID)
	if err != nil {
		return fmt.Errorf("get WAL segment: %w", err)
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	for _, record := range records {
		payload := record.Payload
		entrySize := 4 + 4 + len(payload)
		entry := make([]byte, entrySize)

		binary.BigEndian.PutUint32(entry[0:4], uint32(len(payload)))
		crc := crc32.ChecksumIEEE(payload)
		binary.BigEndian.PutUint32(entry[4:8], crc)
		copy(entry[8:], payload)

		n, err := seg.file.Write(entry)
		if err != nil {
			return fmt.Errorf("WAL batch write: %w", err)
		}
		seg.sizeBytes += int64(n)
		seg.recordCount++
	}

	// Single fsync for the entire batch
	if wm.config.SyncMode == "fsync" {
		if err := seg.file.Sync(); err != nil {
			return fmt.Errorf("WAL batch fsync: %w", err)
		}
	}

	// Check if segment should be sealed
	if seg.sizeBytes >= wm.config.SegmentSizeBytes ||
		time.Since(seg.createdAt) >= wm.config.SegmentAge {
		seg.state = SegmentSealed
	}

	return nil
}

// SealSegment marks the active segment for a stream as sealed.
func (wm *WALManager) SealSegment(streamID common.StreamID) (*WALSegment, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	seg, ok := wm.segments[streamID]
	if !ok {
		return nil, nil
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.state == SegmentActive {
		seg.state = SegmentSealed
	}

	return seg, nil
}

// GetSealedSegments returns all sealed segments ready for flushing.
func (wm *WALManager) GetSealedSegments() []*WALSegment {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var sealed []*WALSegment
	for _, seg := range wm.segments {
		seg.mu.Lock()
		if seg.state == SegmentSealed {
			sealed = append(sealed, seg)
		}
		seg.mu.Unlock()
	}
	return sealed
}

// MarkFlushing marks a segment as currently being flushed to S3.
func (wm *WALManager) MarkFlushing(seg *WALSegment) {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	seg.state = SegmentFlushing
}

// MarkFlushed marks a segment as successfully flushed to S3.
func (wm *WALManager) MarkFlushed(seg *WALSegment) {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	seg.state = SegmentFlushed
}

// DeleteFlushedSegments removes flushed segment files from disk.
func (wm *WALManager) DeleteFlushedSegments() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for streamID, seg := range wm.segments {
		seg.mu.Lock()
		if seg.state == SegmentFlushed {
			if err := seg.file.Close(); err != nil {
				seg.mu.Unlock()
				return fmt.Errorf("close WAL segment: %w", err)
			}
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
				seg.mu.Unlock()
				return fmt.Errorf("remove WAL segment: %w", err)
			}
			seg.state = SegmentDeleted
			delete(wm.segments, streamID)
		}
		seg.mu.Unlock()
	}

	return nil
}

// ReadSegment reads all records from a WAL segment file for recovery.
func ReadSegment(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open WAL segment: %w", err)
	}
	_ = f.Close()

	// Read and validate header
	header := make([]byte, walHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, fmt.Errorf("read WAL header: %w", err)
	}

	if header[0] != walMagic[0] || header[1] != walMagic[1] ||
		header[2] != walMagic[2] || header[3] != walMagic[3] ||
		header[4] != walMagic[4] || header[5] != walMagic[5] {
		return nil, fmt.Errorf("invalid WAL magic bytes")
	}

	version := binary.BigEndian.Uint16(header[6:8])
	if version != walVersion {
		return nil, fmt.Errorf("unsupported WAL version: %d", version)
	}

	// Read record entries
	var records [][]byte
	for {
		// Read length prefix
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(f, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read record length: %w", err)
		}

		payloadLen := binary.BigEndian.Uint32(lenBuf)

		// Read CRC
		crcBuf := make([]byte, 4)
		if _, err := io.ReadFull(f, crcBuf); err != nil {
			return nil, fmt.Errorf("read record CRC: %w", err)
		}
		expectedCRC := binary.BigEndian.Uint32(crcBuf)

		// Read payload
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil, fmt.Errorf("read record payload: %w", err)
		}

		// Verify CRC
		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
		}

		records = append(records, payload)
	}

	return records, nil
}

// Recover replays uncommitted WAL segments on startup.
func (wm *WALManager) Recover() (map[common.StreamID][][]byte, error) {
	result := make(map[common.StreamID][][]byte)

	entries, err := os.ReadDir(wm.config.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, fmt.Errorf("read WAL dir: %w", err)
	}

	// Find stream directories
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		streamID := common.StreamID(entry.Name())
		streamDir := filepath.Join(wm.config.Dir, entry.Name())

		segFiles, err := os.ReadDir(streamDir)
		if err != nil {
			return nil, fmt.Errorf("read stream WAL dir: %w", err)
		}

		// Sort segment files by name (which includes segment number)
		var segPaths []string
		for _, sf := range segFiles {
			if !sf.IsDir() && strings.HasSuffix(sf.Name(), ".wal") {
				segPaths = append(segPaths, filepath.Join(streamDir, sf.Name()))
			}
		}
		sort.Strings(segPaths)

		for _, segPath := range segPaths {
			records, err := ReadSegment(segPath)
			if err != nil {
				// Log and skip corrupted segments
				continue
			}
			result[streamID] = append(result[streamID], records...)
		}
	}

	return result, nil
}

// Close closes all active WAL segments.
func (wm *WALManager) Close() error {
	wm.closed.Store(true)

	wm.mu.Lock()
	defer wm.mu.Unlock()

	var firstErr error
	for _, seg := range wm.segments {
		seg.mu.Lock()
		if seg.file != nil {
			// Write footer before closing
			if err := writeFooter(seg); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := seg.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		seg.mu.Unlock()
	}

	return firstErr
}

// SegmentPath returns the path for a segment.
func (seg *WALSegment) SegmentPath() string {
	return seg.path
}

// State returns the current state of the segment.
func (seg *WALSegment) State() SegmentState {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	return seg.state
}

// RecordCount returns the number of records in the segment.
func (seg *WALSegment) RecordCount() uint32 {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	return seg.recordCount
}

// StreamID returns the stream ID of the segment.
func (seg *WALSegment) StreamID() common.StreamID {
	return seg.streamID
}

// --- internal helpers ---

func (wm *WALManager) getOrCreateSegment(streamID common.StreamID) (*WALSegment, error) {
	wm.mu.RLock()
	seg, ok := wm.segments[streamID]
	wm.mu.RUnlock()

	if ok && seg.state == SegmentActive {
		return seg, nil
	}

	return wm.createSegment(streamID)
}

func (wm *WALManager) createSegment(streamID common.StreamID) (*WALSegment, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Double-check after acquiring write lock
	if seg, ok := wm.segments[streamID]; ok && seg.state == SegmentActive {
		return seg, nil
	}

	segNum := wm.seqGen.Add(1)

	// Create stream directory
	streamDir := filepath.Join(wm.config.Dir, string(streamID))
	if err := os.MkdirAll(streamDir, 0755); err != nil {
		return nil, fmt.Errorf("create stream WAL dir: %w", err)
	}

	// Create segment file
	segPath := filepath.Join(streamDir, fmt.Sprintf("%016d.wal", segNum))
	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("create WAL segment file: %w", err)
	}

	// Write header
	header := make([]byte, walHeaderSize)
	copy(header[0:6], walMagic[:])
	binary.BigEndian.PutUint16(header[6:8], walVersion)
	// Stream ID as bytes (padded/truncated to 16 bytes)
	streamIDBytes := []byte(streamID)
	if len(streamIDBytes) > 16 {
		streamIDBytes = streamIDBytes[:16]
	}
	copy(header[8:24], streamIDBytes)
	binary.BigEndian.PutUint64(header[24:32], segNum)

	if _, err := f.Write(header); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write WAL header: %w", err)
	}

	seg := &WALSegment{
		file:          f,
		streamID:      streamID,
		segmentNumber: segNum,
		state:         SegmentActive,
		sizeBytes:     int64(walHeaderSize),
		recordCount:   0,
		createdAt:     time.Now(),
		path:          segPath,
	}

	wm.segments[streamID] = seg
	return seg, nil
}

func (wm *WALManager) rotateSegment(streamID common.StreamID) (*WALSegment, error) {
	wm.mu.Lock()
	oldSeg, ok := wm.segments[streamID]
	wm.mu.Unlock()

	if ok {
		oldSeg.mu.Lock()
		if oldSeg.state == SegmentActive {
			oldSeg.state = SegmentSealed
		}
		oldSeg.mu.Unlock()
	}

	return wm.createSegment(streamID)
}

func writeFooter(seg *WALSegment) error {
	footer := make([]byte, walFooterSize)
	binary.BigEndian.PutUint64(footer[0:8], seg.checksum)
	binary.BigEndian.PutUint32(footer[8:12], seg.recordCount)
	_, err := seg.file.Write(footer)
	return err
}
