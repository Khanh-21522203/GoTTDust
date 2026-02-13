package ingestion

import (
	"fmt"
	"sync"
	"time"

	"GoTTDust/internal/common"
)

const (
	defaultMaxRecords  = 10000
	defaultMaxBytes    = 16 * 1024 * 1024 // 16MB
	backpressureGreen  = 0.70
	backpressureYellow = 0.90
)

// BackpressureLevel indicates the current load level.
type BackpressureLevel int

const (
	BackpressureHealthy  BackpressureLevel = iota // < 70% utilization
	BackpressureWarning                           // 70% - 90%
	BackpressureCritical                          // > 90%
	BackpressureOverload                          // 100%
)

func (b BackpressureLevel) String() string {
	switch b {
	case BackpressureHealthy:
		return "HEALTHY"
	case BackpressureWarning:
		return "WARNING"
	case BackpressureCritical:
		return "CRITICAL"
	case BackpressureOverload:
		return "OVERLOAD"
	default:
		return "UNKNOWN"
	}
}

// RecordBuffer is an in-memory bounded buffer per stream.
type RecordBuffer struct {
	mu              sync.Mutex
	streamID        common.StreamID
	records         []*common.ValidatedRecord
	sizeBytes       int64
	oldestTimestamp time.Time
	maxRecords      int
	maxBytes        int64
}

// NewRecordBuffer creates a new buffer for a stream.
func NewRecordBuffer(streamID common.StreamID, maxRecords int, maxBytes int64) *RecordBuffer {
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	return &RecordBuffer{
		streamID:   streamID,
		records:    make([]*common.ValidatedRecord, 0, maxRecords),
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
	}
}

// Add adds a record to the buffer. Returns an error if the buffer is full (overload).
func (b *RecordBuffer) Add(record *common.ValidatedRecord) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	level := b.backpressureLevelLocked()
	if level == BackpressureOverload {
		return fmt.Errorf("%w: buffer full for stream %s", common.ErrCapacity, b.streamID)
	}

	payloadSize := int64(len(record.Payload))
	b.records = append(b.records, record)
	b.sizeBytes += payloadSize

	if len(b.records) == 1 {
		b.oldestTimestamp = time.Now()
	}

	return nil
}

// Flush drains all records from the buffer and returns them.
func (b *RecordBuffer) Flush() []*common.ValidatedRecord {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return nil
	}

	records := b.records
	b.records = make([]*common.ValidatedRecord, 0, b.maxRecords)
	b.sizeBytes = 0
	b.oldestTimestamp = time.Time{}

	return records
}

// Len returns the number of records in the buffer.
func (b *RecordBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

// SizeBytes returns the total payload size in the buffer.
func (b *RecordBuffer) SizeBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sizeBytes
}

// BackpressureLevel returns the current backpressure level.
func (b *RecordBuffer) BackpressureLevel() BackpressureLevel {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.backpressureLevelLocked()
}

// OldestTimestamp returns the timestamp of the oldest record in the buffer.
func (b *RecordBuffer) OldestTimestamp() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.oldestTimestamp
}

// ShouldFlush returns true if the buffer should be flushed based on size or age.
func (b *RecordBuffer) ShouldFlush(flushInterval time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return false
	}

	// Flush if at capacity
	if len(b.records) >= b.maxRecords || b.sizeBytes >= b.maxBytes {
		return true
	}

	// Flush if oldest record is too old
	if !b.oldestTimestamp.IsZero() && time.Since(b.oldestTimestamp) >= flushInterval {
		return true
	}

	return false
}

func (b *RecordBuffer) backpressureLevelLocked() BackpressureLevel {
	recordUtil := float64(len(b.records)) / float64(b.maxRecords)
	byteUtil := float64(b.sizeBytes) / float64(b.maxBytes)

	util := recordUtil
	if byteUtil > util {
		util = byteUtil
	}

	switch {
	case util >= 1.0:
		return BackpressureOverload
	case util >= backpressureYellow:
		return BackpressureCritical
	case util >= backpressureGreen:
		return BackpressureWarning
	default:
		return BackpressureHealthy
	}
}

// BufferManager manages buffers for multiple streams.
type BufferManager struct {
	mu         sync.RWMutex
	buffers    map[common.StreamID]*RecordBuffer
	maxRecords int
	maxBytes   int64
}

// NewBufferManager creates a new buffer manager.
func NewBufferManager(maxRecords int, maxBytes int64) *BufferManager {
	return &BufferManager{
		buffers:    make(map[common.StreamID]*RecordBuffer),
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
	}
}

// GetOrCreate returns the buffer for a stream, creating one if needed.
func (bm *BufferManager) GetOrCreate(streamID common.StreamID) *RecordBuffer {
	bm.mu.RLock()
	buf, ok := bm.buffers[streamID]
	bm.mu.RUnlock()
	if ok {
		return buf
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Double-check
	if buf, ok := bm.buffers[streamID]; ok {
		return buf
	}

	buf = NewRecordBuffer(streamID, bm.maxRecords, bm.maxBytes)
	bm.buffers[streamID] = buf
	return buf
}

// FlushAll flushes all buffers and returns records grouped by stream.
func (bm *BufferManager) FlushAll() map[common.StreamID][]*common.ValidatedRecord {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	result := make(map[common.StreamID][]*common.ValidatedRecord)
	for streamID, buf := range bm.buffers {
		records := buf.Flush()
		if len(records) > 0 {
			result[streamID] = records
		}
	}
	return result
}

// FlushReady flushes buffers that are ready based on the flush interval.
func (bm *BufferManager) FlushReady(flushInterval time.Duration) map[common.StreamID][]*common.ValidatedRecord {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	result := make(map[common.StreamID][]*common.ValidatedRecord)
	for streamID, buf := range bm.buffers {
		if buf.ShouldFlush(flushInterval) {
			records := buf.Flush()
			if len(records) > 0 {
				result[streamID] = records
			}
		}
	}
	return result
}
