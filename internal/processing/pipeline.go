package processing

import (
	"fmt"
	"time"

	"GoTTDust/internal/common"
)

// Transform is the interface all processing stages must implement.
type Transform interface {
	// Name returns the transform type name.
	Name() string
	// Process transforms a record payload, returning the modified payload.
	// Returns an error if the record should be routed to DLQ.
	Process(payload []byte) ([]byte, error)
}

// Pipeline executes a sequence of transforms on records.
type Pipeline struct {
	ID     string
	Stream common.StreamID
	Stages []Transform
	DLQ    *DLQWriter
}

// PipelineResult holds the outcome of processing a record.
type PipelineResult struct {
	Record  *common.ValidatedRecord
	Payload []byte // Transformed payload
	Failed  bool
	DLQEntry *common.DLQRecord
}

// NewPipeline creates a new processing pipeline from config.
func NewPipeline(config common.PipelineConfig, dlqWriter *DLQWriter) (*Pipeline, error) {
	stages := make([]Transform, 0, len(config.Stages))

	for _, sc := range config.Stages {
		t, err := NewTransform(sc)
		if err != nil {
			return nil, fmt.Errorf("create transform %s: %w", sc.Type, err)
		}
		stages = append(stages, t)
	}

	return &Pipeline{
		ID:     config.PipelineID,
		Stream: config.StreamID,
		Stages: stages,
		DLQ:    dlqWriter,
	}, nil
}

// Process runs a record through all pipeline stages.
// If any stage fails, the record is routed to the DLQ.
func (p *Pipeline) Process(record *common.ValidatedRecord) *PipelineResult {
	payload := record.Payload

	for _, stage := range p.Stages {
		var err error
		payload, err = stage.Process(payload)
		if err != nil {
			dlqRecord := &common.DLQRecord{
				OriginalRecord: *record,
				Error: common.DLQError{
					Stage:   stage.Name(),
					Message: err.Error(),
				},
				Metadata: common.DLQMetadata{
					SourceStream:      record.StreamID,
					OriginalTimestamp: record.IngestedAt,
					FailureTimestamp:  time.Now(),
					RetryCount:        0,
				},
			}

			if p.DLQ != nil {
				p.DLQ.Write(dlqRecord)
			}

			return &PipelineResult{
				Record:   record,
				Failed:   true,
				DLQEntry: dlqRecord,
			}
		}
	}

	return &PipelineResult{
		Record:  record,
		Payload: payload,
		Failed:  false,
	}
}

// ProcessBatch processes a batch of records through the pipeline.
func (p *Pipeline) ProcessBatch(records []*common.ValidatedRecord) []*PipelineResult {
	results := make([]*PipelineResult, 0, len(records))
	for _, record := range records {
		results = append(results, p.Process(record))
	}
	return results
}

// NewTransform creates a Transform from a StageConfig.
func NewTransform(config common.StageConfig) (Transform, error) {
	switch config.Type {
	case "field_projection":
		return NewFieldProjection(config.Config)
	case "timestamp_normalization":
		return NewTimestampNormalization(config.Config)
	case "type_coercion":
		return NewTypeCoercion(config.Config)
	default:
		return nil, fmt.Errorf("unknown transform type: %s", config.Type)
	}
}

// DLQWriter writes failed records to the dead letter queue.
type DLQWriter struct {
	streamID      common.StreamID
	retentionDays int
	records       []*common.DLQRecord
}

// NewDLQWriter creates a new DLQ writer.
func NewDLQWriter(streamID common.StreamID, retentionDays int) *DLQWriter {
	return &DLQWriter{
		streamID:      streamID,
		retentionDays: retentionDays,
	}
}

// Write adds a record to the DLQ.
func (w *DLQWriter) Write(record *common.DLQRecord) {
	w.records = append(w.records, record)
}

// Flush returns and clears all DLQ records.
func (w *DLQWriter) Flush() []*common.DLQRecord {
	records := w.records
	w.records = nil
	return records
}

// Len returns the number of pending DLQ records.
func (w *DLQWriter) Len() int {
	return len(w.records)
}
