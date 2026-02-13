package ingestion

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"GoTTDust/internal/common"
	"GoTTDust/internal/schema"
)

// BatchJobStatus represents the status of a batch import job.
type BatchJobStatus string

const (
	BatchJobPending   BatchJobStatus = "PENDING"
	BatchJobRunning   BatchJobStatus = "RUNNING"
	BatchJobCompleted BatchJobStatus = "COMPLETED"
	BatchJobFailed    BatchJobStatus = "FAILED"
)

// BatchJob represents an async batch import job.
type BatchJob struct {
	JobID            string         `json:"job_id"`
	StreamID         string         `json:"stream_id"`
	SourceURI        string         `json:"source_uri"`
	Format           string         `json:"format"`
	Status           BatchJobStatus `json:"status"`
	RecordsProcessed int64          `json:"records_processed"`
	RecordsSucceeded int64          `json:"records_succeeded"`
	RecordsFailed    int64          `json:"records_failed"`
	Errors           []BatchError   `json:"errors,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	CompletedAt      *time.Time     `json:"completed_at,omitempty"`
}

// BatchError describes a single error in a batch import.
type BatchError struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
}

// BatchExecutor runs batch import jobs asynchronously.
type BatchExecutor struct {
	db             *sql.DB
	schemaRegistry *schema.Registry
	walManager     *WALManager
	bufferManager  *BufferManager
	// fetchFile downloads a file from a URI (e.g. S3) and returns a reader.
	fetchFile func(ctx context.Context, uri string) (io.ReadCloser, error)
}

// NewBatchExecutor creates a new batch executor.
func NewBatchExecutor(
	db *sql.DB,
	schemaRegistry *schema.Registry,
	walManager *WALManager,
	bufferManager *BufferManager,
	fetchFile func(ctx context.Context, uri string) (io.ReadCloser, error),
) *BatchExecutor {
	return &BatchExecutor{
		db:             db,
		schemaRegistry: schemaRegistry,
		walManager:     walManager,
		bufferManager:  bufferManager,
		fetchFile:      fetchFile,
	}
}

// SubmitJob creates a new batch job and starts async execution.
func (be *BatchExecutor) SubmitJob(ctx context.Context, streamID, sourceURI, format string) (*BatchJob, error) {
	job := &BatchJob{
		JobID:     common.GenerateTraceID(),
		StreamID:  streamID,
		SourceURI: sourceURI,
		Format:    format,
		Status:    BatchJobPending,
		CreatedAt: time.Now(),
	}

	// Persist job
	_, err := be.db.ExecContext(ctx,
		`INSERT INTO system_config (config_key, config_value, description, updated_by)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (config_key) DO UPDATE SET config_value = $2, updated_at = NOW()`,
		"batch_job:"+job.JobID, mustJSON(job), "Batch import job", "system")
	if err != nil {
		return nil, fmt.Errorf("persist job: %w", err)
	}

	// Start async execution
	go be.executeJob(context.Background(), job)

	return job, nil
}

// GetJob returns the current state of a batch job.
func (be *BatchExecutor) GetJob(ctx context.Context, jobID string) (*BatchJob, error) {
	var configValue json.RawMessage
	err := be.db.QueryRowContext(ctx,
		`SELECT config_value FROM system_config WHERE config_key = $1`,
		"batch_job:"+jobID,
	).Scan(&configValue)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: job %s", common.ErrNotFound, jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}

	var job BatchJob
	if err := json.Unmarshal(configValue, &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

// executeJob runs the batch import.
func (be *BatchExecutor) executeJob(ctx context.Context, job *BatchJob) {
	// Update status to RUNNING
	job.Status = BatchJobRunning
	be.persistJob(ctx, job)

	// Fetch file
	reader, err := be.fetchFile(ctx, job.SourceURI)
	if err != nil {
		job.Status = BatchJobFailed
		job.Errors = append(job.Errors, BatchError{Line: 0, Error: fmt.Sprintf("fetch file: %v", err)})
		now := time.Now()
		job.CompletedAt = &now
		be.persistJob(ctx, job)
		return
	}
	defer reader.Close()

	streamID := common.StreamID(job.StreamID)
	buf := be.bufferManager.GetOrCreate(streamID)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB line buffer

	lineNum := 0
	var firstError *BatchError

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		job.RecordsProcessed++

		// Parse JSON
		var payload json.RawMessage
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			firstError = &BatchError{Line: lineNum, Error: fmt.Sprintf("invalid JSON: %v", err)}
			break // Atomic failure: reject entire batch
		}

		// Assign metadata
		record := &common.ValidatedRecord{
			RecordID:       common.RecordID(common.GenerateTraceID()),
			StreamID:       streamID,
			SequenceNumber: common.SequenceNumber(lineNum),
			Payload:        payload,
			IngestedAt:     time.Now(),
		}

		// Buffer
		if err := buf.Add(record); err != nil {
			firstError = &BatchError{Line: lineNum, Error: fmt.Sprintf("buffer full: %v", err)}
			break
		}

		// Periodic WAL flush every 10K records
		if job.RecordsProcessed%10000 == 0 {
			if err := be.walManager.Write(streamID, record); err != nil {
				log.Printf("Batch WAL write warning at line %d: %v", lineNum, err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		firstError = &BatchError{Line: lineNum, Error: fmt.Sprintf("read error: %v", err)}
	}

	now := time.Now()
	job.CompletedAt = &now

	if firstError != nil {
		// Atomic failure: reject entire batch
		job.Status = BatchJobFailed
		job.RecordsFailed = job.RecordsProcessed
		job.RecordsSucceeded = 0
		job.Errors = append(job.Errors, *firstError)

		// Discard buffered records
		buf.Flush()
	} else {
		job.Status = BatchJobCompleted
		job.RecordsSucceeded = job.RecordsProcessed
		job.RecordsFailed = 0
	}

	be.persistJob(ctx, job)
}

func (be *BatchExecutor) persistJob(ctx context.Context, job *BatchJob) {
	_, err := be.db.ExecContext(ctx,
		`UPDATE system_config SET config_value = $1, updated_at = NOW()
		 WHERE config_key = $2`,
		mustJSON(job), "batch_job:"+job.JobID)
	if err != nil {
		log.Printf("Failed to persist batch job %s: %v", job.JobID, err)
	}
}

func mustJSON(v interface{}) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
