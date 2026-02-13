package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"GoTTDust/internal/common"
)

// Manager manages processing pipeline configurations in PostgreSQL.
type Manager struct {
	db *sql.DB
}

// NewManager creates a new pipeline manager.
func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// CreatePipelineRequest holds the input for creating a pipeline.
type CreatePipelineRequest struct {
	Name     string
	StreamID string
	Stages   []StageInput
	DLQ      *DLQInput
}

// StageInput holds input for a pipeline stage.
type StageInput struct {
	StageType string
	Config    json.RawMessage
	Enabled   bool
}

// DLQInput holds input for DLQ configuration.
type DLQInput struct {
	Enabled    bool
	StreamName string
}

// supportedStageTypes lists the valid stage types for MVP.
var supportedStageTypes = map[string]bool{
	"field_projection":        true,
	"timestamp_normalization": true,
	"type_coercion":           true,
}

// CreatePipeline creates a new pipeline with stages (atomic).
func (m *Manager) CreatePipeline(ctx context.Context, req CreatePipelineRequest) (*common.Pipeline, []*common.PipelineStage, error) {
	// Validate stream exists and is active
	var streamStatus string
	err := m.db.QueryRowContext(ctx,
		`SELECT status FROM streams WHERE stream_id = $1`, req.StreamID,
	).Scan(&streamStatus)
	if err == sql.ErrNoRows {
		return nil, nil, fmt.Errorf("%w: stream %s not found", common.ErrNotFound, req.StreamID)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("check stream: %w", err)
	}
	if streamStatus != common.StreamStatusActive {
		return nil, nil, fmt.Errorf("%w: stream must be active, got %s", common.ErrInvalidState, streamStatus)
	}

	// Validate stage types
	for i, stage := range req.Stages {
		if !supportedStageTypes[stage.StageType] {
			return nil, nil, fmt.Errorf("%w: unsupported stage type %q at position %d", common.ErrValidation, stage.StageType, i)
		}
	}

	// Begin transaction
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Insert pipeline
	pipeline := &common.Pipeline{}
	err = tx.QueryRowContext(ctx,
		`INSERT INTO pipelines (name, stream_id, status)
		 VALUES ($1, $2, $3)
		 RETURNING pipeline_id, name, stream_id, status, dlq_stream_id, created_at, updated_at`,
		req.Name, req.StreamID, common.PipelineStatusActive,
	).Scan(&pipeline.PipelineID, &pipeline.Name, &pipeline.StreamID, &pipeline.Status,
		&sql.NullString{}, &pipeline.CreatedAt, &pipeline.UpdatedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("insert pipeline: %w", err)
	}

	// Insert stages
	stages := make([]*common.PipelineStage, 0, len(req.Stages))
	for i, stageInput := range req.Stages {
		stage := &common.PipelineStage{}
		enabled := stageInput.Enabled
		if !enabled && i == 0 {
			enabled = true // Default first stage to enabled
		}
		err = tx.QueryRowContext(ctx,
			`INSERT INTO pipeline_stages (pipeline_id, stage_order, stage_type, config, enabled)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING stage_id, pipeline_id, stage_order, stage_type, config, enabled`,
			pipeline.PipelineID, i+1, stageInput.StageType, stageInput.Config, enabled,
		).Scan(&stage.StageID, &stage.PipelineID, &stage.StageOrder, &stage.StageType,
			&stage.Config, &stage.Enabled)
		if err != nil {
			return nil, nil, fmt.Errorf("insert stage %d: %w", i, err)
		}
		stages = append(stages, stage)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit: %w", err)
	}

	return pipeline, stages, nil
}

// GetPipeline returns a pipeline by ID.
func (m *Manager) GetPipeline(ctx context.Context, pipelineID string) (*common.Pipeline, error) {
	p := &common.Pipeline{}
	var dlqStreamID sql.NullString
	err := m.db.QueryRowContext(ctx,
		`SELECT pipeline_id, name, stream_id, status, dlq_stream_id, created_at, updated_at
		 FROM pipelines WHERE pipeline_id = $1`,
		pipelineID,
	).Scan(&p.PipelineID, &p.Name, &p.StreamID, &p.Status,
		&dlqStreamID, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: pipeline %s", common.ErrNotFound, pipelineID)
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	if dlqStreamID.Valid {
		p.DLQStreamID = dlqStreamID.String
	}
	return p, nil
}

// GetPipelineStages returns all stages for a pipeline.
func (m *Manager) GetPipelineStages(ctx context.Context, pipelineID string) ([]*common.PipelineStage, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT stage_id, pipeline_id, stage_order, stage_type, config, enabled
		 FROM pipeline_stages WHERE pipeline_id = $1 ORDER BY stage_order ASC`,
		pipelineID,
	)
	if err != nil {
		return nil, fmt.Errorf("get stages: %w", err)
	}
	defer rows.Close()

	var stages []*common.PipelineStage
	for rows.Next() {
		s := &common.PipelineStage{}
		if err := rows.Scan(&s.StageID, &s.PipelineID, &s.StageOrder, &s.StageType,
			&s.Config, &s.Enabled); err != nil {
			return nil, fmt.Errorf("scan stage: %w", err)
		}
		stages = append(stages, s)
	}
	return stages, nil
}

// ListPipelines returns all pipelines, optionally filtered by stream.
func (m *Manager) ListPipelines(ctx context.Context, streamID string) ([]*common.Pipeline, error) {
	var rows *sql.Rows
	var err error

	if streamID != "" {
		rows, err = m.db.QueryContext(ctx,
			`SELECT pipeline_id, name, stream_id, status, dlq_stream_id, created_at, updated_at
			 FROM pipelines WHERE stream_id = $1 ORDER BY created_at DESC`, streamID)
	} else {
		rows, err = m.db.QueryContext(ctx,
			`SELECT pipeline_id, name, stream_id, status, dlq_stream_id, created_at, updated_at
			 FROM pipelines ORDER BY created_at DESC`)
	}
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []*common.Pipeline
	for rows.Next() {
		p := &common.Pipeline{}
		var dlqStreamID sql.NullString
		if err := rows.Scan(&p.PipelineID, &p.Name, &p.StreamID, &p.Status,
			&dlqStreamID, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline: %w", err)
		}
		if dlqStreamID.Valid {
			p.DLQStreamID = dlqStreamID.String
		}
		pipelines = append(pipelines, p)
	}
	return pipelines, nil
}

// UpdatePipelineStages replaces all stages atomically.
func (m *Manager) UpdatePipelineStages(ctx context.Context, pipelineID string, stages []StageInput) ([]*common.PipelineStage, error) {
	// Validate stage types
	for i, stage := range stages {
		if !supportedStageTypes[stage.StageType] {
			return nil, fmt.Errorf("%w: unsupported stage type %q at position %d", common.ErrValidation, stage.StageType, i)
		}
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Delete existing stages
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_stages WHERE pipeline_id = $1`, pipelineID); err != nil {
		return nil, fmt.Errorf("delete old stages: %w", err)
	}

	// Insert new stages
	result := make([]*common.PipelineStage, 0, len(stages))
	for i, stageInput := range stages {
		s := &common.PipelineStage{}
		err = tx.QueryRowContext(ctx,
			`INSERT INTO pipeline_stages (pipeline_id, stage_order, stage_type, config, enabled)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING stage_id, pipeline_id, stage_order, stage_type, config, enabled`,
			pipelineID, i+1, stageInput.StageType, stageInput.Config, stageInput.Enabled,
		).Scan(&s.StageID, &s.PipelineID, &s.StageOrder, &s.StageType, &s.Config, &s.Enabled)
		if err != nil {
			return nil, fmt.Errorf("insert stage %d: %w", i, err)
		}
		result = append(result, s)
	}

	// Update pipeline timestamp
	if _, err := tx.ExecContext(ctx,
		`UPDATE pipelines SET updated_at = $1 WHERE pipeline_id = $2`,
		time.Now(), pipelineID); err != nil {
		return nil, fmt.Errorf("update pipeline: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return result, nil
}

// DeletePipeline deletes a pipeline and its stages.
func (m *Manager) DeletePipeline(ctx context.Context, pipelineID string) error {
	result, err := m.db.ExecContext(ctx, `DELETE FROM pipelines WHERE pipeline_id = $1`, pipelineID)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("%w: pipeline %s", common.ErrNotFound, pipelineID)
	}
	return nil
}

// DisablePipeline disables a pipeline.
func (m *Manager) DisablePipeline(ctx context.Context, pipelineID string) error {
	_, err := m.db.ExecContext(ctx,
		`UPDATE pipelines SET status = $1, updated_at = $2 WHERE pipeline_id = $3`,
		common.PipelineStatusDisabled, time.Now(), pipelineID)
	return err
}

// EnablePipeline enables a pipeline.
func (m *Manager) EnablePipeline(ctx context.Context, pipelineID string) error {
	_, err := m.db.ExecContext(ctx,
		`UPDATE pipelines SET status = $1, updated_at = $2 WHERE pipeline_id = $3`,
		common.PipelineStatusActive, time.Now(), pipelineID)
	return err
}

// GetPipelineForStream returns the active pipeline for a stream.
func (m *Manager) GetPipelineForStream(ctx context.Context, streamID string) (*common.Pipeline, []*common.PipelineStage, error) {
	p := &common.Pipeline{}
	var dlqStreamID sql.NullString
	err := m.db.QueryRowContext(ctx,
		`SELECT pipeline_id, name, stream_id, status, dlq_stream_id, created_at, updated_at
		 FROM pipelines WHERE stream_id = $1 AND status = 'active' LIMIT 1`,
		streamID,
	).Scan(&p.PipelineID, &p.Name, &p.StreamID, &p.Status,
		&dlqStreamID, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil, nil // No pipeline configured
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get pipeline for stream: %w", err)
	}
	if dlqStreamID.Valid {
		p.DLQStreamID = dlqStreamID.String
	}

	stages, err := m.GetPipelineStages(ctx, p.PipelineID)
	if err != nil {
		return nil, nil, err
	}

	return p, stages, nil
}
