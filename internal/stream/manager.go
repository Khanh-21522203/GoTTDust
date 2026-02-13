package stream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"GoTTDust/internal/common"
)

// Manager manages stream lifecycle and configuration.
type Manager struct {
	db *sql.DB
}

// NewManager creates a new stream manager.
func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// validTransitions defines the stream state machine.
var validTransitions = map[string][]string{
	common.StreamStatusCreating: {common.StreamStatusActive},
	common.StreamStatusActive:   {common.StreamStatusPaused, common.StreamStatusDeleting},
	common.StreamStatusPaused:   {common.StreamStatusActive, common.StreamStatusDeleting},
	common.StreamStatusDeleting: {common.StreamStatusDeleted},
}

// CreateStream creates a new stream and transitions it to ACTIVE.
func (m *Manager) CreateStream(ctx context.Context, name, description, schemaID, createdBy string, retention *common.RetentionPolicy, partition *common.PartitionConfig) (*common.Stream, error) {
	if retention == nil {
		retention = &common.RetentionPolicy{Type: "time_based", DurationDays: 365}
	}
	if partition == nil {
		partition = &common.PartitionConfig{PartitionBy: "time", Granularity: "hourly"}
	}

	retJSON, err := json.Marshal(retention)
	if err != nil {
		return nil, fmt.Errorf("marshal retention: %w", err)
	}
	partJSON, err := json.Marshal(partition)
	if err != nil {
		return nil, fmt.Errorf("marshal partition: %w", err)
	}

	// Verify schema exists
	var schemaExists bool
	err = m.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schemas WHERE schema_id = $1)`, schemaID).Scan(&schemaExists)
	if err != nil {
		return nil, fmt.Errorf("check schema: %w", err)
	}
	if !schemaExists {
		return nil, fmt.Errorf("%w: schema %s not found", common.ErrNotFound, schemaID)
	}

	// Insert as ACTIVE (CREATING→ACTIVE is automatic after validation)
	stream := &common.Stream{}
	err = m.db.QueryRowContext(ctx,
		`INSERT INTO streams (name, description, schema_id, status, retention_policy, partition_config, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING stream_id, name, description, schema_id, status, retention_policy, partition_config, created_at, updated_at, created_by`,
		name, description, schemaID, common.StreamStatusActive, retJSON, partJSON, createdBy,
	).Scan(&stream.StreamID, &stream.Name, &stream.Description, &stream.SchemaID, &stream.Status,
		&stream.RetentionPolicy, &stream.PartitionConfig, &stream.CreatedAt, &stream.UpdatedAt, &stream.CreatedBy)
	if err != nil {
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return stream, nil
}

// GetStream returns a stream by ID.
func (m *Manager) GetStream(ctx context.Context, streamID string) (*common.Stream, error) {
	stream := &common.Stream{}
	err := m.db.QueryRowContext(ctx,
		`SELECT stream_id, name, description, schema_id, status, retention_policy, partition_config, created_at, updated_at, created_by
		 FROM streams WHERE stream_id = $1`,
		streamID,
	).Scan(&stream.StreamID, &stream.Name, &stream.Description, &stream.SchemaID, &stream.Status,
		&stream.RetentionPolicy, &stream.PartitionConfig, &stream.CreatedAt, &stream.UpdatedAt, &stream.CreatedBy)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: stream %s", common.ErrNotFound, streamID)
	}
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}
	return stream, nil
}

// ListStreams returns all streams, optionally filtered by status.
func (m *Manager) ListStreams(ctx context.Context, status string) ([]*common.Stream, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = m.db.QueryContext(ctx,
			`SELECT stream_id, name, description, schema_id, status, retention_policy, partition_config, created_at, updated_at, created_by
			 FROM streams WHERE status = $1 ORDER BY created_at DESC`, status)
	} else {
		rows, err = m.db.QueryContext(ctx,
			`SELECT stream_id, name, description, schema_id, status, retention_policy, partition_config, created_at, updated_at, created_by
			 FROM streams WHERE status != 'deleted' ORDER BY created_at DESC`)
	}
	if err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}
	defer rows.Close()

	var streams []*common.Stream
	for rows.Next() {
		s := &common.Stream{}
		if err := rows.Scan(&s.StreamID, &s.Name, &s.Description, &s.SchemaID, &s.Status,
			&s.RetentionPolicy, &s.PartitionConfig, &s.CreatedAt, &s.UpdatedAt, &s.CreatedBy); err != nil {
			return nil, fmt.Errorf("scan stream: %w", err)
		}
		streams = append(streams, s)
	}
	return streams, nil
}

// UpdateStream updates a stream's description and retention/partition config.
func (m *Manager) UpdateStream(ctx context.Context, streamID string, description *string, retention *common.RetentionPolicy, partition *common.PartitionConfig) (*common.Stream, error) {
	stream, err := m.GetStream(ctx, streamID)
	if err != nil {
		return nil, err
	}

	if description != nil {
		stream.Description = *description
	}

	if retention != nil {
		retJSON, err := json.Marshal(retention)
		if err != nil {
			return nil, fmt.Errorf("marshal retention: %w", err)
		}
		stream.RetentionPolicy = retJSON
	}

	if partition != nil {
		partJSON, err := json.Marshal(partition)
		if err != nil {
			return nil, fmt.Errorf("marshal partition: %w", err)
		}
		stream.PartitionConfig = partJSON
	}

	_, err = m.db.ExecContext(ctx,
		`UPDATE streams SET description = $1, retention_policy = $2, partition_config = $3, updated_at = $4
		 WHERE stream_id = $5`,
		stream.Description, stream.RetentionPolicy, stream.PartitionConfig, time.Now(), streamID)
	if err != nil {
		return nil, fmt.Errorf("update stream: %w", err)
	}

	return m.GetStream(ctx, streamID)
}

// TransitionState transitions a stream to a new state, enforcing the state machine.
func (m *Manager) TransitionState(ctx context.Context, streamID, newStatus string) error {
	stream, err := m.GetStream(ctx, streamID)
	if err != nil {
		return err
	}

	allowed := validTransitions[stream.Status]
	valid := false
	for _, s := range allowed {
		if s == newStatus {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("%w: cannot transition from %s to %s", common.ErrInvalidState, stream.Status, newStatus)
	}

	_, err = m.db.ExecContext(ctx,
		`UPDATE streams SET status = $1, updated_at = $2 WHERE stream_id = $3`,
		newStatus, time.Now(), streamID)
	if err != nil {
		return fmt.Errorf("transition stream state: %w", err)
	}

	return nil
}

// PauseStream pauses a stream (ACTIVE → PAUSED).
func (m *Manager) PauseStream(ctx context.Context, streamID string) error {
	return m.TransitionState(ctx, streamID, common.StreamStatusPaused)
}

// ResumeStream resumes a stream (PAUSED → ACTIVE).
func (m *Manager) ResumeStream(ctx context.Context, streamID string) error {
	return m.TransitionState(ctx, streamID, common.StreamStatusActive)
}

// DeleteStream initiates stream deletion (ACTIVE/PAUSED → DELETING).
func (m *Manager) DeleteStream(ctx context.Context, streamID string) error {
	return m.TransitionState(ctx, streamID, common.StreamStatusDeleting)
}

// CompleteDelete marks a stream as fully deleted (DELETING → DELETED).
func (m *Manager) CompleteDelete(ctx context.Context, streamID string) error {
	return m.TransitionState(ctx, streamID, common.StreamStatusDeleted)
}

// IsActive returns true if the stream is in ACTIVE state.
func (m *Manager) IsActive(ctx context.Context, streamID string) (bool, error) {
	stream, err := m.GetStream(ctx, streamID)
	if err != nil {
		return false, err
	}
	return stream.Status == common.StreamStatusActive, nil
}

// UpdateStats upserts daily statistics for a stream.
func (m *Manager) UpdateStats(ctx context.Context, streamID string, recordsIngested, bytesIngested int64) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO stream_stats (stream_id, stat_date, records_ingested, bytes_ingested)
		 VALUES ($1, CURRENT_DATE, $2, $3)
		 ON CONFLICT (stream_id, stat_date)
		 DO UPDATE SET records_ingested = stream_stats.records_ingested + $2,
		               bytes_ingested = stream_stats.bytes_ingested + $3`,
		streamID, recordsIngested, bytesIngested)
	if err != nil {
		return fmt.Errorf("update stats: %w", err)
	}
	return nil
}
