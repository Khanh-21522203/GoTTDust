package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"GoTTDust/internal/common"
)

// Logger writes audit log entries to PostgreSQL.
type Logger struct {
	db *sql.DB
}

// NewLogger creates a new audit logger.
func NewLogger(db *sql.DB) *Logger {
	return &Logger{db: db}
}

// Log writes an audit log entry.
func (l *Logger) Log(ctx context.Context, entry common.AuditLog) error {
	_, err := l.db.ExecContext(ctx,
		`INSERT INTO audit_log (actor, action, resource_type, resource_id, details, ip_address, trace_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		entry.Actor, entry.Action, entry.ResourceType, entry.ResourceID,
		entry.Details, nullString(entry.IPAddress), nullString(entry.TraceID))
	if err != nil {
		return fmt.Errorf("write audit log: %w", err)
	}
	return nil
}

// LogAction is a convenience method for logging a simple action.
func (l *Logger) LogAction(ctx context.Context, actor, action, resourceType, resourceID string, details interface{}) error {
	var detailsJSON json.RawMessage
	if details != nil {
		data, err := json.Marshal(details)
		if err != nil {
			return fmt.Errorf("marshal details: %w", err)
		}
		detailsJSON = data
	}

	return l.Log(ctx, common.AuditLog{
		Actor:        actor,
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Details:      detailsJSON,
		TraceID:      common.GenerateTraceID(),
	})
}

// Query returns audit log entries filtered by resource.
func (l *Logger) Query(ctx context.Context, resourceType, resourceID string, limit int) ([]*common.AuditLog, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := l.db.QueryContext(ctx,
		`SELECT log_id, timestamp, actor, action, resource_type, resource_id, details, ip_address, trace_id
		 FROM audit_log
		 WHERE ($1 = '' OR resource_type = $1) AND ($2 = '' OR resource_id = $2)
		 ORDER BY timestamp DESC LIMIT $3`,
		resourceType, resourceID, limit)
	if err != nil {
		return nil, fmt.Errorf("query audit log: %w", err)
	}
	_ = rows.Close()

	var entries []*common.AuditLog
	for rows.Next() {
		e := &common.AuditLog{}
		var ipAddr, traceID sql.NullString
		if err := rows.Scan(&e.LogID, &e.Timestamp, &e.Actor, &e.Action,
			&e.ResourceType, &e.ResourceID, &e.Details, &ipAddr, &traceID); err != nil {
			return nil, fmt.Errorf("scan audit log: %w", err)
		}
		if ipAddr.Valid {
			e.IPAddress = ipAddr.String
		}
		if traceID.Valid {
			e.TraceID = traceID.String
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
