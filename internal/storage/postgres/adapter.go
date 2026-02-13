package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Config holds PostgreSQL adapter configuration.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// Adapter provides operations against PostgreSQL.
type Adapter struct {
	db *sql.DB
}

// NewAdapter creates a new PostgreSQL adapter.
func NewAdapter(cfg Config) (*Adapter, error) {
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	return &Adapter{db: db}, nil
}

// Ping checks the PostgreSQL connection.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Close closes the PostgreSQL connection pool.
func (a *Adapter) Close() error {
	return a.db.Close()
}

// DB returns the underlying *sql.DB for direct queries.
func (a *Adapter) DB() *sql.DB {
	return a.db
}

// Exec executes a query that doesn't return rows.
func (a *Adapter) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row.
func (a *Adapter) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.db.QueryRowContext(ctx, query, args...)
}

// Query executes a query that returns rows.
func (a *Adapter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.db.QueryContext(ctx, query, args...)
}
