package domain

import (
	"context"
	"time"
)

// SchemaRepository persists schema aggregates.
type SchemaRepository interface {
	Save(ctx context.Context, schema *SchemaAggregate) error
	FindByID(ctx context.Context, id string) (*SchemaAggregate, error)
	FindByName(ctx context.Context, namespace, name string) (*SchemaAggregate, error)
	List(ctx context.Context, filter SchemaFilter, pagination Pagination) (*Page, error)
	Delete(ctx context.Context, id string) error
}

// SchemaFilter for listing schemas.
type SchemaFilter struct {
	Namespace *string
}

// StreamRepository persists stream aggregates.
type StreamRepository interface {
	Save(ctx context.Context, stream *StreamAggregate) error
	FindByID(ctx context.Context, id string) (*StreamAggregate, error)
	FindByName(ctx context.Context, name string) (*StreamAggregate, error)
	List(ctx context.Context, filter StreamFilter, pagination Pagination) (*Page, error)
	UpdateStatus(ctx context.Context, id string, status StreamStatus) error
	Delete(ctx context.Context, id string) error
}

// RecordRepository persists and queries records.
type RecordRepository interface {
	Append(ctx context.Context, streamID string, records []*RecordEntity) error
	QueryByTimeRange(ctx context.Context, streamID string, start, end time.Time, limit int, cursor string) (*Page, error)
	FindByKey(ctx context.Context, streamID string, keyField, keyValue string) (*RecordEntity, error)
}

// PipelineRepository persists pipeline aggregates.
type PipelineRepository interface {
	Save(ctx context.Context, pipeline *PipelineAggregate) error
	FindByID(ctx context.Context, id string) (*PipelineAggregate, error)
	FindByStreamID(ctx context.Context, streamID string) (*PipelineAggregate, error)
	Delete(ctx context.Context, id string) error
}
