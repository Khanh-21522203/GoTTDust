package domain

import (
	"sync"
	"time"
)

// Event is the base interface for all domain events.
type Event interface {
	EventType() string
	OccurredAt() time.Time
}

// --- Schema Events ---

// SchemaCreated is emitted when a new schema is created.
type SchemaCreated struct {
	SchemaID  string    `json:"schema_id"`
	Name      string    `json:"name"`
	Namespace string    `json:"namespace"`
	Version   int       `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Actor     ActorID   `json:"actor"`
}

func (e SchemaCreated) EventType() string    { return "schema.created" }
func (e SchemaCreated) OccurredAt() time.Time { return e.Timestamp }

// SchemaVersionAdded is emitted when a new version is added to a schema.
type SchemaVersionAdded struct {
	SchemaID    string    `json:"schema_id"`
	Version     int       `json:"version"`
	Fingerprint string    `json:"fingerprint"`
	Timestamp   time.Time `json:"timestamp"`
	Actor       ActorID   `json:"actor"`
}

func (e SchemaVersionAdded) EventType() string    { return "schema.version_added" }
func (e SchemaVersionAdded) OccurredAt() time.Time { return e.Timestamp }

// SchemaVersionDeprecated is emitted when a schema version is deprecated.
type SchemaVersionDeprecated struct {
	SchemaID  string    `json:"schema_id"`
	Version   int       `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Actor     ActorID   `json:"actor"`
}

func (e SchemaVersionDeprecated) EventType() string    { return "schema.version_deprecated" }
func (e SchemaVersionDeprecated) OccurredAt() time.Time { return e.Timestamp }

// SchemaDeleted is emitted when a schema is deleted.
type SchemaDeleted struct {
	SchemaID  string    `json:"schema_id"`
	Timestamp time.Time `json:"timestamp"`
	Actor     ActorID   `json:"actor"`
}

func (e SchemaDeleted) EventType() string    { return "schema.deleted" }
func (e SchemaDeleted) OccurredAt() time.Time { return e.Timestamp }

// --- Stream Events ---

// StreamCreated is emitted when a new stream is created.
type StreamCreated struct {
	StreamID  string    `json:"stream_id"`
	Name      string    `json:"name"`
	SchemaID  string    `json:"schema_id"`
	Timestamp time.Time `json:"timestamp"`
	Actor     ActorID   `json:"actor"`
}

func (e StreamCreated) EventType() string    { return "stream.created" }
func (e StreamCreated) OccurredAt() time.Time { return e.Timestamp }

// StreamStatusChanged is emitted when a stream's status changes.
type StreamStatusChanged struct {
	StreamID  string       `json:"stream_id"`
	OldStatus StreamStatus `json:"old_status"`
	NewStatus StreamStatus `json:"new_status"`
	Timestamp time.Time    `json:"timestamp"`
	Actor     ActorID      `json:"actor"`
}

func (e StreamStatusChanged) EventType() string    { return "stream.status_changed" }
func (e StreamStatusChanged) OccurredAt() time.Time { return e.Timestamp }

// StreamDeleted is emitted when a stream is deleted.
type StreamDeleted struct {
	StreamID  string    `json:"stream_id"`
	Timestamp time.Time `json:"timestamp"`
	Actor     ActorID   `json:"actor"`
}

func (e StreamDeleted) EventType() string    { return "stream.deleted" }
func (e StreamDeleted) OccurredAt() time.Time { return e.Timestamp }

// PipelineUpdated is emitted when a stream's pipeline is updated.
type PipelineUpdated struct {
	StreamID   string    `json:"stream_id"`
	PipelineID string    `json:"pipeline_id"`
	Timestamp  time.Time `json:"timestamp"`
	Actor      ActorID   `json:"actor"`
}

func (e PipelineUpdated) EventType() string    { return "stream.pipeline_updated" }
func (e PipelineUpdated) OccurredAt() time.Time { return e.Timestamp }

// --- Ingestion Events ---

// RecordIngested is emitted when a record is successfully ingested.
type RecordIngested struct {
	RecordID  string    `json:"record_id"`
	StreamID  string    `json:"stream_id"`
	Sequence  int64     `json:"sequence"`
	SizeBytes int       `json:"size_bytes"`
	Timestamp time.Time `json:"timestamp"`
}

func (e RecordIngested) EventType() string    { return "ingestion.record_ingested" }
func (e RecordIngested) OccurredAt() time.Time { return e.Timestamp }

// RecordRejected is emitted when a record is rejected.
type RecordRejected struct {
	StreamID  string    `json:"stream_id"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

func (e RecordRejected) EventType() string    { return "ingestion.record_rejected" }
func (e RecordRejected) OccurredAt() time.Time { return e.Timestamp }

// BatchCompleted is emitted when a batch import job completes.
type BatchCompleted struct {
	JobID            string    `json:"job_id"`
	StreamID         string    `json:"stream_id"`
	RecordsSucceeded int64     `json:"records_succeeded"`
	RecordsFailed    int64     `json:"records_failed"`
	Timestamp        time.Time `json:"timestamp"`
}

func (e BatchCompleted) EventType() string    { return "ingestion.batch_completed" }
func (e BatchCompleted) OccurredAt() time.Time { return e.Timestamp }

// --- EventBus ---

// EventHandler processes a domain event.
type EventHandler func(event Event)

// EventBus is a simple in-process event bus for domain events.
type EventBus struct {
	mu       sync.RWMutex
	handlers map[string][]EventHandler
}

// NewEventBus creates a new event bus.
func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[string][]EventHandler),
	}
}

// Subscribe registers a handler for an event type.
func (eb *EventBus) Subscribe(eventType string, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.handlers[eventType] = append(eb.handlers[eventType], handler)
}

// SubscribeAll registers a handler for all event types.
func (eb *EventBus) SubscribeAll(handler EventHandler) {
	eb.Subscribe("*", handler)
}

// Publish dispatches an event to all registered handlers.
func (eb *EventBus) Publish(event Event) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	// Type-specific handlers
	for _, h := range eb.handlers[event.EventType()] {
		h(event)
	}
	// Wildcard handlers
	for _, h := range eb.handlers["*"] {
		h(event)
	}
}
