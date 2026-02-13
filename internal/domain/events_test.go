package domain

import (
	"sync"
	"testing"
	"time"
)

func TestEventBusSubscribeAndPublish(t *testing.T) {
	bus := NewEventBus()
	var received []string
	var mu sync.Mutex

	bus.Subscribe("schema.created", func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, e.EventType())
	})

	bus.Publish(SchemaCreated{
		SchemaID:  "sch-1",
		Name:      "test-schema",
		Namespace: "default",
		Version:   1,
		Timestamp: time.Now(),
		Actor:     "user-1",
	})

	// Unrelated event should not trigger handler
	bus.Publish(StreamCreated{
		StreamID:  "str-1",
		Name:      "test-stream",
		SchemaID:  "sch-1",
		Timestamp: time.Now(),
		Actor:     "user-1",
	})

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Errorf("expected 1 event, got %d", len(received))
	}
	if received[0] != "schema.created" {
		t.Errorf("expected 'schema.created', got %q", received[0])
	}
}

func TestEventBusSubscribeAll(t *testing.T) {
	bus := NewEventBus()
	var count int
	var mu sync.Mutex

	bus.SubscribeAll(func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		count++
	})

	bus.Publish(SchemaCreated{Timestamp: time.Now()})
	bus.Publish(StreamCreated{Timestamp: time.Now()})
	bus.Publish(RecordIngested{Timestamp: time.Now()})

	mu.Lock()
	defer mu.Unlock()
	if count != 3 {
		t.Errorf("expected 3 events via SubscribeAll, got %d", count)
	}
}

func TestEventBusMultipleHandlers(t *testing.T) {
	bus := NewEventBus()
	var count1, count2 int

	bus.Subscribe("stream.created", func(e Event) { count1++ })
	bus.Subscribe("stream.created", func(e Event) { count2++ })

	bus.Publish(StreamCreated{Timestamp: time.Now()})

	if count1 != 1 || count2 != 1 {
		t.Errorf("expected both handlers called once, got %d and %d", count1, count2)
	}
}

func TestEventTypes(t *testing.T) {
	tests := []struct {
		event    Event
		expected string
	}{
		{SchemaCreated{Timestamp: time.Now()}, "schema.created"},
		{SchemaVersionAdded{Timestamp: time.Now()}, "schema.version_added"},
		{SchemaVersionDeprecated{Timestamp: time.Now()}, "schema.version_deprecated"},
		{SchemaDeleted{Timestamp: time.Now()}, "schema.deleted"},
		{StreamCreated{Timestamp: time.Now()}, "stream.created"},
		{StreamStatusChanged{Timestamp: time.Now()}, "stream.status_changed"},
		{StreamDeleted{Timestamp: time.Now()}, "stream.deleted"},
		{PipelineUpdated{Timestamp: time.Now()}, "stream.pipeline_updated"},
		{RecordIngested{Timestamp: time.Now()}, "ingestion.record_ingested"},
		{RecordRejected{Timestamp: time.Now()}, "ingestion.record_rejected"},
		{BatchCompleted{Timestamp: time.Now()}, "ingestion.batch_completed"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			if got := tc.event.EventType(); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
			if tc.event.OccurredAt().IsZero() {
				t.Error("OccurredAt should not be zero")
			}
		})
	}
}
