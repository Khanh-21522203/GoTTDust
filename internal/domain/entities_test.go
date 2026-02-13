package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func intPtr(v int) *int { return &v }

func TestSchemaAggregateValidate(t *testing.T) {
	validSchema := SchemaAggregate{
		ID:        "sch-123",
		Name:      "user-events",
		Namespace: "default",
		Versions: []SchemaVersionEntity{
			{ID: "sv-1", SchemaID: "sch-123", Version: 1, Status: VersionActive},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	tests := []struct {
		name    string
		mutate  func(s SchemaAggregate) SchemaAggregate
		wantErr bool
	}{
		{"valid schema", func(s SchemaAggregate) SchemaAggregate { return s }, false},
		{"invalid name", func(s SchemaAggregate) SchemaAggregate { s.Name = "AB"; return s }, true},
		{"no versions", func(s SchemaAggregate) SchemaAggregate { s.Versions = nil; return s }, true},
		{"no active version", func(s SchemaAggregate) SchemaAggregate {
			s.Versions = []SchemaVersionEntity{{ID: "sv-1", Status: VersionDeprecated}}
			return s
		}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := tc.mutate(validSchema)
			err := schema.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
		})
	}
}

func TestStreamAggregateValidate(t *testing.T) {
	validStream := StreamAggregate{
		ID:       "str-123",
		Name:     "click-events",
		SchemaID: "sch-456",
		Status:   StreamStatusActive,
		RetentionPolicy: RetentionPolicyValue{
			PolicyType:   RetentionTimeBased,
			DurationDays: intPtr(30),
		},
	}

	tests := []struct {
		name    string
		mutate  func(s StreamAggregate) StreamAggregate
		wantErr bool
	}{
		{"valid stream", func(s StreamAggregate) StreamAggregate { return s }, false},
		{"invalid name", func(s StreamAggregate) StreamAggregate { s.Name = "AB"; return s }, true},
		{"missing schema_id", func(s StreamAggregate) StreamAggregate { s.SchemaID = ""; return s }, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream := tc.mutate(validStream)
			err := stream.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
		})
	}
}

func TestPipelineAggregateValidate(t *testing.T) {
	validPipeline := PipelineAggregate{
		ID:       "pip-123",
		Name:     "transform-pipeline",
		StreamID: "str-456",
		Status:   PipelineActive,
		Stages: []PipelineStageEntity{
			{ID: "stg-1", PipelineID: "pip-123", Order: 1, StageType: StageFieldProjection, Config: json.RawMessage(`{}`), Enabled: true},
		},
	}

	tests := []struct {
		name    string
		mutate  func(p PipelineAggregate) PipelineAggregate
		wantErr bool
	}{
		{"valid pipeline", func(p PipelineAggregate) PipelineAggregate { return p }, false},
		{"no stages", func(p PipelineAggregate) PipelineAggregate { p.Stages = nil; return p }, true},
		{"invalid stage type", func(p PipelineAggregate) PipelineAggregate {
			p.Stages = []PipelineStageEntity{{Order: 1, StageType: StageType("invalid")}}
			return p
		}, true},
		{"non-contiguous order", func(p PipelineAggregate) PipelineAggregate {
			p.Stages = []PipelineStageEntity{
				{Order: 1, StageType: StageFieldProjection},
				{Order: 3, StageType: StageFieldProjection},
			}
			return p
		}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := tc.mutate(validPipeline)
			err := pipeline.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
		})
	}
}

func TestSoftDelete(t *testing.T) {
	s := &SchemaAggregate{
		ID:        "sch-123",
		Name:      "test-schema",
		Namespace: "default",
	}

	if s.IsDeleted() {
		t.Error("new schema should not be deleted")
	}

	s.MarkDeleted(time.Now())

	if !s.IsDeleted() {
		t.Error("schema should be deleted after MarkDeleted")
	}
	if s.DeletedAt == nil {
		t.Error("DeletedAt should be set")
	}
}

func TestStreamTransitionStatus(t *testing.T) {
	s := &StreamAggregate{
		ID:       "str-123",
		Name:     "test-stream",
		SchemaID: "sch-456",
		Status:   StreamStatusCreating,
	}

	// Valid transition: creating -> active
	if err := s.TransitionStatus(StreamStatusActive); err != nil {
		t.Errorf("expected valid transition, got: %v", err)
	}
	if s.Status != StreamStatusActive {
		t.Errorf("expected status active, got %s", s.Status)
	}

	// Invalid transition: active -> creating
	if err := s.TransitionStatus(StreamStatusCreating); err == nil {
		t.Error("expected error for invalid transition, got nil")
	}
}
