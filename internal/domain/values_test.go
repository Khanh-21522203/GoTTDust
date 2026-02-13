package domain

import (
	"fmt"
	"testing"
)

func TestNewSchemaName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
	}{
		{"valid simple", "user-events", false},
		{"valid with numbers", "a123", false},
		{"valid long", "my-long-schema-name-123", false},
		{"valid min length", "abc", false},
		{"invalid uppercase", "UserEvents", true},
		{"invalid too short", "ab", true},
		{"invalid starts with number", "123schema", true},
		{"invalid empty", "", true},
		{"invalid spaces", "user events", true},
		{"invalid underscore", "user_events", true},
		{"invalid starts with dash", "-events", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sn, err := NewSchemaName(tc.input)
			if tc.wantErr && err == nil {
				t.Errorf("expected %q to be invalid, got no error", tc.input)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected %q to be valid, got error: %v", tc.input, err)
			}
			if !tc.wantErr && sn.Value() != tc.input {
				t.Errorf("expected value %q, got %q", tc.input, sn.Value())
			}
		})
	}
}

func TestNewStreamName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid", "click-events", false},
		{"invalid uppercase", "ClickEvents", true},
		{"invalid too short", "ab", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewStreamName(tc.input)
			if tc.wantErr && err == nil {
				t.Errorf("expected %q to be invalid", tc.input)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected %q to be valid, got: %v", tc.input, err)
			}
		})
	}
}

func TestNewNamespace(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid", "production", false},
		{"valid single char", "a", false},
		{"invalid uppercase", "Production", true},
		{"invalid starts with number", "1prod", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewNamespace(tc.input)
			if tc.wantErr && err == nil {
				t.Errorf("expected %q to be invalid", tc.input)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected %q to be valid, got: %v", tc.input, err)
			}
		})
	}
}

func TestParseFieldPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "user_id", false},
		{"valid nested", "data.user.name", false},
		{"invalid empty", "", true},
		{"invalid empty segment", "data..name", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFieldPath(tc.input)
			if tc.wantErr && err == nil {
				t.Errorf("expected %q to be invalid", tc.input)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected %q to be valid, got: %v", tc.input, err)
			}
		})
	}
}

func TestActorID(t *testing.T) {
	// ActorID is a type alias; verify basic usage
	actor := ActorID("user-123")
	if string(actor) != "user-123" {
		t.Errorf("expected 'user-123', got %q", actor)
	}

	empty := ActorID("")
	if string(empty) != "" {
		t.Errorf("expected empty string, got %q", empty)
	}
}

func TestStreamStatusCanTransitionTo(t *testing.T) {
	tests := []struct {
		from     StreamStatus
		to       StreamStatus
		expected bool
	}{
		{StreamStatusCreating, StreamStatusActive, true},
		{StreamStatusActive, StreamStatusPaused, true},
		{StreamStatusActive, StreamStatusDeleting, true},
		{StreamStatusPaused, StreamStatusActive, true},
		{StreamStatusPaused, StreamStatusDeleting, true},
		{StreamStatusDeleting, StreamStatusDeleted, true},
		{StreamStatusActive, StreamStatusCreating, false},
		{StreamStatusDeleted, StreamStatusActive, false},
		{StreamStatusCreating, StreamStatusPaused, false},
		{StreamStatusCreating, StreamStatusDeleted, false},
		{StreamStatusDeleted, StreamStatusDeleting, false},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s_to_%s", tc.from, tc.to), func(t *testing.T) {
			result := tc.from.CanTransitionTo(tc.to)
			if result != tc.expected {
				t.Errorf("expected transition from %s to %s to be %v, got %v",
					tc.from, tc.to, tc.expected, result)
			}
		})
	}
}
