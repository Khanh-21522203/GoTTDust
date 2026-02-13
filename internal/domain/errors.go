package domain

import (
	"errors"
	"fmt"
)

// Sentinel domain errors.
var (
	ErrValidation             = errors.New("validation error")
	ErrNotFound               = errors.New("not found")
	ErrConflict               = errors.New("conflict")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrSchemaIncompatible     = errors.New("schema incompatible")
	ErrProcessing             = errors.New("processing error")
)

// DomainError is a rich domain error with context.
type DomainError struct {
	Kind    error
	Message string
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

func (e *DomainError) Unwrap() error {
	return e.Kind
}

// NewDomainError creates a new domain error.
func NewDomainError(kind error, message string) *DomainError {
	return &DomainError{Kind: kind, Message: message}
}

// NotFoundError creates a not-found error for an entity.
func NotFoundError(entityType, id string) *DomainError {
	return &DomainError{
		Kind:    ErrNotFound,
		Message: fmt.Sprintf("%s with id %s not found", entityType, id),
	}
}

// ConflictError creates a conflict error.
func ConflictError(message string) *DomainError {
	return &DomainError{Kind: ErrConflict, Message: message}
}

// ValidationError provides field-level validation detail.
type ValidationError struct {
	Field      string `json:"field"`
	Message    string `json:"message"`
	Constraint string `json:"constraint,omitempty"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s: %s", e.Field, e.Message)
}

func (e *ValidationError) Unwrap() error {
	return ErrValidation
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors struct {
	Errors []*ValidationError
}

func (e *ValidationErrors) Error() string {
	return fmt.Sprintf("validation failed: %d error(s)", len(e.Errors))
}

func (e *ValidationErrors) Unwrap() error {
	return ErrValidation
}

// Add appends a validation error.
func (e *ValidationErrors) Add(field, message string) {
	e.Errors = append(e.Errors, &ValidationError{Field: field, Message: message})
}

// HasErrors returns true if there are any errors.
func (e *ValidationErrors) HasErrors() bool {
	return len(e.Errors) > 0
}

// OrNil returns nil if no errors, or the ValidationErrors.
func (e *ValidationErrors) OrNil() error {
	if !e.HasErrors() {
		return nil
	}
	return e
}

// ProcessingError describes a pipeline processing failure.
type ProcessingError struct {
	StageType StageType
	Field     string
	Message   string
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("processing error at %s: %s", e.StageType, e.Message)
}

func (e *ProcessingError) Unwrap() error {
	return ErrProcessing
}
