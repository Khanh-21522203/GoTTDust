package common

import (
	"errors"
	"fmt"
)

// Domain errors
var (
	ErrValidation   = errors.New("validation error")
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrInvalidState = errors.New("invalid state")
)

// Infrastructure errors
var (
	ErrConnection = errors.New("connection error")
	ErrIO         = errors.New("io error")
	ErrEncoding   = errors.New("encoding error")
	ErrCapacity   = errors.New("capacity exceeded")
)

// External errors
var (
	ErrTimeout        = errors.New("timeout")
	ErrRateLimit      = errors.New("rate limited")
	ErrUnavailable    = errors.New("service unavailable")
	ErrAuthentication = errors.New("authentication error")
	ErrAuthorization  = errors.New("authorization error")
)

// AppError is the unified error type with context and trace ID.
type AppError struct {
	Kind    error
	Message string
	TraceID string
	Cause   error
	Fields  map[string]string
}

func (e *AppError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.TraceID, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.TraceID, e.Message)
}

func (e *AppError) Unwrap() error {
	return e.Kind
}

func NewAppError(kind error, message string, traceID string) *AppError {
	return &AppError{
		Kind:    kind,
		Message: message,
		TraceID: traceID,
		Fields:  make(map[string]string),
	}
}

func (e *AppError) WithCause(cause error) *AppError {
	e.Cause = cause
	return e
}

func (e *AppError) WithField(key, value string) *AppError {
	e.Fields[key] = value
	return e
}

// ValidationError provides field-level validation error details.
type ValidationError struct {
	Errors []FieldError
}

type FieldError struct {
	Path       string `json:"path"`
	Message    string `json:"message"`
	SchemaPath string `json:"schema_path"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %d error(s)", len(e.Errors))
}

func NewValidationError(errs ...FieldError) *ValidationError {
	return &ValidationError{Errors: errs}
}
