package observability

import (
	"context"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// LogConfig holds logging configuration per observability.md §3.5.
type LogConfig struct {
	Level         string `mapstructure:"level"`          // debug, info, warn, error
	Format        string `mapstructure:"format"`         // json, pretty
	Output        string `mapstructure:"output"`         // stdout, file
	FilePath      string `mapstructure:"file_path"`
	IncludeTarget bool   `mapstructure:"include_target"`
	IncludeSpan   bool   `mapstructure:"include_span"`
}

// Logger wraps logrus with TTDust conventions.
type Logger struct {
	*logrus.Logger
}

// NewLogger creates a structured logger per observability.md §3.
func NewLogger(cfg LogConfig) *Logger {
	l := logrus.New()

	// Set level
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	l.SetLevel(level)

	// Set format
	switch cfg.Format {
	case "pretty":
		l.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000000Z",
		})
	default: // json
		l.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000000Z",
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
		})
	}

	// Set output
	switch cfg.Output {
	case "file":
		if cfg.FilePath != "" {
			f, err := os.OpenFile(cfg.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				l.SetOutput(io.MultiWriter(os.Stdout, f))
			}
		}
	default:
		l.SetOutput(os.Stdout)
	}

	return &Logger{Logger: l}
}

// DefaultLogger returns a logger with sensible defaults.
func DefaultLogger() *Logger {
	return NewLogger(LogConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	})
}

type logContextKey string

const traceIDKey logContextKey = "trace_id"

// WithTraceID adds a trace ID to the context for log correlation.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFromContext extracts the trace ID from context.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

// WithContext returns a log entry enriched with trace context.
func (l *Logger) WithContext(ctx context.Context) *logrus.Entry {
	entry := l.Logger.WithContext(ctx)
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		entry = entry.WithField("trace_id", traceID)
	}
	return entry
}

// ForComponent returns a logger scoped to a specific component target.
func (l *Logger) ForComponent(component string) *logrus.Entry {
	return l.Logger.WithField("target", "ttdust::"+component)
}
