package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// TracingConfig holds tracing configuration per observability.md §4.4.
type TracingConfig struct {
	Enabled      bool    `mapstructure:"enabled"`
	ServiceName  string  `mapstructure:"service_name"`
	Exporter     string  `mapstructure:"exporter"`      // otlp, stdout
	OTLPEndpoint string  `mapstructure:"otlp_endpoint"` // e.g. localhost:4317
	SampleRatio  float64 `mapstructure:"sample_ratio"`  // 0.0 to 1.0
}

// InitTracing initializes the OpenTelemetry tracer provider per observability.md §4.
func InitTracing(ctx context.Context, cfg TracingConfig) (*sdktrace.TracerProvider, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = "ttdust"
	}
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 0.1 // 10% default for production
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	switch cfg.Exporter {
	case "otlp":
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("create OTLP exporter: %w", err)
		}
		exporter = exp
	case "stdout":
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("create stdout exporter: %w", err)
		}
		exporter = exp
	default:
		return nil, fmt.Errorf("unsupported exporter: %s", cfg.Exporter)
	}

	// Sampler
	var sampler sdktrace.Sampler
	if cfg.SampleRatio >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

// Tracer returns a named tracer for a component.
func Tracer(component string) trace.Tracer {
	return otel.Tracer("ttdust/" + component)
}

// Common span attribute helpers per observability.md §4.3.
func StreamIDAttr(id string) attribute.KeyValue {
	return attribute.String("ttdust.stream.id", id)
}

func SchemaIDAttr(id string) attribute.KeyValue {
	return attribute.String("ttdust.schema.id", id)
}

func RecordIDAttr(id string) attribute.KeyValue {
	return attribute.String("ttdust.record.id", id)
}

func PipelineIDAttr(id string) attribute.KeyValue {
	return attribute.String("ttdust.pipeline.id", id)
}
