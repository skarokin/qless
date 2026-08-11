package qless

import (
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultQueueSize        = 100
	defaultMaxPayloadBytes  = 1 << 20
	defaultExecutionTimeout = 30 * time.Second
	defaultBaseBackoff      = 100 * time.Millisecond
)

type Config struct {
	// QueueSize is the number of waiting jobs allowed in addition to active
	// workers. At most QueueSize+Workers payloads are retained. Default 100.
	QueueSize int
	// Workers is the maximum number of concurrently executing jobs. Default runtime.GOMAXPROCS(0).
	Workers int
	// MaxRetries is the number of additional attempts after the first failure.
	// Retry backoff occupies the same worker, so keep this bounded.
	MaxRetries int
	// MaxPayloadBytes bounds each HTTP request body. Default 1 MiB.
	MaxPayloadBytes int64
	// BaseBackoff is doubled after each failed attempt. Default 100 ms.
	BaseBackoff time.Duration
	// ExecutionTimeout is the timeout for each attempt. Default 30 seconds.
	ExecutionTimeout time.Duration
	// Backpressure controls what HTTPHandler does when the maximum number of
	// payloads is already in use. The default is immediate rejection with HTTP 503.
	Backpressure BackpressurePolicy

	// Logger defaults to slog.Default.
	Logger *slog.Logger
	// MeterProvider and TracerProvider are used to record metrics and traces. Default OpenTelemetry's global providers.
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
	// Propagator is used to propagate trace context and baggage between HTTP requests and background jobs. Default W3C Trace Context and Baggage.
	Propagator propagation.TextMapPropagator
}

type normalizedConfig struct {
	Config
}

func normalizeConfig(cfg Config) (normalizedConfig, error) {
	if cfg.QueueSize == 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.Workers == 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	if cfg.BaseBackoff == 0 {
		cfg.BaseBackoff = defaultBaseBackoff
	}
	if cfg.ExecutionTimeout == 0 {
		cfg.ExecutionTimeout = defaultExecutionTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = otel.GetTracerProvider()
	}
	if cfg.Propagator == nil {
		cfg.Propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}

	switch {
	case cfg.QueueSize < 1:
		return normalizedConfig{}, fmt.Errorf("qless: QueueSize must be positive")
	case cfg.Workers < 1:
		return normalizedConfig{}, fmt.Errorf("qless: Workers must be positive")
	case cfg.QueueSize > math.MaxInt-cfg.Workers:
		return normalizedConfig{}, fmt.Errorf("qless: QueueSize plus Workers is too large")
	case cfg.MaxRetries < 0:
		return normalizedConfig{}, fmt.Errorf("qless: MaxRetries cannot be negative")
	case cfg.MaxPayloadBytes < 1:
		return normalizedConfig{}, fmt.Errorf("qless: MaxPayloadBytes must be positive")
	case cfg.BaseBackoff < 0:
		return normalizedConfig{}, fmt.Errorf("qless: BaseBackoff cannot be negative")
	case cfg.ExecutionTimeout < 0:
		return normalizedConfig{}, fmt.Errorf("qless: ExecutionTimeout cannot be negative")
	case cfg.Backpressure.mode == backpressureBlockWithTimeout && cfg.Backpressure.timeout <= 0:
		return normalizedConfig{}, fmt.Errorf("qless: backpressure timeout must be positive")
	case cfg.Backpressure.mode > backpressureBlockWithTimeout:
		return normalizedConfig{}, fmt.Errorf("qless: invalid backpressure policy")
	}

	return normalizedConfig{Config: cfg}, nil
}
