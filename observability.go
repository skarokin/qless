package qless

import (
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/skarokin/qless"

type observability struct {
	logger     *slog.Logger
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator

	received      metric.Int64Counter
	enqueued      metric.Int64Counter
	executions    metric.Int64Counter
	retries       metric.Int64Counter
	exhausted     metric.Int64Counter
	finalFailures metric.Int64Counter
	backpressure  metric.Int64Counter
	taskDuration  metric.Float64Histogram
	waitDuration  metric.Float64Histogram
	payloadSize   metric.Int64Histogram
	queueDepth    metric.Int64UpDownCounter
	activeWorkers metric.Int64UpDownCounter
}

func newObservability(cfg normalizedConfig) (*observability, error) {
	meter := cfg.MeterProvider.Meter(instrumentationName)
	o := &observability{
		logger:     cfg.Logger.With("component", "qless"),
		tracer:     cfg.TracerProvider.Tracer(instrumentationName),
		propagator: cfg.Propagator,
	}

	var err error
	if o.received, err = meter.Int64Counter(
		"qless.tasks.received",
		metric.WithDescription("Jobs received by the HTTP ingestion handler"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create received metric: %w", err)
	}
	if o.enqueued, err = meter.Int64Counter(
		"qless.tasks.enqueued",
		metric.WithDescription("Jobs accepted into the in-memory queue"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create enqueued metric: %w", err)
	}
	if o.executions, err = meter.Int64Counter(
		"qless.task.executions",
		metric.WithDescription("Job execution attempts"),
		metric.WithUnit("{attempt}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create executions metric: %w", err)
	}
	if o.retries, err = meter.Int64Counter(
		"qless.task.retries",
		metric.WithDescription("Job retries scheduled after failed attempts"),
		metric.WithUnit("{retry}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create retries metric: %w", err)
	}
	if o.exhausted, err = meter.Int64Counter(
		"qless.task.exhausted",
		metric.WithDescription("Jobs that exhausted their configured retries"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create exhausted metric: %w", err)
	}
	if o.finalFailures, err = meter.Int64Counter(
		"qless.task.final_failures",
		metric.WithDescription("Jobs that ended without succeeding"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create final failures metric: %w", err)
	}
	if o.backpressure, err = meter.Int64Counter(
		"qless.backpressure.events",
		metric.WithDescription("Payload-limit backpressure outcomes"),
		metric.WithUnit("{event}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create backpressure metric: %w", err)
	}
	if o.taskDuration, err = meter.Float64Histogram(
		"qless.task.duration",
		metric.WithDescription("Duration of each job execution attempt"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, fmt.Errorf("qless: create task duration metric: %w", err)
	}
	if o.waitDuration, err = meter.Float64Histogram(
		"qless.enqueue.wait.duration",
		metric.WithDescription("Time spent waiting for payload space"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, fmt.Errorf("qless: create wait duration metric: %w", err)
	}
	if o.payloadSize, err = meter.Int64Histogram(
		"qless.task.payload.size",
		metric.WithDescription("Size of received job payloads"),
		metric.WithUnit("By"),
	); err != nil {
		return nil, fmt.Errorf("qless: create payload size metric: %w", err)
	}
	if o.queueDepth, err = meter.Int64UpDownCounter(
		"qless.queue.depth",
		metric.WithDescription("Jobs currently waiting in the in-memory queue"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create queue depth metric: %w", err)
	}
	if o.activeWorkers, err = meter.Int64UpDownCounter(
		"qless.workers.active",
		metric.WithDescription("Workers currently executing jobs"),
		metric.WithUnit("{worker}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create active workers metric: %w", err)
	}

	return o, nil
}