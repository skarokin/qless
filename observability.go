package qless

import (
	"context"
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
	meter      metric.Meter

	received      metric.Int64Counter
	enqueued      metric.Int64Counter
	executions    metric.Int64Counter
	retries       metric.Int64Counter
	exhausted     metric.Int64Counter
	finalFailures metric.Int64Counter
	backpressure  metric.Int64Counter
	taskDuration  metric.Float64Histogram
	queueDuration metric.Float64Histogram
	waitDuration  metric.Float64Histogram
	payloadSize   metric.Int64Histogram

	queueDepth      metric.Int64ObservableGauge
	activeJobs      metric.Int64ObservableGauge
	outstandingJobs metric.Int64ObservableGauge
	jobCapacity     metric.Int64ObservableGauge
	pendingEnqueues metric.Int64ObservableGauge
	accepting       metric.Int64ObservableGauge
	registration    metric.Registration
}

func newObservability(cfg normalizedConfig) (*observability, error) {
	meter := cfg.MeterProvider.Meter(instrumentationName)
	o := &observability{
		logger:     cfg.Logger.With("component", "qless"),
		tracer:     cfg.TracerProvider.Tracer(instrumentationName),
		propagator: cfg.Propagator,
		meter:      meter,
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
	if o.queueDuration, err = meter.Float64Histogram(
		"qless.task.queue.duration",
		metric.WithDescription("Time from accepting a job until a worker begins processing it"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, fmt.Errorf("qless: create queue duration metric: %w", err)
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
	if o.queueDepth, err = meter.Int64ObservableGauge(
		"qless.queue.depth",
		metric.WithDescription("Jobs currently waiting in the in-memory queue"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create queue depth metric: %w", err)
	}
	if o.activeJobs, err = meter.Int64ObservableGauge(
		"qless.jobs.active",
		metric.WithDescription("Jobs currently executing or waiting to retry"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create active jobs metric: %w", err)
	}
	if o.outstandingJobs, err = meter.Int64ObservableGauge(
		"qless.jobs.outstanding",
		metric.WithDescription("Accepted jobs that have not finished, including queued and active jobs"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create outstanding jobs metric: %w", err)
	}
	if o.jobCapacity, err = meter.Int64ObservableGauge(
		"qless.jobs.capacity",
		metric.WithDescription("Maximum number of accepted jobs the processor can retain"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create job capacity metric: %w", err)
	}
	if o.pendingEnqueues, err = meter.Int64ObservableGauge(
		"qless.enqueues.pending",
		metric.WithDescription("HTTP enqueue requests currently attempting to acquire payload capacity"),
		metric.WithUnit("{request}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create pending enqueues metric: %w", err)
	}
	if o.accepting, err = meter.Int64ObservableGauge(
		"qless.processor.accepting",
		metric.WithDescription("Whether the processor is accepting jobs (1) or not (0)"),
		metric.WithUnit("1"),
	); err != nil {
		return nil, fmt.Errorf("qless: create accepting metric: %w", err)
	}

	return o, nil
}

func (o *observability) registerProcessorMetrics(p *Processor) error {
	registration, err := o.meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			stats := p.Stats()
			observer.ObserveInt64(o.queueDepth, int64(stats.QueueDepth))
			observer.ObserveInt64(o.activeJobs, stats.ActiveJobs)
			observer.ObserveInt64(o.outstandingJobs, int64(stats.OutstandingJobs))
			observer.ObserveInt64(o.jobCapacity, int64(stats.Capacity))
			observer.ObserveInt64(o.pendingEnqueues, stats.PendingEnqueues)
			if stats.Accepting {
				observer.ObserveInt64(o.accepting, 1)
			} else {
				observer.ObserveInt64(o.accepting, 0)
			}
			return nil
		},
		o.queueDepth,
		o.activeJobs,
		o.outstandingJobs,
		o.jobCapacity,
		o.pendingEnqueues,
		o.accepting,
	)
	if err != nil {
		return fmt.Errorf("qless: register processor metrics: %w", err)
	}
	o.registration = registration
	return nil
}

func (o *observability) unregisterProcessorMetrics() {
	if o.registration != nil {
		_ = o.registration.Unregister()
	}
}
