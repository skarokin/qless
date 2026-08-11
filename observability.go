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
	rejected      metric.Int64Counter
	executions    metric.Int64Counter
	retries       metric.Int64Counter
	finalFailures metric.Int64Counter
	backpressure  metric.Int64Counter
	jobDuration   metric.Float64Histogram
	queueDuration metric.Float64Histogram
	waitDuration  metric.Float64Histogram
	payloadSize   metric.Int64Histogram

	queueDepth      metric.Int64ObservableGauge
	activeJobs      metric.Int64ObservableGauge
	outstandingJobs metric.Int64ObservableGauge
	jobCapacity     metric.Int64ObservableGauge
	pendingEnqueues metric.Int64ObservableGauge
	accepting       metric.Int64ObservableGauge
	workers         metric.Int64ObservableGauge
	registration    metric.Registration
}

// Histogram bucket advice. The OTel SDK's default boundaries are tuned for
// millisecond magnitudes; qless records seconds and bytes, so without advice
// every realistic value would land in the lowest default buckets.
var (
	// attemptDurationBuckets covers handler execution: milliseconds to minutes.
	attemptDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}
	// queueWaitBuckets covers in-process waits: sub-millisecond when idle to seconds under load.
	queueWaitBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	// payloadSizeBuckets covers typical JSON payloads: bytes to megabytes.
	payloadSizeBuckets = []float64{256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216}
)

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
		"qless.jobs.received",
		metric.WithDescription("Jobs received by the HTTP ingestion handler"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create received metric: %w", err)
	}
	if o.enqueued, err = meter.Int64Counter(
		"qless.jobs.enqueued",
		metric.WithDescription("Jobs accepted into the in-memory queue"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create enqueued metric: %w", err)
	}
	if o.rejected, err = meter.Int64Counter(
		"qless.jobs.rejected",
		metric.WithDescription("Jobs rejected by the HTTP handler before enqueue, excluding backpressure (see qless.backpressure.events)"),
		metric.WithUnit("{job}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create rejected metric: %w", err)
	}
	if o.executions, err = meter.Int64Counter(
		"qless.jobs.executions",
		metric.WithDescription("Job execution attempts"),
		metric.WithUnit("{attempt}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create executions metric: %w", err)
	}
	if o.retries, err = meter.Int64Counter(
		"qless.jobs.retries",
		metric.WithDescription("Job retries scheduled after failed attempts"),
		metric.WithUnit("{retry}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create retries metric: %w", err)
	}
	if o.finalFailures, err = meter.Int64Counter(
		"qless.jobs.final_failures",
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
	if o.jobDuration, err = meter.Float64Histogram(
		"qless.job.duration",
		metric.WithDescription("Duration of each job execution attempt"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(attemptDurationBuckets...),
	); err != nil {
		return nil, fmt.Errorf("qless: create job duration metric: %w", err)
	}
	if o.queueDuration, err = meter.Float64Histogram(
		"qless.job.queue.duration",
		metric.WithDescription("Time from accepting a job until a worker begins processing it"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(queueWaitBuckets...),
	); err != nil {
		return nil, fmt.Errorf("qless: create queue duration metric: %w", err)
	}
	if o.waitDuration, err = meter.Float64Histogram(
		"qless.enqueue.wait.duration",
		metric.WithDescription("Time spent waiting for payload space"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(queueWaitBuckets...),
	); err != nil {
		return nil, fmt.Errorf("qless: create wait duration metric: %w", err)
	}
	if o.payloadSize, err = meter.Int64Histogram(
		"qless.job.payload.size",
		metric.WithDescription("Size of received job payloads"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(payloadSizeBuckets...),
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
	if o.workers, err = meter.Int64ObservableGauge(
		"qless.workers.configured",
		metric.WithDescription("Configured worker pool size; divide qless.jobs.active by this for pool utilization"),
		metric.WithUnit("{worker}"),
	); err != nil {
		return nil, fmt.Errorf("qless: create workers metric: %w", err)
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
			observer.ObserveInt64(o.workers, int64(stats.Workers))
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
		o.workers,
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
