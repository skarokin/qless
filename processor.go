package qless

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Handler is the user's function passed in to the processor that will be executed for each request.
// The payload is the raw HTTP request body. Returning an error wrapped with Permanent skips retries.
type Handler func(context.Context, []byte) error

type processorState int

const (
	stateCreated processorState = iota
	stateRunning
	stateStopping
	stateStopped
)

// Stats is a point-in-time snapshot of a Processor. Its fields are also exported as
// OpenTelemetry observable gauges from the configured MeterProvider.
type Stats struct {
	// QueueDepth is the number of jobs waiting for a worker. Active jobs are excluded.
	QueueDepth int `json:"queue_depth"`
	// ActiveJobs is the number of jobs executing or waiting to retry.
	ActiveJobs int64 `json:"active_jobs"`
	// OutstandingJobs is the number of accepted jobs that have not finished.
	// It includes both queued and active jobs and is the appropriate signal for keep-alive decisions.
	OutstandingJobs int `json:"outstanding_jobs"`
	// Capacity is the maximum number of jobs the processor can retain.
	Capacity int `json:"capacity"`
	// PendingEnqueues is the number of HTTP requests currently attempting to enqueue,
	// including requests blocked by the backpressure policy.
	PendingEnqueues int64 `json:"pending_enqueues"`
	// Accepting reports whether the processor currently accepts new jobs.
	Accepting bool `json:"accepting"`
}

// Processor accepts jobs over HTTP and executes them on a bounded in-memory worker pool.
// Create one with New, call Start, mount HTTPHandler on a POST route, and call Shutdown during application teardown.
type Processor struct {
	cfg     normalizedConfig
	handler Handler
	obs     *observability

	// queue holds jobs waiting for a worker. Its capacity equals the payload
	// slot capacity (QueueSize+Workers) so a send never blocks once a slot is held.
	queue chan *job
	// slots bounds retained payloads: acquire by sending, release by receiving. it is a buffered channel so
	// sending to a full channel will block until a slot is available and receiving from channel will free up a slot.
	// a payload is retained from the moment the handler accepts a request body until a worker finishes executing it.
	slots chan struct{}

	// mu guards state transitions. Handlers take the read lock only to check state and register with enqueueWG.
	mu    sync.RWMutex
	state processorState

	// stopCh is closed when Shutdown begins so blocked enqueues bail out immediately.
	stopCh chan struct{}
	// abandoning is set when graceful shutdown ran out of time: workers drain
	// the remaining queue by abandoning jobs instead of executing them.
	abandoning atomic.Bool

	enqueueWG       sync.WaitGroup
	pendingEnqueues atomic.Int64
	workersWG       sync.WaitGroup
	activeJobs      atomic.Int64

	// baseCtx is the parent of every execution attempt; cancelled on hard shutdown.
	baseCtx   context.Context
	cancelAll context.CancelFunc

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

// New creates a Processor from cfg and the user's handler. It validates the config and registers instruments
// but does not start any workers; call Start before serving traffic.
func New(cfg Config, handler Handler) (*Processor, error) {
	if handler == nil {
		return nil, errors.New("qless: handler must not be nil")
	}
	normalizedCfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	obs, err := newObservability(normalizedCfg)
	if err != nil {
		return nil, err
	}

	capacity := normalizedCfg.QueueSize + normalizedCfg.Workers
	p := &Processor{
		cfg:          normalizedCfg,
		handler:      handler,
		obs:          obs,
		queue:        make(chan *job, capacity),
		slots:        make(chan struct{}, capacity),
		stopCh:       make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	if err := obs.registerProcessorMetrics(p); err != nil {
		return nil, err
	}
	return p, nil
}

// Start starts the worker pool which will begin executing jobs as they are received.
// It returns ErrAlreadyStarted if called twice and ErrStopped after Shutdown.
func (p *Processor) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch p.state {
	case stateRunning:
		return ErrAlreadyStarted
	case stateStopping, stateStopped:
		return ErrStopped
	}

	p.baseCtx, p.cancelAll = context.WithCancel(context.Background())
	for range p.cfg.Workers {
		p.workersWG.Add(1)
		go p.worker()
	}
	p.state = stateRunning

	p.obs.logger.Info("processor started",
		"workers", p.cfg.Workers,
		"queue_size", p.cfg.QueueSize,
		"max_retries", p.cfg.MaxRetries,
		"execution_timeout", p.cfg.ExecutionTimeout,
	)
	return nil
}

// HTTPHandler returns the handler that is used at the application level.
func (p *Processor) HTTPHandler() http.Handler {
	return http.HandlerFunc(p.serveHTTP)
}

// Stats returns a point-in-time snapshot of current processor activity. It is safe to call from any goroutine.
func (p *Processor) Stats() Stats {
	p.mu.RLock()
	accepting := p.state == stateRunning
	p.mu.RUnlock()

	return Stats{
		QueueDepth:      len(p.queue),
		ActiveJobs:      p.activeJobs.Load(),
		OutstandingJobs: len(p.slots),
		Capacity:        cap(p.slots),
		PendingEnqueues: p.pendingEnqueues.Load(),
		Accepting:       accepting,
	}
}

// Shutdown stops accepting new work and waits for accepted work to finish. If ctx expires first, queued jobs
// are abandoned, active attempts are cancelled, and a *ShutdownError describing the lost work is returned.
// Subsequent calls wait for the first shutdown and return its result.
func (p *Processor) Shutdown(ctx context.Context) error {
	first := false
	p.shutdownOnce.Do(func() {
		first = true
		p.shutdownErr = p.doShutdown(ctx)
		close(p.shutdownDone)
	})

	if !first {
		select {
		case <-p.shutdownDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return p.shutdownErr
}

func (p *Processor) doShutdown(ctx context.Context) error {
	defer p.obs.unregisterProcessorMetrics()

	p.mu.Lock()
	prev := p.state
	p.state = stateStopping
	close(p.stopCh)
	p.mu.Unlock()

	if prev == stateCreated {
		p.mu.Lock()
		p.state = stateStopped
		p.mu.Unlock()
		return nil
	}

	p.obs.logger.Info("processor shutting down",
		"queued", len(p.queue),
		"active", p.activeJobs.Load(),
		"pending_enqueues", p.pendingEnqueues.Load(),
	)

	var cause error

	// Phase 1: wait for in-flight HTTP enqueues. Blocked waiters observe stopCh
	// and bail immediately, so this only covers handlers finishing their sends.
	enqueuesDone := make(chan struct{})
	go func() {
		p.enqueueWG.Wait()
		close(enqueuesDone)
	}()
	var pending int64
	select {
	case <-enqueuesDone:
	case <-ctx.Done():
		cause = ctx.Err()
		pending = p.pendingEnqueues.Load()
		p.beginAbandon()
		// Remaining enqueues complete quickly: slot sends are non-blocking
		// and waiters have already been released via stopCh.
		<-enqueuesDone
	}

	// close the queue channel - any currently active workers will finish and any new sends will panic.
	close(p.queue)

	workersDone := make(chan struct{})
	go func() {
		p.workersWG.Wait()
		close(workersDone)
	}()

	var queued int
	var active int64
	if cause == nil {
		select {
		case <-workersDone:
		case <-ctx.Done():
			cause = ctx.Err()
			queued = len(p.queue)
			active = p.activeJobs.Load()
			p.beginAbandon()
			<-workersDone
		}
	} else {
		queued = len(p.queue)
		active = p.activeJobs.Load()
		<-workersDone
	}

	p.cancelAll()
	p.mu.Lock()
	p.state = stateStopped
	p.mu.Unlock()

	if cause != nil {
		err := &ShutdownError{
			Queued:          queued,
			Active:          active,
			PendingEnqueues: pending,
			Cause:           cause,
		}
		p.obs.logger.Warn("processor shutdown interrupted", "error", err)
		return err
	}

	p.obs.logger.Info("processor shutdown complete")
	return nil
}

// beginAbandon flips workers into abandon mode and cancels active attempts.
func (p *Processor) beginAbandon() {
	p.abandoning.Store(true)
	p.cancelAll()
}

func (p *Processor) newJob(payload []byte, sc trace.SpanContext, bag baggage.Baggage) *job {
	return &job{
		id:          newJobID(),
		payload:     payload,
		enqueuedAt:  time.Now(),
		spanContext: sc,
		baggage:     bag,
	}
}

func (p *Processor) worker() {
	defer p.workersWG.Done()
	for j := range p.queue {
		if p.abandoning.Load() {
			p.abandon(j)
		} else {
			p.execute(j)
		}
		<-p.slots
	}
}

// abandon records a job that was dropped because shutdown ran out of time.
func (p *Processor) abandon(j *job) {
	ctx := context.Background()
	p.obs.finalFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "abandoned")))
	p.obs.logger.Warn("job abandoned during shutdown", "job_id", j.id, "error", errAbandoned)
}

// execute runs the job in the current worker slot, handling retries and backoff.
func (p *Processor) execute(j *job) {
	p.activeJobs.Add(1)
	defer p.activeJobs.Add(-1)

	ctx := p.baseCtx
	if j.baggage.Len() > 0 {
		ctx = baggage.ContextWithBaggage(ctx, j.baggage)
	}
	if j.spanContext.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, j.spanContext)
	}
	ctx, span := p.obs.tracer.Start(ctx, "qless.task.execute",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("qless.job.id", j.id),
			attribute.Int("qless.payload.bytes", len(j.payload)),
		),
	)
	defer span.End()

	maxAttempts := p.cfg.MaxRetries + 1
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = p.attempt(ctx, j, attempt)
		if err == nil {
			span.SetAttributes(attribute.Int("qless.attempts", attempt))
			span.SetStatus(codes.Ok, "")
			return
		}

		if IsPermanent(err) {
			p.finalFailure(ctx, span, j, err, "permanent", attempt)
			return
		}
		if attempt == maxAttempts {
			p.obs.exhausted.Add(ctx, 1)
			p.finalFailure(ctx, span, j, err, "exhausted", attempt)
			return
		}

		p.obs.retries.Add(ctx, 1)
		p.obs.logger.Warn("job attempt failed, retrying",
			"job_id", j.id, "attempt", attempt, "max_attempts", maxAttempts, "error", err)
		if !p.sleepBackoff(attempt) {
			// Shutdown deadline hit mid-backoff: the retry is abandoned.
			p.finalFailure(ctx, span, j, err, "shutdown", attempt)
			return
		}
	}
}

// sleepBackoff blocks the worker for BaseBackoff*2^(attempt-1). It returns
// false if the processor was hard-cancelled while waiting.
func (p *Processor) sleepBackoff(attempt int) bool {
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	backoff := p.cfg.BaseBackoff << shift

	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-p.baseCtx.Done():
		return false
	}
}

func (p *Processor) attempt(ctx context.Context, j *job, attempt int) error {
	attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.ExecutionTimeout)
	defer cancel()

	start := time.Now()
	err := p.safeCall(attemptCtx, j.payload)
	elapsed := time.Since(start)

	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	attrs := metric.WithAttributes(attribute.String("outcome", outcome))
	p.obs.executions.Add(ctx, 1, attrs)
	p.obs.taskDuration.Record(ctx, elapsed.Seconds(), attrs)

	if err == nil {
		p.obs.logger.Info("job succeeded",
			"job_id", j.id, "attempt", attempt, "duration", elapsed)
	}
	return err
}

// safeCall invokes the user handler, converting panics into retryable errors.
func (p *Processor) safeCall(ctx context.Context, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qless: handler panicked: %v", r)
			p.obs.logger.Error("handler panic recovered", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	return p.handler(ctx, payload)
}

func (p *Processor) finalFailure(ctx context.Context, span trace.Span, j *job, err error, reason string, attempt int) {
	p.obs.finalFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	span.RecordError(err)
	span.SetAttributes(
		attribute.Int("qless.attempts", attempt),
		attribute.String("qless.failure.reason", reason),
	)
	span.SetStatus(codes.Error, reason)
	p.obs.logger.Error("job failed permanently",
		"job_id", j.id, "reason", reason, "attempts", attempt, "error", err)
}
