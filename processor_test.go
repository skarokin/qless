package qless

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startProcessor creates and starts a processor, registering shutdown as cleanup.
func startProcessor(t *testing.T, cfg Config, handler Handler) *Processor {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = testLogger()
	}
	p, err := New(cfg, handler)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	})
	return p
}

func post(p *Processor, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/enqueue", strings.NewReader(body))
	rec := httptest.NewRecorder()
	p.HTTPHandler().ServeHTTP(rec, req)
	return rec
}

type blockingBody struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func newBlockingBody() *blockingBody {
	return &blockingBody{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (b *blockingBody) Read([]byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.closed
	return 0, errors.New("body closed")
}

func (b *blockingBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestEnqueueAndProcess(t *testing.T) {
	got := make(chan []byte, 1)
	p := startProcessor(t, Config{Workers: 2, QueueSize: 4}, func(_ context.Context, payload []byte) error {
		got <- payload
		return nil
	})

	rec := post(p, `{"hello":"world"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"id":"`) {
		t.Fatalf("body missing job id: %s", rec.Body)
	}

	select {
	case payload := <-got:
		if string(payload) != `{"hello":"world"}` {
			t.Fatalf("payload = %q", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job was never processed")
	}
}

func TestRetryThenSuccess(t *testing.T) {
	var attempts atomic.Int32
	done := make(chan struct{})
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, MaxRetries: 3, BaseBackoff: time.Millisecond,
	}, func(context.Context, []byte) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		close(done)
		return nil
	})

	if rec := post(p, "x"); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job never succeeded")
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
}

func TestHandlerContextContainsJobMetadata(t *testing.T) {
	type invocation struct {
		jobID   string
		attempt int
	}
	invocations := make(chan invocation, 2)
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, MaxRetries: 1, BaseBackoff: time.Millisecond,
	}, func(ctx context.Context, _ []byte) error {
		jobID, hasJobID := JobIDFromContext(ctx)
		attempt, hasAttempt := AttemptFromContext(ctx)
		if !hasJobID || !hasAttempt {
			return Permanent(errors.New("missing job metadata"))
		}
		invocations <- invocation{jobID: jobID, attempt: attempt}
		if attempt == 1 {
			return errors.New("retry me")
		}
		return nil
	})

	rec := post(p, "x")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	first := <-invocations
	second := <-invocations
	if first.jobID == "" || first.jobID != second.jobID {
		t.Fatalf("job IDs = %q and %q, want same non-empty ID", first.jobID, second.jobID)
	}
	if first.attempt != 1 || second.attempt != 2 {
		t.Fatalf("attempts = %d and %d, want 1 and 2", first.attempt, second.attempt)
	}
}

func TestPermanentErrorNotRetried(t *testing.T) {
	var attempts atomic.Int32
	seen := make(chan struct{}, 4)
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, MaxRetries: 5, BaseBackoff: time.Millisecond,
	}, func(context.Context, []byte) error {
		attempts.Add(1)
		seen <- struct{}{}
		return Permanent(errors.New("bad payload"))
	})

	post(p, "x")
	<-seen
	// Give the retry loop a chance to (incorrectly) run again.
	time.Sleep(100 * time.Millisecond)
	if n := attempts.Load(); n != 1 {
		t.Fatalf("attempts = %d, want 1 (permanent errors must not retry)", n)
	}
}

func TestRetriesExhausted(t *testing.T) {
	var attempts atomic.Int32
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, MaxRetries: 2, BaseBackoff: time.Millisecond,
	}, func(context.Context, []byte) error {
		attempts.Add(1)
		return errors.New("always fails")
	})

	post(p, "x")
	deadline := time.Now().Add(2 * time.Second)
	for attempts.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if n := attempts.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3 (1 initial + 2 retries)", n)
	}
}

func TestExecutionTimeout(t *testing.T) {
	timedOut := make(chan error, 1)
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, ExecutionTimeout: 50 * time.Millisecond,
	}, func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		timedOut <- ctx.Err()
		return ctx.Err()
	})

	post(p, "x")
	select {
	case err := <-timedOut:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ctx err = %v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("attempt was never cancelled by ExecutionTimeout")
	}
}

func TestHandlerPanicIsRecovered(t *testing.T) {
	var attempts atomic.Int32
	done := make(chan struct{})
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, MaxRetries: 1, BaseBackoff: time.Millisecond,
	}, func(context.Context, []byte) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		close(done)
		return nil
	})

	post(p, "x")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("panicking job was not retried")
	}
}

func TestPayloadTooLarge(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, QueueSize: 1, MaxPayloadBytes: 8}, func(context.Context, []byte) error {
		return nil
	})
	rec := post(p, strings.Repeat("a", 64))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestPayloadBodiesAreBoundedByCapacity(t *testing.T) {
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, Backpressure: DropWith503(),
	}, func(context.Context, []byte) error {
		return nil
	})

	bodies := []*blockingBody{newBlockingBody(), newBlockingBody()}
	results := make(chan int, len(bodies))
	for _, body := range bodies {
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/enqueue", body)
			rec := httptest.NewRecorder()
			p.HTTPHandler().ServeHTTP(rec, req)
			results <- rec.Code
		}()
		<-body.started
	}

	stats := p.Stats()
	if stats.OutstandingJobs != stats.Capacity || stats.PendingEnqueues != 2 {
		t.Fatalf("stats with bodies being read = %+v, want full capacity and two pending", stats)
	}

	overflow := newBlockingBody()
	req := httptest.NewRequest(http.MethodPost, "/enqueue", overflow)
	rec := httptest.NewRecorder()
	p.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("overflow status = %d, want 503", rec.Code)
	}
	select {
	case <-overflow.started:
		t.Fatal("overflow request body was read before capacity was acquired")
	default:
	}

	for _, body := range bodies {
		_ = body.Close()
	}
	for range bodies {
		if code := <-results; code != http.StatusBadRequest {
			t.Fatalf("closed body status = %d, want 400", code)
		}
	}
}

func TestShutdownClosesBodyBeingRead(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, QueueSize: 1}, func(context.Context, []byte) error {
		return nil
	})

	body := newBlockingBody()
	result := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/enqueue", body)
		rec := httptest.NewRecorder()
		p.HTTPHandler().ServeHTTP(rec, req)
		result <- rec.Code
	}()
	<-body.started

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case code := <-result:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", code)
		}
	case <-time.After(time.Second):
		t.Fatal("request body remained blocked after shutdown")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, QueueSize: 1}, func(context.Context, []byte) error { return nil })
	req := httptest.NewRequest(http.MethodGet, "/enqueue", nil)
	rec := httptest.NewRecorder()
	p.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

// fillProcessor enqueues Workers+QueueSize jobs that block until release is closed.
func fillProcessor(t *testing.T, p *Processor, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if rec := post(p, "fill"); rec.Code != http.StatusAccepted {
			t.Fatalf("fill job %d: status = %d", i, rec.Code)
		}
	}
}

func TestBackpressureDropWith503(t *testing.T) {
	release := make(chan struct{})
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, Backpressure: DropWith503(),
	}, func(context.Context, []byte) error {
		<-release
		return nil
	})
	defer close(release)

	fillProcessor(t, p, 2) // 1 executing + 1 queued = all payload slots held
	rec := post(p, "overflow")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestBackpressureBlockWithTimeoutEventuallyAccepts(t *testing.T) {
	release := make(chan struct{})
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, Backpressure: BlockWithTimeout(3 * time.Second),
	}, func(context.Context, []byte) error {
		<-release
		return nil
	})

	fillProcessor(t, p, 2)

	result := make(chan int, 1)
	go func() {
		result <- post(p, "waited").Code
	}()

	// The waiter must still be blocked, then get the slot once a job finishes.
	select {
	case code := <-result:
		t.Fatalf("request returned %d before space was available", code)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)

	select {
	case code := <-result:
		if code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue never completed")
	}
}

func TestBackpressureBlockWithTimeoutExpires(t *testing.T) {
	release := make(chan struct{})
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, Backpressure: BlockWithTimeout(50 * time.Millisecond),
	}, func(context.Context, []byte) error {
		<-release
		return nil
	})
	defer close(release)

	fillProcessor(t, p, 2)
	start := time.Now()
	rec := post(p, "overflow")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("rejected after %v, expected to wait ~50ms first", elapsed)
	}
}

func TestStatsIncludesPendingEnqueue(t *testing.T) {
	release := make(chan struct{})
	p := startProcessor(t, Config{
		Workers: 1, QueueSize: 1, Backpressure: BlockWithTimeout(3 * time.Second),
	}, func(context.Context, []byte) error {
		<-release
		return nil
	})

	fillProcessor(t, p, 2)
	result := make(chan int, 1)
	go func() {
		result <- post(p, "waiting").Code
	}()

	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().PendingEnqueues != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stats := p.Stats()
	if stats.PendingEnqueues != 1 || stats.OutstandingJobs != stats.Capacity {
		t.Fatalf("stats while enqueue waits = %+v, want one pending and full capacity", stats)
	}

	close(release)
	select {
	case code := <-result:
		if code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending enqueue never completed")
	}
}

func TestGracefulShutdownDrainsQueue(t *testing.T) {
	var processed atomic.Int32
	p := startProcessor(t, Config{Workers: 2, QueueSize: 32}, func(context.Context, []byte) error {
		time.Sleep(10 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	const jobs = 20
	fillProcessor(t, p, jobs)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := processed.Load(); n != jobs {
		t.Fatalf("processed = %d, want %d", n, jobs)
	}
}

func TestShutdownTimeoutAbandonsWork(t *testing.T) {
	started := make(chan struct{}, 1)
	p := startProcessor(t, Config{Workers: 1, QueueSize: 4}, func(ctx context.Context, _ []byte) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})

	fillProcessor(t, p, 3) // 1 active + 2 queued
	<-started              // ensure the worker picked up the first job

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := p.Shutdown(ctx)

	var shutdownErr *ShutdownError
	if !errors.As(err, &shutdownErr) {
		t.Fatalf("Shutdown err = %v, want *ShutdownError", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unwrap = %v, want DeadlineExceeded", errors.Unwrap(shutdownErr))
	}
	if shutdownErr.Queued != 2 || shutdownErr.Active != 1 {
		t.Fatalf("ShutdownError = %+v, want Queued=2 Active=1", shutdownErr)
	}
}

func TestShutdownDeadlineDoesNotWaitForNonCooperativeHandler(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := startProcessor(t, Config{Workers: 1, QueueSize: 1}, func(context.Context, []byte) error {
		close(started)
		<-release // deliberately ignores context cancellation
		return nil
	})

	post(p, "x")
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Shutdown(ctx)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Shutdown returned after %v, should honor its deadline", elapsed)
	}
	var shutdownErr *ShutdownError
	if !errors.As(err, &shutdownErr) || shutdownErr.Active != 1 {
		t.Fatalf("Shutdown error = %#v, want active *ShutdownError", err)
	}

	// Let background cleanup complete so the test does not leak a worker.
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().ActiveJobs != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentEnqueueAndShutdown(t *testing.T) {
	p := startProcessor(t, Config{
		Workers: 4, QueueSize: 16, Backpressure: BlockWithTimeout(50 * time.Millisecond),
	}, func(ctx context.Context, _ []byte) error {
		select {
		case <-time.After(time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	const requests = 200
	var requestsWG sync.WaitGroup
	requestsWG.Add(requests)
	statuses := make(chan int, requests)
	start := make(chan struct{})
	for range requests {
		go func() {
			defer requestsWG.Done()
			<-start
			statuses <- post(p, "x").Code
		}()
	}
	close(start)

	time.Sleep(2 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdownErr := p.Shutdown(ctx)
	requestsWG.Wait()
	close(statuses)

	if shutdownErr != nil {
		t.Fatalf("Shutdown: %v", shutdownErr)
	}
	for status := range statuses {
		if status != http.StatusAccepted && status != http.StatusServiceUnavailable {
			t.Fatalf("enqueue status = %d, want 202 or 503", status)
		}
	}
}

func TestLifecycleErrors(t *testing.T) {
	p, err := New(Config{Workers: 1, QueueSize: 1, Logger: testLogger()}, func(context.Context, []byte) error { return nil })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Enqueue before Start is rejected.
	if rec := post(p, "x"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("pre-start status = %d, want 503", rec.Code)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start after Shutdown = %v, want ErrStopped", err)
	}
	if rec := post(p, "x"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown status = %d, want 503", rec.Code)
	}
	// Second Shutdown returns the same (nil) result.
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestShutdownWithoutStart(t *testing.T) {
	p, err := New(Config{Workers: 1, QueueSize: 1, Logger: testLogger()}, func(context.Context, []byte) error { return nil })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown without Start: %v", err)
	}
}

func TestStats(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	p := startProcessor(t, Config{Workers: 1, QueueSize: 8}, func(context.Context, []byte) error {
		started <- struct{}{}
		<-release
		return nil
	})

	initial := p.Stats()
	if initial.QueueDepth != 0 || initial.ActiveJobs != 0 || initial.OutstandingJobs != 0 {
		t.Fatalf("initial activity = %+v, want no jobs", initial)
	}
	if initial.Capacity != 9 || initial.PendingEnqueues != 0 || initial.Workers != 1 || !initial.Accepting {
		t.Fatalf("initial stats = %+v, want capacity 9, workers 1, and accepting", initial)
	}

	// First job occupies the only worker; the next three wait in the queue.
	fillProcessor(t, p, 1)
	<-started
	fillProcessor(t, p, 3)
	stats := p.Stats()
	if stats.QueueDepth != 3 || stats.ActiveJobs != 1 || stats.OutstandingJobs != 4 {
		t.Fatalf("stats = %+v, want depth 3, active 1, outstanding 4", stats)
	}

	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().OutstandingJobs != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if stats := p.Stats(); stats.QueueDepth != 0 || stats.ActiveJobs != 0 || stats.OutstandingJobs != 0 {
		t.Fatalf("stats after drain = %+v, want no jobs", stats)
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if stats := p.Stats(); stats.Accepting {
		t.Fatalf("stats after shutdown = %+v, want accepting false", stats)
	}
}

func TestNewNilHandler(t *testing.T) {
	if _, err := New(Config{}, nil); err == nil {
		t.Fatal("New with nil handler should error")
	}
}
