package qless

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// serveHTTP ingests one job per POST request. The raw request body becomes the
// job payload. Responses:
//
//	202 Accepted            job enqueued; body is {"id":"<job id>"}
//	400 Bad Request         body could not be read
//	405 Method Not Allowed  non-POST request
//	413 Content Too Large   body exceeds MaxPayloadBytes
//	503 Service Unavailable processor full (per backpressure policy) or not running
func (p *Processor) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "qless: only POST is supported", http.StatusMethodNotAllowed)
		return
	}

	// Continue the caller's trace: this span is the remote parent of the background execution span.
	extracted := p.obs.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := p.obs.tracer.Start(extracted, "qless.enqueue", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	p.obs.received.Add(ctx, 1)
	p.totals.received.Add(1)

	p.mu.RLock()
	if p.state != stateRunning {
		p.mu.RUnlock()
		p.rejectJob(ctx, "not_running")
		http.Error(w, ErrNotRunning.Error(), http.StatusServiceUnavailable)
		return
	}

	// Processor is running at this point, we can attempt to enqueue the job
	p.enqueueWG.Add(1)
	p.pendingEnqueues.Add(1)
	p.mu.RUnlock()

	defer func() {
		p.pendingEnqueues.Add(-1)
		p.enqueueWG.Done()
	}()

	if !p.acquireSlot(ctx, w) {
		return
	}

	slotOwnedByHTTPHandler := true
	defer func() {
		// defer freeing up the slot if it was acquired by the HTTP handler but never passed down to the worker
		// this cleans up in any failure paths before the worker is able to start executing the job
		if slotOwnedByHTTPHandler {
			<-p.slots
		}
	}()

	payload, err := p.readPayload(w, r)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			p.rejectJob(ctx, "payload_too_large")
			p.obs.logger.WarnContext(ctx, "enqueue payload rejected",
				"reason", "payload_too_large",
				"max_payload_bytes", p.cfg.MaxPayloadBytes,
			)
			http.Error(w, "qless: payload exceeds MaxPayloadBytes", http.StatusRequestEntityTooLarge)
		} else {
			select {
			case <-p.stopCh:
				p.rejectJob(ctx, "shutdown")
				http.Error(w, ErrNotRunning.Error(), http.StatusServiceUnavailable)
			default:
				p.rejectJob(ctx, "body_read_error")
				p.obs.logger.DebugContext(ctx, "failed to read enqueue payload", "error", err)
				http.Error(w, "qless: failed to read request body", http.StatusBadRequest)
			}
		}
		return
	}
	p.obs.payloadSize.Record(ctx, int64(len(payload)))

	j := p.newJob(payload, span.SpanContext(), baggage.FromContext(ctx))
	p.queue <- j
	// it is now the responsibility of the worker to release the slot when job is finished
	slotOwnedByHTTPHandler = false

	p.obs.enqueued.Add(ctx, 1)
	p.totals.enqueued.Add(1)
	span.SetAttributes(attribute.String("qless.job.id", j.id))
	p.obs.logger.Debug("job enqueued", "job_id", j.id, "payload_bytes", len(payload))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"id":"` + j.id + `"}`))
}

// rejectJob records a job turned away before enqueue for a non-backpressure
// reason. Backpressure outcomes are counted separately in qless.backpressure.events,
// so qless.jobs.received = enqueued + rejected + failed backpressure outcomes.
func (p *Processor) rejectJob(ctx context.Context, reason string) {
	p.obs.rejected.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	p.totals.rejected.Add(1)
}

// backpressureFailure records an enqueue that was turned away because the
// processor was full. The "waited" outcome is a success and is not counted here.
func (p *Processor) backpressureFailure(ctx context.Context, outcome string) {
	p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	p.totals.backpressure.Add(1)
}

// readPayload closes an in-progress request body when processor shutdown begins. This releases its payload slot
// and prevents a slow client from indefinitely delaying the queue close.
func (p *Processor) readPayload(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	done := make(chan struct{})
	go func() {
		select {
		case <-p.stopCh:
			_ = r.Body.Close()
		case <-r.Context().Done():
			_ = r.Body.Close()
		case <-done:
		}
	}()

	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, p.cfg.MaxPayloadBytes))
	close(done)
	return payload, err
}

// acquireSlot reserves a payload slot according to the backpressure policy.
// It writes the 503 response itself when no slot could be acquired.
func (p *Processor) acquireSlot(ctx context.Context, w http.ResponseWriter) bool {
	// occupying buffer space is how you acquire a slot. if buffer is not full the first case will succeed
	// otherwise, return true/false based on backpressure policy set by the config
	select {
	case p.slots <- struct{}{}:
		return true
	default:
	}

	if p.cfg.Backpressure.mode == backpressureRejectImmediately {
		p.backpressureFailure(ctx, "rejected")
		p.obs.logger.DebugContext(ctx, "enqueue rejected by backpressure",
			"outcome", "rejected",
			"outstanding_jobs", len(p.slots),
			"capacity", cap(p.slots),
		)
		http.Error(w, "qless: processor is full", http.StatusServiceUnavailable)
		return false
	}

	start := time.Now()
	timer := time.NewTimer(p.cfg.Backpressure.timeout)
	defer timer.Stop()

	select {
	case p.slots <- struct{}{}:
		waited := time.Since(start)
		p.obs.waitDuration.Record(ctx, waited.Seconds())
		p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "waited")))
		p.obs.logger.DebugContext(ctx, "enqueue acquired capacity after waiting", "wait_duration", waited)
		return true
	case <-timer.C:
		p.backpressureFailure(ctx, "timeout")
		p.obs.logger.WarnContext(ctx, "enqueue backpressure timed out",
			"wait_duration", time.Since(start),
			"outstanding_jobs", len(p.slots),
			"capacity", cap(p.slots),
		)
		http.Error(w, "qless: processor is full", http.StatusServiceUnavailable)
	case <-ctx.Done():
		p.backpressureFailure(ctx, "canceled")
		p.obs.logger.DebugContext(ctx, "enqueue canceled while waiting for capacity",
			"wait_duration", time.Since(start),
			"error", ctx.Err(),
		)
		// The client is gone; the response write is best-effort.
		http.Error(w, "qless: request canceled while waiting for queue space", http.StatusServiceUnavailable)
	case <-p.stopCh:
		p.backpressureFailure(ctx, "shutdown")
		p.obs.logger.DebugContext(ctx, "enqueue interrupted by processor shutdown",
			"wait_duration", time.Since(start),
		)
		http.Error(w, ErrNotRunning.Error(), http.StatusServiceUnavailable)
	}
	return false
}
