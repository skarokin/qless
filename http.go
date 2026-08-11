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

	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, p.cfg.MaxPayloadBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "qless: payload exceeds MaxPayloadBytes", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "qless: failed to read request body", http.StatusBadRequest)
		}
		return
	}
	p.obs.payloadSize.Record(ctx, int64(len(payload)))

	// Register the enqueue under the read lock so Shutdown either sees it via
	// enqueueWG or this request observes the state change and is rejected.
	p.mu.RLock()
	if p.state != stateRunning {
		p.mu.RUnlock()
		http.Error(w, ErrNotRunning.Error(), http.StatusServiceUnavailable)
		return
	}
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

	j := p.newJob(payload, span.SpanContext(), baggage.FromContext(ctx))
	// Queue capacity equals slot capacity, so this send cannot block.
	p.queue <- j

	p.obs.enqueued.Add(ctx, 1)
	span.SetAttributes(attribute.String("qless.job.id", j.id))
	p.obs.logger.Debug("job enqueued", "job_id", j.id, "payload_bytes", len(payload))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"id":"` + j.id + `"}`))
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
		p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "rejected")))
		http.Error(w, "qless: processor is full", http.StatusServiceUnavailable)
		return false
	}

	start := time.Now()
	timer := time.NewTimer(p.cfg.Backpressure.timeout)
	defer timer.Stop()

	select {
	case p.slots <- struct{}{}:
		p.obs.waitDuration.Record(ctx, time.Since(start).Seconds())
		p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "waited")))
		return true
	case <-timer.C:
		p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "timeout")))
		http.Error(w, "qless: processor is full", http.StatusServiceUnavailable)
	case <-ctx.Done():
		p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "canceled")))
		// The client is gone; the response write is best-effort.
		http.Error(w, "qless: request canceled while waiting for queue space", http.StatusServiceUnavailable)
	case <-p.stopCh:
		p.obs.backpressure.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "shutdown")))
		http.Error(w, ErrNotRunning.Error(), http.StatusServiceUnavailable)
	}
	return false
}
