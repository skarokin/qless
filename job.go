package qless

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

type job struct {
	id         string
	payload    []byte
	enqueuedAt time.Time

	// spanContext and baggage carry the trace context extracted from the
	// enqueue HTTP request into the background execution.
	spanContext trace.SpanContext
	baggage     baggage.Baggage
}

func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; fall back to a
		// timestamp so job IDs remain usable for log correlation.
		return "t-" + hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}
