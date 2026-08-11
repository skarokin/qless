package qless

import (
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

type job struct {
	id          string
	payload     []byte
	spanContext trace.SpanContext
	baggage     baggage.Baggage
}
