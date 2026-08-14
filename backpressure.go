package qless

import (
	"math"
	"time"
)

type backpressureMode uint8

const (
	backpressureRejectImmediately backpressureMode = iota
	backpressureBlockWithTimeout
)

// BackpressurePolicy controls what HTTPHandler does when the processor is already retaining its maximum number of payloads.
// Max number of payloads = QueueSize + Workers
type BackpressurePolicy struct {
	mode       backpressureMode
	timeout    time.Duration
	maxWaiters int
	retryAfter time.Duration
}

// BlockWithTimeout waits for payload space for at most timeout, then returns 503 if no queue space is available.
// Waiters (how many requests are blocked waiting for a slot) are unbounded unless MaxWaiters is set.
// Chain RetryAfter to advertise when clients should retry after receiving 503
func BlockWithTimeout(timeout time.Duration) BackpressurePolicy {
	return BackpressurePolicy{
		mode:    backpressureBlockWithTimeout,
		timeout: timeout,
	}
}

// DropWith503 immediately returns 503 if the processor is full.
func DropWith503() BackpressurePolicy {
	return BackpressurePolicy{
		mode: backpressureRejectImmediately,
	}
}

// MaxWaiters caps how many HTTP requests may block waiting for a payload slot.
// Further requests receive an immediate 503 (outcome "overflow") instead of joining the wait.
// 0, the default, means unbounded waiters. Only valid with BlockWithTimeout.
func (p BackpressurePolicy) MaxWaiters(n int) BackpressurePolicy {
	p.maxWaiters = n
	return p
}

// RetryAfter sets the Retry-After header on 503 responses caused by a full processor:
// immediate drop, waiter overflow, or wait timeout. 0 (the default) omits the header.
// The value is encoded as integer seconds, rounded up, per RFC 9110.
func (p BackpressurePolicy) RetryAfter(d time.Duration) BackpressurePolicy {
	p.retryAfter = d
	return p
}

// retryAfterSeconds converts d to the integer seconds used by the Retry-After header.
func retryAfterSeconds(d time.Duration) int {
	sec := int(math.Ceil(d.Seconds()))
	if sec < 1 {
		return 1
	}
	return sec
}
