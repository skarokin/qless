package qless

type backpressureMode uint8

const (
	backpressureRejectImmediately backpressureMode = iota
	backpressureBlockWithTimeout
)

// BackpressurePolicy controls what HTTPHandler does when the processor is
// already retaining its maximum number of payloads.
type BackpressurePolicy struct {
	mode    backpressureMode
	timeout time.Duration
}

// BlockWithTimeout waits for payload space for at most timeout, then returns 503 if no queue space is available.
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
