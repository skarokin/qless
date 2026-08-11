package qless

import (
	"errors"
	"fmt"
)

var (
	// ErrNotRunning indicates that the qless processor is not accepting new work.
	ErrNotRunning = errors.New("qless: processor is not running")
	// ErrAlreadyStarted indicates that Start was called more than once.
	ErrAlreadyStarted = errors.New("qless: processor has already been started")
	// ErrStopped indicates that Start was called after shutdown began.
	ErrStopped = errors.New("qless: processor has been stopped")

	// errAbandoned is returned when a job is abandoned because processor is shutting down.
	errAbandoned = errors.New("qless: job abandoned during shutdown")
)

type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return e.err.Error()
}

func (e permanentError) Unwrap() error {
	return e.err
}

// Permanent marks err as non-retryable. Passing nil returns nil.
func Permanent(err error) error {
	if err == nil || IsPermanent(err) {
		return err
	}

	return permanentError{err: err}
}

func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

// ShutdownError reports work that could not finish before Shutdown's context expired. The abandoned work is not persisted.
type ShutdownError struct {
	Queued          int
	Active          int64
	PendingEnqueues int64
	Cause           error
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf(
		"qless: shutdown interrupted with %d queued, %d active, and %d pending enqueues: %v",
		e.Queued,
		e.Active,
		e.PendingEnqueues,
		e.Cause,
	)
}

// Unwrap returns the cancellation or deadline error that interrupted graceful shutdown.
func (e *ShutdownError) Unwrap() error {
	return e.Cause
}
