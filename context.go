package qless

import "context"

type jobContextKey uint8

const (
	jobIDContextKey jobContextKey = iota
	attemptContextKey
)

// JobIDFromContext returns the qless job ID associated with a handler invocation. The ID remains the same across retries.
func JobIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(jobIDContextKey).(string)
	return id, ok
}

// AttemptFromContext returns the one-based execution attempt associated with a handler invocation.
func AttemptFromContext(ctx context.Context) (int, bool) {
	attempt, ok := ctx.Value(attemptContextKey).(int)
	return attempt, ok
}
