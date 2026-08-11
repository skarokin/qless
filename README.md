# qless

Zero-infrastructure, config-driven HTTP async processing for Go — built for serverless and containerized environments.

`qless` bridges the gap between two options for async processing:
1. **Heavyweight:** provisioning SQS, Kafka, or Redis for what is often a trivial amount of async work.
2. **Naive:** `go doWork()` inside an HTTP handler - unmanaged, unbound.

**Use `qless` when:** losing an occasional job is tolerable — cache warming, notifications, analytics events, non-critical webhooks, etc.

**Do NOT use qless when:** you need durability, exactly-once processing, or audit guarantees — payments, orders, etc.

`qless` provides out-of-the-box observability, retries, worker pool configuration, and job queueing for you.

## Install

```bash
go get github.com/skarokin/qless
```

## Quickstart

```go
processor, err := qless.New(qless.Config{
    QueueSize:              256,                                     // waiting jobs; retained payloads are bounded by QueueSize+Workers
    Workers:                8,                                       // per-instance number of workers processing jobs
    MaxRetries:             3,                                       // max number of retries for a job
    MaxPayloadBytes:        1024 * 1024,                             // max size of a job payload
    BaseBackoff:            100 * time.Millisecond,                  // base backoff for a job that fails
    ExecutionTimeout:       10 * time.Second,                        // cooperative timeout for each attempt
    Backpressure:           qless.BlockWithTimeout(3 * time.Second), // also qless.DropWith503()
    Logger:                 logger,
}, processJob)
if err != nil {
    logger.Error("invalid qless config", "error", err)
    os.Exit(1)
}

// Start the qless processor
if err := processor.Start(); err != nil {
    logger.Error("start qless processor", "error", err)
    os.Exit(1)
}

// ... start an HTTP server and attach the qless handler ( processor.HTTPHandler() ) to a POST endpoint ...
// ... gracefully handle shutdown by calling processor.Shutdown(ctx) ...
```

The payload is the raw request body as `[]byte`. Job metadata is available from the handler context for application log correlation:

```go
jobID, _ := qless.JobIDFromContext(ctx)
attempt, _ := qless.AttemptFromContext(ctx) // one-based
```

Execution timeouts and shutdown cancellation are cooperative. Handlers should stop promptly when `ctx.Done()` is closed. `Shutdown` still returns when its own context expires if a handler ignores cancellation, but that handler goroutine can continue until the function returns or the process is killed.

## Runtime status

Applications can expose a point-in-time processor snapshot from their own health or status endpoints:

```go
stats := processor.Stats()
```

`Stats` reports queued, active, outstanding, capacity, pending-enqueue, and accepting values. Use `OutstandingJobs > 0` rather than `QueueDepth > 0` for keep-alive decisions because the final job leaves the queue while it is still executing. The same values are exported from the configured OpenTelemetry meter as observable gauges under `qless.queue.*`, `qless.jobs.*`, `qless.enqueues.*`, and `qless.processor.*`.

See [`examples/main.go`](examples/main.go) for a complete program.
