# qless

Zero-infrastructure, config-driven HTTP async processing for Go — built for serverless and containerized environments.

`qless` bridges the gap between two options for async processing:
1. **Heavyweight:** provisioning SQS, Kafka, or Redis for what is often a trivial amount of async work.
2. **Naive:** `go doWork()` inside an HTTP handler - unmanaged, unbound.

**Use `qless` when:** losing an occasional job is tolerable (at-most-once) - cache warming, notifications, non-critical webhooks, etc.

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
    Backpressure:           qless.BlockWithTimeout(3 * time.Second).MaxWaiters(32).RetryAfter(2 * time.Second), // also qless.DropWith503()
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

## Where to deploy

qless does its work **after** the HTTP response is sent, so the platform must satisfy two requirements:

1. **CPU stays available between requests** — the worker pool runs in the background after the 202.
2. **SIGTERM is delivered before the instance stops** — qless drains the queue during the grace window.

| Infrastructure | Fit | Notes |
| --- | --- | --- |
| VM / bare metal (EC2, GCE, Hetzner, homelab) | Good | Always-on, full control over shutdown. See [`deploy/vm`](deploy/vm) for a Docker Compose setup. |
| Kubernetes | Good | Configurable `terminationGracePeriodSeconds` (default 30s). Wire `/readyz` to a readiness probe — never a liveness probe. |
| ECS on Fargate | Good | Always-on tasks; `stopTimeout` up to 120s. No scale-to-zero, so budget for a minimal always-on task. |
| Azure Container Apps | Good | Scales to zero **and** grace period configurable up to 600s — the friendliest scale-to-zero target. |
| Cloud Run (instance-based billing) | Good, short jobs | Requires `cpu_idle = false`. Scale-to-zero works when jobs drain within the ~10s SIGTERM window; set `min_instance_count = 1` for long jobs or deep queues. See [`deploy/cloudrun`](deploy/cloudrun). |
| Fly.io Machines / Knative | Good, short jobs | Scale-to-zero with configurable grace (`kill_timeout` up to 300s on Fly). Same drain-window caveat as Cloud Run. |
| Cloud Run (request-based billing, the default) | Bad | CPU is throttled to near zero after the response; queued jobs stall mid-drain. |
| AWS Lambda / Cloud Functions / Azure Functions | Bad | Execution freezes the moment the response returns; "after the response" does not exist. Use the platform's native async invocation instead. |

Any platform that bills or throttles per-request will silently stall your queue, because from its perspective an instance holding 200 queued jobs is *idle*.

## Runtime status

Applications can expose a point-in-time processor snapshot from their own health or status endpoints:

```go
stats := processor.Stats()
```

`Stats` reports queued, active, outstanding, capacity, pending-enqueue, waiting-enqueue, max-waiters, worker, and accepting values, plus cumulative `Totals` (received, enqueued, rejected, backpressure, succeeded, retries, final failures) that mirror the OpenTelemetry counters. Use `OutstandingJobs > 0` rather than `QueueDepth > 0` for keep-alive decisions because the final job leaves the queue while it is still executing. Every point-in-time `Stats` field is also exported as an OpenTelemetry observable gauge (see below).

A zero-dependency HTML page that polls the same `Stats` snapshot once per second and charts the last five minutes ships with the library:

```go
mux.Handle("GET /debug/qless/", processor.DashboardHandler())
```

Like `net/http/pprof`, it exposes internal state: mount it behind authentication or on an internal port, never on the public internet. It shows only the current instance's in-memory state.

## Observability

qless logs through `slog` and instruments through the OpenTelemetry **API** — the API is the library's only dependency, and its instruments are no-ops until the application installs the OTel SDK. Install the SDK once in `main()` (set the global providers, or pass providers via `Config`) and every metric and span below flows to your OTLP endpoint, collector, or Prometheus scrape. See [`examples/telemetry.go`](examples/telemetry.go) for a complete OTLP setup.

Counters:

| Metric | Attributes | Meaning |
| --- | --- | --- |
| `qless.jobs.received` | | Jobs received by the HTTP handler |
| `qless.jobs.enqueued` | | Jobs accepted into the queue |
| `qless.jobs.rejected` | `reason`: `payload_too_large`, `body_read_error`, `not_running`, `shutdown` | Jobs turned away before enqueue (excluding backpressure) |
| `qless.backpressure.events` | `outcome`: `rejected`, `waited`, `timeout`, `overflow`, `canceled`, `shutdown` | Capacity-wait outcomes |
| `qless.jobs.executions` | `outcome`: `success`, `failure` | Execution attempts |
| `qless.jobs.retries` | | Retries scheduled after failed attempts |
| `qless.jobs.final_failures` | `reason`: `permanent`, `exhausted`, `abandoned`, `shutdown` | Jobs that ended without succeeding |

`received = enqueued + rejected + failed backpressure outcomes`, and every accepted job ends in exactly one success (`executions{outcome="success"}`) or one `final_failures` increment.

Histograms: `qless.job.duration` (per attempt), `qless.job.queue.duration` (accept → worker pickup), `qless.enqueue.wait.duration` (backpressure wait), `qless.job.payload.size`.

Gauges (mirror `Stats` exactly): `qless.queue.depth`, `qless.jobs.active`, `qless.jobs.outstanding`, `qless.jobs.capacity`, `qless.enqueues.pending`, `qless.enqueues.waiting`, `qless.enqueues.waiters.configured`, `qless.workers.configured`, `qless.processor.accepting`. Pool utilization is `jobs.active / workers.configured`; saturation is `jobs.outstanding / jobs.capacity`; waiter utilization is `enqueues.waiting / enqueues.waiters.configured` when the waiter cap is positive.

Traces: the enqueue handler continues the caller's W3C trace context, and the background execution span uses the enqueue span as its remote parent — callers that propagate `traceparent` get one distributed trace across the async boundary.

See [`examples/main.go`](examples/main.go) for a complete program. The example lives in its own Go module so the OTel SDK and OTLP exporters it demonstrates never enter the library's dependency graph. Run it from the repo root with:

```bash
go -C examples run .
```
