// qless provides bounded, best-effort execution of background work submitted over HTTP.
//
// It is intended to be deployed on long-running, serverless containers for async work that needs
// to be executed in a separate process from the main application that does not require the complexity
// of a durable message broker like RabbitMQ or Kafka but still needs to be relatively reliable and monitorable.
//
// qless revolves around the application submitting a simple function that works on the HTTP request payload
// with configurable policies around concurrency, backpressure, retries, and other aspects of the job execution.
//
// Accepted risks include:
// 1. Tasks may be lost if the container is stopped or restarted.
// 2. Tasks may be lost if the worker pool & backpressure policy does not scale to the load.
// 3. There is no delivery guarantees - if the task is not executed, it is lost.
// 4. There is no coordination between replicas - tasks are not guaranteed to be idempotent, executed in order, etc.
//
// qless emits structured lifecycle logs and records OpenTelemetry traces and metrics by default. Applications
// remain responsible for configuring their OpenTelemetry SDK and exporters.
//
package qless
