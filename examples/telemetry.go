package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// setupTelemetry installs the OpenTelemetry SDK as the global meter and tracer
// provider. qless records against the globals by default (or accepts providers
// explicitly via Config.MeterProvider / Config.TracerProvider), so this is the
// only telemetry wiring the application needs.
//
// Everything is exported over OTLP/gRPC to whatever OTEL_EXPORTER_OTLP_ENDPOINT
// points at - typically an OpenTelemetry Collector, which then fans out to
// Prometheus, Datadog, Honeycomb, Jaeger, Grafana, etc. When the variable is
// unset this is a no-op: qless's instruments compile down to the API's no-op
// implementations and cost nearly nothing.
//
// The returned shutdown function flushes buffered metrics and spans; call it
// after the processor has drained so the final job telemetry is not lost.
func setupTelemetry(ctx context.Context, logger *slog.Logger) (shutdown func(context.Context) error, err error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		logger.Info("OTEL_EXPORTER_OTLP_ENDPOINT not set; metrics and traces are disabled")
		return func(context.Context) error { return nil }, nil
	}

	res := resource.NewSchemaless(
		attribute.String("service.name", "qless-example"),
	)

	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	otel.SetMeterProvider(meterProvider)

	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		_ = meterProvider.Shutdown(ctx)
		return nil, err
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)
	otel.SetTracerProvider(tracerProvider)

	logger.Info("opentelemetry SDK installed", "endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	return func(ctx context.Context) error {
		return errors.Join(
			tracerProvider.Shutdown(ctx),
			meterProvider.Shutdown(ctx),
		)
	}, nil
}
