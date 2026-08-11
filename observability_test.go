package qless

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestStatsMetricsMatchSnapshot(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	p := startProcessor(t, Config{
		Workers:       1,
		QueueSize:     2,
		MeterProvider: provider,
	}, func(context.Context, []byte) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	fillProcessor(t, p, 1)
	<-started
	fillProcessor(t, p, 2)

	stats := p.Stats()
	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	values := make(map[string]int64)
	for _, scope := range resourceMetrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok || len(gauge.DataPoints) != 1 {
				continue
			}
			values[m.Name] = gauge.DataPoints[0].Value
		}
	}

	want := map[string]int64{
		"qless.queue.depth":         int64(stats.QueueDepth),
		"qless.jobs.active":         stats.ActiveJobs,
		"qless.jobs.outstanding":    int64(stats.OutstandingJobs),
		"qless.jobs.capacity":       int64(stats.Capacity),
		"qless.enqueues.pending":    stats.PendingEnqueues,
		"qless.processor.accepting": 1,
	}
	for name, expected := range want {
		if actual, ok := values[name]; !ok {
			t.Errorf("metric %q was not collected", name)
		} else if actual != expected {
			t.Errorf("metric %q = %d, Stats value = %d", name, actual, expected)
		}
	}

	close(release)
}
