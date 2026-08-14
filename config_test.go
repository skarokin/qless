package qless

import (
	"runtime"
	"testing"
	"time"
)

func TestNormalizeConfigDefaults(t *testing.T) {
	ncfg, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig: %v", err)
	}
	if ncfg.QueueSize != defaultQueueSize {
		t.Errorf("QueueSize = %d, want %d", ncfg.QueueSize, defaultQueueSize)
	}
	if ncfg.Workers != runtime.GOMAXPROCS(0) {
		t.Errorf("Workers = %d, want GOMAXPROCS", ncfg.Workers)
	}
	if ncfg.MaxPayloadBytes != defaultMaxPayloadBytes {
		t.Errorf("MaxPayloadBytes = %d, want %d", ncfg.MaxPayloadBytes, defaultMaxPayloadBytes)
	}
	if ncfg.BaseBackoff != defaultBaseBackoff {
		t.Errorf("BaseBackoff = %v, want %v", ncfg.BaseBackoff, defaultBaseBackoff)
	}
	if ncfg.ExecutionTimeout != defaultExecutionTimeout {
		t.Errorf("ExecutionTimeout = %v, want %v", ncfg.ExecutionTimeout, defaultExecutionTimeout)
	}
	if ncfg.Logger == nil || ncfg.MeterProvider == nil || ncfg.TracerProvider == nil || ncfg.Propagator == nil {
		t.Error("observability defaults were not applied")
	}
	if ncfg.Backpressure.mode != backpressureRejectImmediately {
		t.Error("default backpressure should be immediate rejection")
	}
}

func TestNormalizeConfigBackpressureOptions(t *testing.T) {
	ncfg, err := normalizeConfig(Config{
		Backpressure: BlockWithTimeout(3 * time.Second).MaxWaiters(8).RetryAfter(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("normalizeConfig: %v", err)
	}
	if ncfg.Backpressure.timeout != 3*time.Second {
		t.Errorf("timeout = %v, want 3s", ncfg.Backpressure.timeout)
	}
	if ncfg.Backpressure.maxWaiters != 8 {
		t.Errorf("maxWaiters = %d, want 8", ncfg.Backpressure.maxWaiters)
	}
	if ncfg.Backpressure.retryAfter != 2*time.Second {
		t.Errorf("retryAfter = %v, want 2s", ncfg.Backpressure.retryAfter)
	}
}

func TestNormalizeConfigInvalid(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"negative queue size", Config{QueueSize: -1}},
		{"negative workers", Config{Workers: -1}},
		{"negative retries", Config{MaxRetries: -1}},
		{"negative payload bytes", Config{MaxPayloadBytes: -1}},
		{"negative base backoff", Config{BaseBackoff: -time.Second}},
		{"negative execution timeout", Config{ExecutionTimeout: -time.Second}},
		{"blocking backpressure without timeout", Config{Backpressure: BlockWithTimeout(0)}},
		{"blocking backpressure negative timeout", Config{Backpressure: BlockWithTimeout(-time.Second)}},
		{"negative max waiters", Config{Backpressure: BlockWithTimeout(time.Second).MaxWaiters(-1)}},
		{"max waiters with drop policy", Config{Backpressure: DropWith503().MaxWaiters(1)}},
		{"negative retry after", Config{Backpressure: DropWith503().RetryAfter(-time.Second)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeConfig(tc.cfg); err == nil {
				t.Fatalf("normalizeConfig(%+v) succeeded, want error", tc.cfg)
			}
		})
	}
}
