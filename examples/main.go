// Example: A qless server running on a long-lived VM or container.
// Cloud Run must use instance-based billing so CPU remains available after the HTTP response.
//
// Demonstrates:
// - wrapping the qless handler in your own HTTP middleware
// - unmarshalling the JSON payload inside your worker function
// - starting the processor and coordinating graceful shutdown with the HTTP server
// - exposing application-owned root and health endpoints
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	qless "github.com/skarokin/qless"
)

// sendEmailJob is YOUR data shape. `qless` only moves raw bytes and you unmarshal at the edge of your own code.
type sendEmailJob struct {
	Email    string `json:"email"`
	Template string `json:"template"`
}

// processJob is your worker function - all business logic lives here.
// It is simply a function that takes a context and a payload and returns an error.
func processJob(ctx context.Context, payload []byte) error {
	var job sendEmailJob
	if err := json.Unmarshal(payload, &job); err != nil {
		// "Permanent" class errors are recorded as failures that don't retry.
		return qless.Permanent(fmt.Errorf("unmarshal job: %w", err))
	}
	if job.Email == "" || job.Template == "" {
		return qless.Permanent(errors.New("email and template are required"))
	}

	slog.InfoContext(ctx, "sending email", "email", job.Email, "template", job.Template)
	// ... do work (simulated here) ...
	select {
	case <-time.After(100 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// apiKeyMiddleware is an example of a custom middleware that can be used in qless.
// qless can accept any HTTP middleware and any amount of middleware can be applied.
func apiKeyMiddleware(apiKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if apiKey != "" && r.Header.Get("X-API-Key") != apiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		logger.Warn("API_KEY is empty; POST /enqueue is unauthenticated")
	}

	// Create a new qless processor with a given config and handler function
	processor, err := qless.New(qless.Config{
		QueueSize:        256,                                     // waiting jobs; retained payloads are bounded by QueueSize+Workers
		Workers:          8,                                       // per-instance number of workers processing jobs
		MaxRetries:       3,                                       // max number of retries for a job
		MaxPayloadBytes:  1024 * 1024,                             // max size of a job payload
		BaseBackoff:      100 * time.Millisecond,                  // base backoff for a job that fails
		ExecutionTimeout: 10 * time.Second,                        // cooperative timeout for each attempt
		Backpressure:     qless.BlockWithTimeout(3 * time.Second), // also qless.DropWith503()
		Logger:           logger,
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

	// You as the user are responsible for setting up the HTTP server and attaching the qless HTTP handler to any POST endpoint.
	// qless is just an HTTP handler like any other - you are free to attach any other endpoints you may need.
	mux := http.NewServeMux()
	mux.Handle("POST /enqueue", apiKeyMiddleware(apiKey, processor.HTTPHandler()))

	// ------------------------------------------------------------------------------------------------
	// The below are optional endpoints that are not strictly required for qlesss to function
	// ------------------------------------------------------------------------------------------------

	// A simple health endpoint
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	})

	// A readiness endpoint that reports whether this processor accepts new jobs using processor.Stats().Accepting
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !processor.Stats().Accepting {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, `{"status":"not_ready"}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"status":"ready"}`)
	})

	// A status endpoint that exposes the same point-in-time values as qless's observable gauges.
	// A keep-alive should use outstanding_jobs > 0 (not queue_depth > 0, because a final job can be active after leaving the queue).
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(processor.Stats())
	})

	// A root endpoint that lists the available endpoints
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"service":"qless","enqueue":"POST /enqueue","health":"GET /healthz","ready":"GET /readyz","status":"GET /status"}`)
	})

	// Spin up the HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("signal received, shutting down")

	// Gracefully shutdown the HTTP server
	httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	if err := server.Shutdown(httpShutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
	cancelHTTPShutdown()

	// You as the user are responsible for coordinating graceful shutdown of the qless processor.
	// qless will attempt to finish jobs in the queue before shutting down but will abandon any job
	// that could not be finished within the timeout (6s in this example)
	processorShutdownCtx, cancelProcessorShutdown := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancelProcessorShutdown()
	if err := processor.Shutdown(processorShutdownCtx); err != nil {
		logger.Error("qless processor shutdown incomplete, remaining jobs abandoned", "error", err)
	}
}
