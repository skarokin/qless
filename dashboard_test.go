package qless

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardHandler(t *testing.T) {
	p := startProcessor(t, Config{Workers: 2, QueueSize: 4}, func(context.Context, []byte) error {
		return nil
	})
	h := p.DashboardHandler()

	t.Run("serves page", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/qless/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("content type = %q, want text/html", ct)
		}
		if !strings.Contains(rec.Body.String(), "qless") {
			t.Fatal("page body does not mention qless")
		}
	})

	t.Run("serves stats", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/qless/stats", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var stats Stats
		if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
			t.Fatalf("unmarshal stats: %v", err)
		}
		if stats.Workers != 2 || stats.Capacity != 6 || !stats.Accepting {
			t.Fatalf("stats = %+v, want workers 2, capacity 6, accepting", stats)
		}
	})

	t.Run("rejects non-GET", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/debug/qless/", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rec.Code)
		}
	})
}
