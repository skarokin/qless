package qless

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"strings"
)

//go:embed dashboard.html
var dashboardHTML []byte

// DashboardHandler returns a self-contained live debug dashboard for this
// processor. It serves an HTML page that polls a "stats" sub-endpoint (JSON
// Stats snapshots) once per second, so it works under any mount point:
//
//	mux.Handle("GET /debug/qless/", processor.DashboardHandler())
//
// The dashboard reflects only this instance's in-memory state. Like
// net/http/pprof, it is a debug surface: mount it behind authentication or on
// an internal port, never on the public internet.
func (p *Processor) DashboardHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "qless: only GET is supported", http.StatusMethodNotAllowed)
			return
		}

		if strings.HasSuffix(r.URL.Path, "/stats") {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(p.Stats())
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(dashboardHTML)
	})
}
