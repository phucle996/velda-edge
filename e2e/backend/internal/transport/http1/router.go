package http1

import (
	"net/http"

	"velda-e2e-backend/internal/metrics"
)

// NewRouter constructs and configures the HTTP/1.1 ServeMux.
func NewRouter(tracker *metrics.Tracker) http.Handler {
	mux := http.NewServeMux()

	standard := NewStandardHandler(tracker)
	failure := NewFailureHandler(tracker)

	// Standard endpoints
	mux.HandleFunc("/", standard.HandleRoot)
	mux.HandleFunc("/health", standard.HandleHealth)
	mux.HandleFunc("/echo", standard.HandleEcho)
	mux.HandleFunc("/large", standard.HandleLarge)
	mux.HandleFunc("/upload", standard.HandleUpload)
	mux.HandleFunc("/empty", standard.HandleEmpty)
	mux.HandleFunc("/stats", standard.HandleStats)

	// Failure simulation & edge case endpoints
	mux.HandleFunc("/status", failure.HandleStatus)
	mux.HandleFunc("/delay", failure.HandleDelay)
	mux.HandleFunc("/hang", failure.HandleHang)
	mux.HandleFunc("/drop", failure.HandleDrop)
	mux.HandleFunc("/drop_after_headers", failure.HandleDropAfterHeaders)
	mux.HandleFunc("/corrupt", failure.HandleCorrupt)
	mux.HandleFunc("/chunked", failure.HandleDrip)
	mux.HandleFunc("/drip", failure.HandleDrip)
	mux.HandleFunc("/infinite", failure.HandleInfinite)
	mux.HandleFunc("/close", failure.HandleClose)

	return mux
}
