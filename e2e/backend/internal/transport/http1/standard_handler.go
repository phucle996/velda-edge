package http1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"velda-e2e-backend/internal/metrics"
)

// StandardHandler handles legitimate HTTP/1.1 workflows.
type StandardHandler struct {
	metrics *metrics.Tracker
}

// NewStandardHandler creates a new StandardHandler.
func NewStandardHandler(tracker *metrics.Tracker) *StandardHandler {
	return &StandardHandler{metrics: tracker}
}

// HandleRoot returns a simple greetings message.
func (h *StandardHandler) HandleRoot(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	msg := []byte("Hello from Velda Edge E2E Upstream Backend!\n")
	_, _ = w.Write(msg)
	h.metrics.AddBytesOut(uint64(len(msg)))
}

// HandleHealth returns 200 OK.
func (h *StandardHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

// HandleEcho echoes back all received request headers and body.
func (h *StandardHandler) HandleEcho(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()

	// Copy custom headers back prefixed with X-Echo-
	for k, v := range r.Header {
		for _, val := range v {
			w.Header().Add("X-Echo-"+k, val)
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	n, _ := io.Copy(w, r.Body)
	h.metrics.AddBytesIn(uint64(n))
	h.metrics.AddBytesOut(uint64(n))
}

// HandleLarge streams large payloads with predictable byte pattern.
func (h *StandardHandler) HandleLarge(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	sizeKb := 64
	if s := r.URL.Query().Get("size_kb"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			sizeKb = v
		}
	}

	totalBytes := sizeKb * 1024
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(totalBytes))
	w.WriteHeader(http.StatusOK)

	chunk := make([]byte, 8192)
	for i := range chunk {
		chunk[i] = byte('A' + (i % 26))
	}

	sent := 0
	for sent < totalBytes {
		toWrite := len(chunk)
		if sent+toWrite > totalBytes {
			toWrite = totalBytes - sent
		}
		n, err := w.Write(chunk[:toWrite])
		if err != nil {
			break
		}
		sent += n
	}
	h.metrics.AddBytesOut(uint64(sent))
}

// HandleUpload reads the entire uploaded body and returns received bytes.
func (h *StandardHandler) HandleUpload(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	n, _ := io.Copy(io.Discard, r.Body)
	h.metrics.AddBytesIn(uint64(n))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := fmt.Sprintf(`{"bytes_received":%d}`+"\n", n)
	_, _ = w.Write([]byte(resp))
	h.metrics.AddBytesOut(uint64(len(resp)))
}

// HandleEmpty returns an empty response (200 with Content-Length 0, or 204 No Content).
func (h *StandardHandler) HandleEmpty(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	if r.URL.Query().Get("status") == "204" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// HandleStats returns JSON metrics snapshot.
func (h *StandardHandler) HandleStats(w http.ResponseWriter, r *http.Request) {
	snapshot := h.metrics.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(snapshot)
}
