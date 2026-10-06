package http1

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"velda-e2e-backend/internal/metrics"
)

// FailureHandler handles chaos and failure simulation endpoints.
type FailureHandler struct {
	metrics *metrics.Tracker
}

// NewFailureHandler creates a new FailureHandler.
func NewFailureHandler(tracker *metrics.Tracker) *FailureHandler {
	return &FailureHandler{metrics: tracker}
}

// HandleStatus returns an arbitrary HTTP status code requested via ?code=XXX.
func (h *FailureHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	code := http.StatusOK
	if c := r.URL.Query().Get("code"); c != "" {
		if v, err := strconv.Atoi(c); err == nil && v >= 100 && v <= 599 {
			code = v
		}
	}
	w.WriteHeader(code)
	if code != http.StatusNoContent && code != http.StatusNotModified {
		msg := fmt.Sprintf("Response with status code %d\n", code)
		_, _ = w.Write([]byte(msg))
		h.metrics.AddBytesOut(uint64(len(msg)))
	}
}

// HandleDelay simulates upstream processing latency (?ms=XXX).
func (h *FailureHandler) HandleDelay(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	ms := 100
	if q := r.URL.Query().Get("ms"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 {
			ms = v
		}
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
	w.WriteHeader(http.StatusOK)
	msg := fmt.Sprintf("Delayed %d ms\n", ms)
	_, _ = w.Write([]byte(msg))
	h.metrics.AddBytesOut(uint64(len(msg)))
}

// HandleHang hangs the request indefinitely until connection context cancels.
func (h *FailureHandler) HandleHang(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	<-r.Context().Done()
}

// HandleDrop immediately hijacks the connection and sends a TCP RST (Linger 0).
func (h *FailureHandler) HandleDrop(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking unsupported", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetLinger(0)
	}
	_ = conn.Close()
}

// HandleDropAfterHeaders sends response headers with Content-Length, then abruptly resets the TCP socket.
func (h *FailureHandler) HandleDropAfterHeaders(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking unsupported", http.StatusInternalServerError)
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Write response headers declaring 100KB body
	_, _ = bufrw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 102400\r\n\r\n")
	_ = bufrw.Flush()

	time.Sleep(5 * time.Millisecond)

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetLinger(0)
	}
	_ = conn.Close()
}

// HandleCorrupt hijacks the connection and writes malformed/corrupted HTTP bytes.
func (h *FailureHandler) HandleCorrupt(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking unsupported", http.StatusInternalServerError)
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Write invalid HTTP protocol line
	_, _ = bufrw.WriteString("HTTP/1.1 NOT_A_VALID_STATUS_LINE\r\n\r\nGARBAGE_PAYLOAD")
	_ = bufrw.Flush()
	_ = conn.Close()
}

// HandleDrip slowly drips chunked data (?count=N&interval_ms=M).
func (h *FailureHandler) HandleDrip(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Flushing unsupported", http.StatusInternalServerError)
		return
	}

	count := 5
	intervalMs := 100
	if c := r.URL.Query().Get("count"); c != "" {
		if v, err := strconv.Atoi(c); err == nil && v > 0 {
			count = v
		}
	}
	if d := r.URL.Query().Get("interval_ms"); d != "" {
		if v, err := strconv.Atoi(d); err == nil && v > 0 {
			intervalMs = v
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for i := 1; i <= count; i++ {
		select {
		case <-r.Context().Done():
			return
		default:
		}

		msg := fmt.Sprintf("data: drip chunk %d of %d\n\n", i, count)
		_, _ = w.Write([]byte(msg))
		flusher.Flush()
		h.metrics.AddBytesOut(uint64(len(msg)))
		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}
}

// HandleInfinite streams endless data until client cancels or disconnects.
func (h *FailureHandler) HandleInfinite(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Flushing unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	chunk := make([]byte, 4096)
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		n, err := w.Write(chunk)
		if err != nil {
			return
		}
		flusher.Flush()
		h.metrics.AddBytesOut(uint64(n))
		time.Sleep(10 * time.Millisecond)
	}
}

// HandleClose returns response with Connection: close header to test pool eviction.
func (h *FailureHandler) HandleClose(w http.ResponseWriter, r *http.Request) {
	h.metrics.IncRequests()
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Connection will close\n"))
}
