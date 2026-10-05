package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	httpPort = flag.Int("http-port", 8081, "HTTP upstream port")
	tcpPort  = flag.Int("tcp-port", 19001, "Raw TCP echo upstream port")
)

type ServerMetrics struct {
	TotalRequests uint64
	TotalBytesIn  uint64
	TotalBytesOut uint64
	ActiveConns   int64
}

var metrics ServerMetrics

func main() {
	flag.Parse()

	log.Printf("[E2E Backend] Starting Velda Edge Test Upstream...")
	log.Printf("[E2E Backend] HTTP/1.1 & HTTP/2 listening on :%d", *httpPort)
	log.Printf("[E2E Backend] Raw TCP Echo listening on :%d", *tcpPort)

	// 1. Start Raw TCP Echo Server
	go startTCPEchoServer(*tcpPort)

	// 2. Start HTTP Upstream Server
	mux := http.NewServeMux()

	// Root / health
	mux.HandleFunc("/", handleRoot)
	mux.HandleFunc("/health", handleHealth)

	// Echo endpoint: echoes back headers & payload
	mux.HandleFunc("/echo", handleEcho)

	// Latency / Slow endpoint: simulates slow upstream
	mux.HandleFunc("/delay", handleDelay)

	// Chunked transfer / streaming endpoint
	mux.HandleFunc("/chunked", handleChunked)

	// Large payload download
	mux.HandleFunc("/large", handleLarge)

	// Upload endpoint (reads entire body and returns byte count)
	mux.HandleFunc("/upload", handleUpload)

	// Abrupt TCP drop: forces TCP reset to test gateway cleanup
	mux.HandleFunc("/drop", handleDrop)

	// Metrics endpoint
	mux.HandleFunc("/stats", handleStats)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *httpPort),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[E2E Backend] HTTP server error: %v", err)
		}
	}()

	// Wait for OS shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[E2E Backend] Shutting down upstream servers cleanly...")
	_ = server.Close()
	log.Println("[E2E Backend] Stopped.")
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Hello from Velda Edge E2E Upstream Backend!\n"))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

func handleEcho(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)

	// Copy custom headers back
	for k, v := range r.Header {
		for _, val := range v {
			w.Header().Add("X-Echo-"+k, val)
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	// Echo body back
	n, _ := io.Copy(w, r.Body)
	atomic.AddUint64(&metrics.TotalBytesIn, uint64(n))
	atomic.AddUint64(&metrics.TotalBytesOut, uint64(n))
}

func handleDelay(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
	ms := 100
	if q := r.URL.Query().Get("ms"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 {
			ms = v
		}
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Delayed %d ms\n", ms)
}

func handleChunked(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	count := 5
	intervalMs := 50
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
		msg := fmt.Sprintf("data: chunk %d of %d at %s\n\n", i, count, time.Now().Format(time.RFC3339Nano))
		_, _ = w.Write([]byte(msg))
		flusher.Flush()
		atomic.AddUint64(&metrics.TotalBytesOut, uint64(len(msg)))
		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}
}

func handleLarge(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
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

	// Send buffer chunks
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
	atomic.AddUint64(&metrics.TotalBytesOut, uint64(sent))
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
	n, _ := io.Copy(io.Discard, r.Body)
	atomic.AddUint64(&metrics.TotalBytesIn, uint64(n))

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Received %d bytes\n", n)
}

func handleDrop(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&metrics.TotalRequests, 1)
	// Hijack connection and close with TCP reset
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
		// Set linger to 0 so Close() sends RST instead of FIN
		_ = tcpConn.SetLinger(0)
	}
	_ = conn.Close()
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"total_requests":%d,"total_bytes_in":%d,"total_bytes_out":%d,"active_conns":%d}`+"\n",
		atomic.LoadUint64(&metrics.TotalRequests),
		atomic.LoadUint64(&metrics.TotalBytesIn),
		atomic.LoadUint64(&metrics.TotalBytesOut),
		atomic.LoadInt64(&metrics.ActiveConns),
	)
}

func startTCPEchoServer(port int) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("[E2E Backend] TCP echo server bind failed on :%d: %v", port, err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			atomic.AddInt64(&metrics.ActiveConns, 1)
			defer func() {
				_ = c.Close()
				atomic.AddInt64(&metrics.ActiveConns, -1)
			}()
			buf := make([]byte, 32768)
			for {
				n, err := c.Read(buf)
				if n > 0 {
					atomic.AddUint64(&metrics.TotalBytesIn, uint64(n))
					wn, werr := c.Write(buf[:n])
					if wn > 0 {
						atomic.AddUint64(&metrics.TotalBytesOut, uint64(wn))
					}
					if werr != nil {
						break
					}
				}
				if err != nil {
					break
				}
			}
		}(conn)
	}
}
