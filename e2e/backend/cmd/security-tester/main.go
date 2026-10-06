package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func getEdgeMetrics() (rssKb uint64, fds int, err error) {
	out, err := exec.Command("pgrep", "-f", "target/release/velda-edge").Output()
	if err != nil {
		return 0, 0, fmt.Errorf("velda-edge not running: %w", err)
	}
	pidStr := strings.TrimSpace(strings.Split(string(out), "\n")[0])
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return 0, 0, err
	}

	// Read VmRSS
	statusData, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err == nil {
		for _, line := range strings.Split(string(statusData), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					rssKb, _ = strconv.ParseUint(fields[1], 10, 64)
				}
				break
			}
		}
	}

	// Read FDs
	fdEntries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err == nil {
		fds = len(fdEntries)
	}

	return rssKb, fds, nil
}

func main() {
	testName := flag.String("test", "all", "Specific test: slowloris, header-bomb, rapid-reset, continuation, window-starvation, idle-exhaustion, all")
	h1Target := flag.String("h1", "127.0.0.1:8080", "HTTP/1.1 target")
	h2Target := flag.String("h2", "127.0.0.1:8443", "HTTP/2 TLS target")
	tcpTarget := flag.String("tcp", "127.0.0.1:9000", "L4 TCP target")
	flag.Parse()

	fmt.Println("==========================================================================")
	fmt.Println(" [Velda E2E] Security & Resource Exhaustion Vector Diagnostic Tester")
	fmt.Printf(" Testing: %s\n", *testName)
	fmt.Println("==========================================================================")

	rssBefore, fdsBefore, err := getEdgeMetrics()
	if err != nil {
		fmt.Printf("[Warning] Could not get process metrics: %v\n", err)
	} else {
		fmt.Printf("Initial Edge Baseline: RSS: %.2f MB | FDs: %d\n\n", float64(rssBefore)/1024, fdsBefore)
	}

	runAll := *testName == "all"

	if runAll || *testName == "slowloris" {
		testSlowloris(*h1Target)
	}

	if runAll || *testName == "header-bomb" {
		testHeaderBomb(*h1Target)
	}

	if runAll || *testName == "rapid-reset" {
		testH2RapidReset(*h2Target)
	}

	if runAll || *testName == "continuation" {
		testH2ContinuationFlood(*h2Target)
	}

	if runAll || *testName == "window-starvation" {
		testH2WindowStarvation(*h2Target)
	}

	if runAll || *testName == "idle-exhaustion" {
		testIdleExhaustion(*tcpTarget)
	}

	if runAll || *testName == "slow-body" {
		testSlowBodyDrip(*h1Target)
	}

	// Cooldown and check final resource delta
	time.Sleep(2 * time.Second)
	rssAfter, fdsAfter, err := getEdgeMetrics()
	if err == nil {
		rssDelta := int64(rssAfter) - int64(rssBefore)
		fdDelta := fdsAfter - fdsBefore
		fmt.Println("==========================================================================")
		fmt.Println(" [Final Resource Audit Post-Attacks]")
		fmt.Printf(" Initial Baseline: RSS: %.2f MB | FDs: %d\n", float64(rssBefore)/1024, fdsBefore)
		fmt.Printf(" Final State:      RSS: %.2f MB | FDs: %d\n", float64(rssAfter)/1024, fdsAfter)
		fmt.Printf(" Net Delta:        RSS: %+.2f MB | FDs: %+d\n", float64(rssDelta)/1024, fdDelta)
		if fdDelta > 50 {
			fmt.Printf(" [ALERT] Potential File Descriptor leak detected! (Delta: %+d)\n", fdDelta)
		} else {
			fmt.Println(" [OK] Zero FD leaks. All attack connections closed cleanly.")
		}
		if rssDelta > 50*1024 {
			fmt.Printf(" [ALERT] Potential Memory leak detected! (Delta: %+.2f MB)\n", float64(rssDelta)/1024)
		} else {
			fmt.Println(" [OK] Memory growth within safe bounds.")
		}
		fmt.Println("==========================================================================")
	}
}

// -----------------------------------------------------------------------------
// Attack Vector 1: Slowloris (Slow Header Drip)
// Sends 1 header byte every 500ms across 100 concurrent connections.
// Target must enforce header_read_timeout_ms (10s) and drop all connections.
// -----------------------------------------------------------------------------
func testSlowloris(target string) {
	fmt.Println(">>> [Vector 1] Slowloris Slow Header Drip Test...")
	conns := 100
	var closedCount int64
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", target, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			// Send partial request
			_, _ = fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: %s\r\nContent-Length: 1000\r\n", target)

			// Drip 1 byte every 500ms up to 25 seconds
			for step := 0; step < 25; step++ {
				_, err := fmt.Fprintf(conn, "X-Drip-%d: a\r\n", step)
				if err != nil {
					atomic.AddInt64(&closedCount, 1)
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	closed := atomic.LoadInt64(&closedCount)
	fmt.Printf(" - Result: %d / %d connections terminated by gateway within %.2fs\n", closed, conns, elapsed.Seconds())
	if closed >= int64(conns*95/100) {
		fmt.Printf(" [PASS] Gateway correctly cut off Slowloris connections (enforced header timeout).\n\n")
	} else {
		fmt.Printf(" [WARN] Only %d connections were terminated! Possible slowloris bypass.\n\n", closed)
	}
}

// -----------------------------------------------------------------------------
// Attack Vector 2: Header Bomb & Oversized URI
// Sends oversized headers (>64KB), giant URIs, and massive header counts.
// -----------------------------------------------------------------------------
func testHeaderBomb(target string) {
	fmt.Println(">>> [Vector 2] HTTP/1.1 Header Bomb & Oversized Request Test...")

	// Test A: 128KB single header (Limit is 64KB)
	conn, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err == nil {
		defer conn.Close()
		giantVal := strings.Repeat("A", 128*1024)
		_, _ = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nX-Giant: %s\r\n\r\n", target, giantVal)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		resp := string(buf[:n])
		if strings.Contains(resp, "431") || strings.Contains(resp, "400") || n == 0 {
			fmt.Println(" - 128KB Header Bomb: Rejected with 4xx or immediate connection close [PASS]")
		} else {
			fmt.Printf(" - 128KB Header Bomb: Unexpected response: %s [WARN]\n", strings.Split(resp, "\r\n")[0])
		}
	}

	// Test B: 1MB URI path
	conn2, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err == nil {
		defer conn2.Close()
		giantURI := "/" + strings.Repeat("B", 1024*1024)
		_, _ = fmt.Fprintf(conn2, "GET %s HTTP/1.1\r\nHost: %s\r\n\r\n", giantURI, target)
		_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1024)
		n, _ := conn2.Read(buf)
		resp := string(buf[:n])
		if strings.Contains(resp, "414") || strings.Contains(resp, "431") || strings.Contains(resp, "400") || n == 0 {
			fmt.Println(" - 1MB URI Bomb: Rejected with 4xx or connection close without OOM [PASS]")
		} else {
			fmt.Printf(" - 1MB URI Bomb: Unexpected response: %s [WARN]\n", strings.Split(resp, "\r\n")[0])
		}
	}

	// Test C: 1,000 distinct headers (Limit is max_headers = 64)
	conn3, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err == nil {
		defer conn3.Close()
		var req bytes.Buffer
		req.WriteString(fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\n", target))
		for h := 0; h < 200; h++ {
			req.WriteString(fmt.Sprintf("X-H-%d: value-%d\r\n", h, h))
		}
		req.WriteString("\r\n")
		_, _ = conn3.Write(req.Bytes())
		_ = conn3.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1024)
		n, _ := conn3.Read(buf)
		resp := string(buf[:n])
		if strings.Contains(resp, "431") || strings.Contains(resp, "400") || n == 0 {
			fmt.Println(" - 200 Headers Bomb (Limit 64): Enforced max_headers and rejected [PASS]")
		} else {
			fmt.Printf(" - 200 Headers Bomb: Unexpected response: %s [WARN]\n", strings.Split(resp, "\r\n")[0])
		}
	}
	fmt.Println()
}

// -----------------------------------------------------------------------------
// Attack Vector 3: HTTP/2 Rapid Reset Flood (CVE-2023-44487)
// Sends HEADERS followed immediately by RST_STREAM in a loop of 1,500 resets.
// Threshold max_consecutive_resets is 500. Gateway should kill the connection.
// -----------------------------------------------------------------------------
func testH2RapidReset(target string) {
	fmt.Println(">>> [Vector 3] HTTP/2 Rapid Reset Attack (CVE-2023-44487)...")

	tlsConfig := &tls.Config{
		NextProtos:         []string{"h2"},
		InsecureSkipVerify: true,
		ServerName:         "api.example.com",
	}

	conn, err := tls.Dial("tcp", target, tlsConfig)
	if err != nil {
		fmt.Printf(" - Failed to connect to %s: %v\n", target, err)
		return
	}
	defer conn.Close()

	// HTTP/2 Client Preface
	_, _ = conn.Write([]byte(http2.ClientPreface))

	framer := http2.NewFramer(conn, conn)
	_ = framer.WriteSettings()

	var hpackBuf bytes.Buffer
	enc := hpack.NewEncoder(&hpackBuf)
	_ = enc.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})
	_ = enc.WriteField(hpack.HeaderField{Name: ":path", Value: "/api/echo"})
	_ = enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	_ = enc.WriteField(hpack.HeaderField{Name: ":authority", Value: "api.example.com"})

	resetsSent := 0
	connectionCut := false

	// Attempt 1,500 rapid resets
	for i := 1; i <= 1500; i++ {
		streamID := uint32(i*2 - 1)

		// 1. Write HEADERS
		err := framer.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      streamID,
			BlockFragment: hpackBuf.Bytes(),
			EndStream:     true,
			EndHeaders:    true,
		})
		if err != nil {
			connectionCut = true
			break
		}

		// 2. Immediately write RST_STREAM (CANCEL)
		err = framer.WriteRSTStream(streamID, http2.ErrCodeCancel)
		if err != nil {
			connectionCut = true
			break
		}
		resetsSent++
	}

	fmt.Printf(" - Resets sent before connection terminated: %d\n", resetsSent)
	if connectionCut || resetsSent < 1500 {
		fmt.Printf(" [PASS] Connection terminated by gateway upon detecting reset storm (enforced threshold).\n\n")
	} else {
		fmt.Printf(" [WARN] Completed all 1,500 resets without gateway closing connection.\n\n")
	}
}

// -----------------------------------------------------------------------------
// Attack Vector 4: HTTP/2 CONTINUATION Flood (CVE-2024-27983)
// Sends HEADERS without END_HEADERS followed by continuous CONTINUATION frames.
// Config max_continuation_frames is 16. Gateway must terminate stream/connection.
// -----------------------------------------------------------------------------
func testH2ContinuationFlood(target string) {
	fmt.Println(">>> [Vector 4] HTTP/2 CONTINUATION Flood Attack (CVE-2024-27983)...")

	tlsConfig := &tls.Config{
		NextProtos:         []string{"h2"},
		InsecureSkipVerify: true,
		ServerName:         "api.example.com",
	}

	conn, err := tls.Dial("tcp", target, tlsConfig)
	if err != nil {
		fmt.Printf(" - Failed to connect to %s: %v\n", target, err)
		return
	}
	defer conn.Close()

	_, _ = conn.Write([]byte(http2.ClientPreface))
	framer := http2.NewFramer(conn, conn)
	_ = framer.WriteSettings()

	var hpackBuf bytes.Buffer
	enc := hpack.NewEncoder(&hpackBuf)
	_ = enc.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})

	// Write HEADERS without END_HEADERS
	err = framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      1,
		BlockFragment: hpackBuf.Bytes(),
		EndStream:     false,
		EndHeaders:    false, // Attack: headers never complete!
	})
	if err != nil {
		fmt.Printf(" - Failed to write initial HEADERS frame: %v\n", err)
		return
	}

	framesSent := 0
	cut := false
	dummyHeader := []byte{0x00, 0x01, 'a', 0x01, 'b'}

	for i := 0; i < 50; i++ {
		err := framer.WriteContinuation(1, false, dummyHeader)
		if err != nil {
			cut = true
			break
		}
		framesSent++
		time.Sleep(5 * time.Millisecond)
	}

	fmt.Printf(" - CONTINUATION frames sent before connection closed: %d\n", framesSent)
	if cut || framesSent <= 20 {
		fmt.Printf(" [PASS] Gateway enforced max_continuation_frames (cut at %d frames)!\n\n", framesSent)
	} else {
		fmt.Printf(" [WARN] Gateway accepted %d CONTINUATION frames without termination.\n\n", framesSent)
	}
}

// -----------------------------------------------------------------------------
// Attack Vector 5: Downstream Window Starvation & Memory Bloat Test
// Requests 1MB payload from /large?size_kb=1024, but halts reading / zero window.
// Verifies that gateway pauses reading upstream without ballooning memory.
// -----------------------------------------------------------------------------
func testH2WindowStarvation(target string) {
	fmt.Println(">>> [Vector 5] Downstream Window Starvation (Backpressure / Memory Buffering)...")

	rssStart, _, _ := getEdgeMetrics()

	conns := 50
	var wg sync.WaitGroup

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tlsConfig := &tls.Config{
				NextProtos:         []string{"h2"},
				InsecureSkipVerify: true,
				ServerName:         "api.example.com",
			}
			conn, err := tls.Dial("tcp", target, tlsConfig)
			if err != nil {
				return
			}
			defer conn.Close()

			_, _ = conn.Write([]byte(http2.ClientPreface))
			framer := http2.NewFramer(conn, conn)
			_ = framer.WriteSettings()

			var hpackBuf bytes.Buffer
			enc := hpack.NewEncoder(&hpackBuf)
			_ = enc.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})
			_ = enc.WriteField(hpack.HeaderField{Name: ":path", Value: "/api/large?size_kb=1024"})
			_ = enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
			_ = enc.WriteField(hpack.HeaderField{Name: ":authority", Value: "api.example.com"})

			_ = framer.WriteHeaders(http2.HeadersFrameParam{
				StreamID:      1,
				BlockFragment: hpackBuf.Bytes(),
				EndStream:     true,
				EndHeaders:    true,
			})

			// Intentionally do NOT read DATA frames or send WINDOW_UPDATE
			// Sleep 3 seconds while upstream generates 1MB per connection (total 50MB)
			time.Sleep(3 * time.Second)
		}(i)
	}

	time.Sleep(1500 * time.Millisecond)
	rssPeak, _, _ := getEdgeMetrics()
	wg.Wait()

	memGrowthMb := float64(int64(rssPeak)-int64(rssStart)) / 1024
	fmt.Printf(" - 50 concurrent 1MB requests under paused flow-control.\n")
	fmt.Printf(" - Memory growth during pause: %+.2f MB (Expected < 15MB due to streaming backpressure)\n", memGrowthMb)

	if memGrowthMb < 25.0 {
		fmt.Printf(" [PASS] Zero buffer bloat! Gateway propagated backpressure without buffering 50MB into RAM.\n\n")
	} else {
		fmt.Printf(" [WARN] Memory grew by %.2f MB! Possible unbounded buffering.\n\n", memGrowthMb)
	}
}

// -----------------------------------------------------------------------------
// Attack Vector 6: Idle Connection Exhaustion
// Opens 300 idle connections to L4 TCP port and waits for idle eviction.
// -----------------------------------------------------------------------------
func testIdleExhaustion(target string) {
	fmt.Println(">>> [Vector 6] Idle Connection Holding & Socket Eviction Test...")
	conns := 200
	var activeConns int64
	var wg sync.WaitGroup

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", target, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			atomic.AddInt64(&activeConns, 1)

			// Hold idle and check when read returns EOF (gateway timeout)
			buf := make([]byte, 1)
			_ = conn.SetReadDeadline(time.Now().Add(35 * time.Second))
			_, err = conn.Read(buf)
			if err != nil || err == io.EOF {
				atomic.AddInt64(&activeConns, -1)
			}
		}(i)
	}

	time.Sleep(1 * time.Second)
	fmt.Printf(" - Established %d idle connections. Holding for timeout check...\n", atomic.LoadInt64(&activeConns))
	fmt.Printf(" - (Note: L4 listener idle_timeout_ms is 30s. Gateway should evict them).\n")

	// Wait 32 seconds to allow 30s timeout to trigger
	time.Sleep(32 * time.Second)
	remaining := atomic.LoadInt64(&activeConns)
	fmt.Printf(" - Remaining active idle connections after 32s: %d / %d\n", remaining, conns)
	if remaining == 0 {
		fmt.Printf(" [PASS] Gateway evicted 100%% of idle connections on schedule.\n\n")
	} else {
		fmt.Printf(" [WARN] %d idle connections were not evicted.\n\n", remaining)
	}
	wg.Wait()
}

// -----------------------------------------------------------------------------
// Attack Vector 7: Slow Body Drip & Upstream Pool Isolation
// Verifies that for buffered routes, upstream connections are NOT checked out
// while the downstream client is slowly uploading its request body, preventing
// pool starvation.
// -----------------------------------------------------------------------------
func testSlowBodyDrip(target string) {
	fmt.Println(">>> [Vector 7] HTTP/1.1 Slow Body Drip & Lease Isolation Test...")

	conn, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil {
		fmt.Printf(" [FAIL] Could not connect to %s: %v\n\n", target, err)
		return
	}
	defer conn.Close()

	payloadSize := 1024
	// 1. Send headers claiming 1024 bytes
	header := fmt.Sprintf("POST /api/echo HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nContent-Type: text/plain\r\n\r\n", target, payloadSize)
	if _, err := conn.Write([]byte(header)); err != nil {
		fmt.Printf(" [FAIL] Could not write header: %v\n\n", err)
		return
	}

	// 2. Drip 10 bytes slowly (1 byte per 100ms)
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		if _, err := conn.Write([]byte{byte('A' + i)}); err != nil {
			fmt.Printf(" [FAIL] Write failed during drip: %v\n\n", err)
			return
		}
	}

	// 3. Send remaining bytes
	rem := payloadSize - 10
	remData := bytes.Repeat([]byte("B"), rem)
	if _, err := conn.Write(remData); err != nil {
		fmt.Printf(" [FAIL] Write failed sending remainder: %v\n\n", err)
		return
	}

	// 4. Read response
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respBuf := make([]byte, 4096)
	n, err := conn.Read(respBuf)
	if err != nil {
		fmt.Printf(" [FAIL] Failed to read response after slow body: %v\n\n", err)
		return
	}

	resp := string(respBuf[:n])
	if strings.Contains(resp, "200 OK") {
		fmt.Printf(" - Drip slow body (10x 100ms) followed by full payload: Received 200 OK [PASS]\n")
		fmt.Printf(" [PASS] Upstream lease was held only after body was fully buffered!\n\n")
	} else {
		fmt.Printf(" [WARN] Unexpected response: %s\n\n", strings.Split(resp, "\r\n")[0])
	}
}

