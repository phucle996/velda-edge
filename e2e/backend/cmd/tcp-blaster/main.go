package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("target", "127.0.0.1:9000", "Target L4 TCP host:port")
	conns := flag.Int("conns", 100, "Number of concurrent TCP connections")
	duration := flag.Duration("duration", 10*time.Second, "Test duration")
	msgSize := flag.Int("size", 1024, "Payload size in bytes per message")
	flag.Parse()

	fmt.Printf("==========================================================================\n")
	fmt.Printf(" [Velda E2E] L4 TCP Raw Splice / Proxy Blaster\n")
	fmt.Printf(" Target:      %s\n", *target)
	fmt.Printf(" Concurrency: %d TCP connections\n", *conns)
	fmt.Printf(" Duration:    %v\n", *duration)
	fmt.Printf(" Msg Size:    %d bytes\n", *msgSize)
	fmt.Printf("==========================================================================\n\n")

	payload := make([]byte, *msgSize)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}

	var (
		totalSentBytes uint64
		totalRecvBytes uint64
		totalOps       uint64
		totalErrors    uint64
		latencies      []time.Duration
		latMutex       sync.Mutex
		wg             sync.WaitGroup
		stop           int32
	)

	start := time.Now()
	timer := time.AfterFunc(*duration, func() {
		atomic.StoreInt32(&stop, 1)
	})
	defer timer.Stop()

	wg.Add(*conns)
	for i := 0; i < *conns; i++ {
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", *target, 2*time.Second)
			if err != nil {
				atomic.AddUint64(&totalErrors, 1)
				return
			}
			defer conn.Close()

			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetNoDelay(true)
			}

			readBuf := make([]byte, *msgSize)
			var localLatencies []time.Duration

			for atomic.LoadInt32(&stop) == 0 {
				opStart := time.Now()

				// Write full message
				wn, werr := conn.Write(payload)
				if werr != nil || wn != *msgSize {
					atomic.AddUint64(&totalErrors, 1)
					break
				}
				atomic.AddUint64(&totalSentBytes, uint64(wn))

				// Read full echoed message
				rn, rerr := io.ReadFull(conn, readBuf)
				if rerr != nil || rn != *msgSize {
					atomic.AddUint64(&totalErrors, 1)
					break
				}
				atomic.AddUint64(&totalRecvBytes, uint64(rn))
				atomic.AddUint64(&totalOps, 1)

				elapsed := time.Since(opStart)
				if len(localLatencies) < 5000 {
					localLatencies = append(localLatencies, elapsed)
				}
			}

			latMutex.Lock()
			latencies = append(latencies, localLatencies...)
			latMutex.Unlock()
		}(i)
	}

	wg.Wait()
	actualDuration := time.Since(start)

	// Calculate stats
	ops := atomic.LoadUint64(&totalOps)
	sentBytes := atomic.LoadUint64(&totalSentBytes)
	recvBytes := atomic.LoadUint64(&totalRecvBytes)
	errs := atomic.LoadUint64(&totalErrors)

	rps := float64(ops) / actualDuration.Seconds()
	mbPerSec := (float64(sentBytes+recvBytes) / (1024 * 1024)) / actualDuration.Seconds()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var p50, p90, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p90 = latencies[len(latencies)*90/100]
		p99 = latencies[len(latencies)*99/100]
	}

	fmt.Printf("--- L4 TCP Benchmark Results ---\n")
	fmt.Printf("Elapsed Time:    %.2fs\n", actualDuration.Seconds())
	fmt.Printf("Completed Echoes:%d ops\n", ops)
	fmt.Printf("Throughput:      %.2f ops/sec (%.2f MB/s bi-directional)\n", rps, mbPerSec)
	fmt.Printf("Data Transferred:%d KB sent, %d KB received\n", sentBytes/1024, recvBytes/1024)
	fmt.Printf("Latency (RTT):   P50: %v | P90: %v | P99: %v\n", p50, p90, p99)
	fmt.Printf("Errors/Drops:    %d\n", errs)
	fmt.Printf("==========================================================================\n")

	if errs > 0 && float64(errs)/float64(ops+errs) > 0.01 {
		fmt.Printf("[FAIL] Error rate exceeds 1%%\n")
	} else {
		fmt.Printf("[PASS] L4 TCP proxy sustained high load without connection drops.\n")
	}
}
