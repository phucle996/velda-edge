# Velda Edge — End-to-End (E2E) & Heavy Load Test Suite

A standalone, production-grade testbed for end-to-end multi-protocol traffic simulation, extreme saturation benchmarking, and long-running soak leak detection.

---

## 1. Architecture & Components

```
e2e/
├── backend/                       # High-performance upstream test servers & tools in Go
│   ├── main.go                    # Serves HTTP/1.1, HTTP/2 (:8081) and Raw TCP Echo (:19001)
│   └── tcp_blaster.go             # High-speed multi-goroutine L4 TCP proxy blaster
├── monitor/                       # Independent real-time OS telemetry tracker
│   └── live_tracker.sh            # Samples /proc/<pid>: RSS, FDs, Threads, TCP sockets
├── scenarios/                     # Dedicated heavy-load stress scenarios
│   ├── 01_http1_saturation.sh     # 4-phase wrk concurrency sweep (50 -> 200 -> 500 -> 1000 conns)
│   ├── 02_connection_churn.sh     # Non-keepalive 'Connection: close' accept-loop stress
│   ├── 03_reload_under_fire.sh    # Continuous atomic reload while flooding 80,000+ RPS
│   ├── 04_soak_test.sh            # Sustained soak test with automatic before/after leak analysis
│   ├── 05_l4_tcp_flood.sh         # L4 TCP high-speed bidirectional flood & splice saturation
│   ├── 06_http2_multiplex_storm.sh# HTTP/2 deep multiplexing storm (up to 25.6k in-flight streams)
│   ├── 07_http2_scan_rst_attack.sh# HTTP/2 fast-path 404 & scanner probe attack defense
│   ├── 08_http1_streaming_stress.sh# HTTP/1.1 chunked streaming, 256KB body & RST recovery
│   └── 09_protocol_cross_pool.sh  # Parallel cross-protocol saturation (L4 + H1 + H2 simultaneously)
└── run.sh                         # Unified master CLI runner
```

---

## 2. Quick Start

### Check Status of Daemons & Tools
```bash
./e2e/run.sh status
```

### Start / Stop Upstream Backend
The backend runs cleanly as a systemd user daemon (`velda-e2e-backend.service`):
```bash
./e2e/run.sh start-backend
./e2e/run.sh stop-backend
```

---

## 3. Running Scenarios

### Scenario 1: HTTP/1.1 Saturation & Peak Throughput
Runs `wrk` across 4 progressive connection tiers (up to 1,000 parallel streams):
```bash
./e2e/run.sh test saturation
```

### Scenario 2: Rapid Connection Churn & Socket Recycling
Tests epoll accept-loop and socket cleanup under 50,000+ new TCP handshakes/sec using `Connection: close`:
```bash
./e2e/run.sh test churn
```

### Scenario 3: Atomic Hot-Reload Under Heavy Fire
Floods 80,000+ RPS while continuously triggering 5 rapid atomic runtime swaps:
```bash
./e2e/run.sh test reload
```

### Scenario 4: Sustained Soak Test & Memory Leak Audit
Runs sustained load for any custom duration (default: 60s, e.g. `10m`, `2h`) with per-second telemetry logging to CSV:
```bash
./e2e/run.sh test soak 60s
```

### Scenario 5: L4 Raw TCP Bidirectional Flood & Splice
Blasts full-duplex TCP traffic through `tcp-ingress` (:9000 -> :19001) up to 1,000 connections with high throughput (>300 MB/s, 160k+ ops/s):
```bash
./e2e/run.sh test l4
```

### Scenario 6: HTTP/2 Deep Multiplexing Storm
Uses `h2load` over TLS 1.3 (:8443) with up to 200 connections and 128 streams/conn (25,600 in-flight streams simultaneously, 45,000+ RPS):
```bash
./e2e/run.sh test h2
```

### Scenario 7: HTTP/2 Fast-Path 404 & Scanner Defense
Floods 50,000 requests against scanner attack probes (e.g. `/.env`, `/wp-admin`) at 140,000+ RPS while verifying legitimate traffic experiences 0 drops:
```bash
./e2e/run.sh test h2-scan
```

### Scenario 8: HTTP/1.1 Streaming, Chunked & Large Payloads
Tests RFC 9112 chunked transfer encoding, Server-Sent Events, 64KB/256KB binary downloads (>2.5 GB/s), upload streaming, and upstream TCP RST recovery:
```bash
./e2e/run.sh test streaming
```

### Scenario 9: Parallel Cross-Protocol Saturation
Simultaneously blasts L4 TCP (:9000), HTTP/1.1 (:8080), and HTTP/2 (:8443) concurrently for 20s to ensure zero lock contention and zero protocol starvation:
```bash
./e2e/run.sh test cross-pool
```

### Run All Scenarios Sequentially
```bash
./e2e/run.sh test all
```

### Real-Time Live Monitor
Launch the terminal telemetry dashboard in another window:
```bash
./e2e/run.sh monitor
```
EOF
