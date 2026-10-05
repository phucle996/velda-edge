# Velda Edge — End-to-End (E2E) & Heavy Load Test Suite

A standalone, production-grade testbed for end-to-end multi-protocol traffic simulation, extreme saturation benchmarking, and long-running soak leak detection.

---

## 1. Architecture & Components

```
e2e/
├── backend/                       # High-performance upstream test server in Go
│   └── main.go                    # Serves HTTP/1.1, HTTP/2 (:8081) and Raw TCP Echo (:19001)
├── monitor/                       # Independent real-time OS telemetry tracker
│   └── live_tracker.sh            # Samples /proc/<pid>: RSS, FDs, Threads, TCP sockets
├── scenarios/                     # Dedicated heavy-load stress scenarios
│   ├── 01_http1_saturation.sh     # 4-phase wrk concurrency sweep (50 -> 200 -> 500 -> 1000 conns)
│   ├── 02_connection_churn.sh     # Non-keepalive 'Connection: close' accept-loop stress
│   ├── 03_reload_under_fire.sh    # Continuous atomic reload while flooding 80,000+ RPS
│   └── 04_soak_test.sh            # Sustained soak test with automatic before/after leak analysis
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
