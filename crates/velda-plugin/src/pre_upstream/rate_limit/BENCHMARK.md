# Performance Benchmark Report: Rate Limiting Plugin (`rate_limit`)

Performance benchmark specification and diagnostic matrix for the hierarchical L1 `CoreLane` + 64-shard `CLOCK` `ShardArena` + Multi-SoT `rate_limit` plugin.

---

## 1. Test Environment & Standardized 5-Tier Metric Taxonomy

### 1.1 Hardware & Network Testbed

*[Pending Re-Benchmark]* — Legacy measurements have been cleared following the ground-up rebuild (`CoreLane` micro-leasing, CSPRNG SWAR hashing, 16-way `CLOCK` slabs, monotonic timing, soft overdraft, and 9 SoT ETP adapters).

### 1.2 Standardized 5-Tier Diagnostic Matrix

1. **Tier 1 (Macro Network & L7 Delivery)**: Duration (s), Total Requests, Throughput (req/s), P50 / P90 / P99 / Max Latency (ms), HTTP Status Code Distribution (`200` / `429`), Success Rate (%).
2. **Tier 2 (System Resource Footprint)**: Frontline Worker CPU (%), Shadow Worker CPU (%), Bounded `ShardSlab` RSS, Heap Allocations / Request (`0` on hot path).
3. **Tier 3 (Kernel & OS Telemetry via `perf stat`)**: System Calls / Request (`0` on request decision), Context Switches / Request, Cache-line residency on `CoreLane`.
4. **Tier 4 (CPU Microarchitecture Counters via `perf stat`)**: Instructions / Request, Cycles / Request, IPC, Branch Misprediction Rate (%), L1D / L2 / L3 Hit Rates (%).
5. **Tier 5 (Plugin Mechanics)**: `CompiledKeyExtractor::extract` SWAR latency, `CoreLane::try_consume` CAS hit rate, 16-way `CLOCK` eviction overhead under cardinality flood, and Shadow Worker SoT push batch RTT across Redis / Memcached / Postgres / MySQL / NATS / DynamoDB / ClickHouse / Elasticsearch / S3.

---

## 2. Benchmark Scenarios

- **Scenario 01 — Warm L1 `CoreLane` Micro-Lease Hit (`burst_capacity: 5000`, single IP)**: Measure frontline RPS and P99 latency when requests consume pre-leased batches atomically without locking `ShardSlab`.
- **Scenario 02 — Hierarchical 3-Tier Multi-Header Evaluation (`["x-tenant-id", "x-api-key", "client_ip"]`)**: Measure throughput across mixed legitimate tenants vs IP-rotating attackers saturating Tier 0.
- **Scenario 03 — IP-Flood Cardinality Attack (`1M` spoofed source IPs over `max_entries: 65536`)**: Verify zero tail latency stalls under continuous O(1) 16-way `CLOCK` eviction and zero memory growth.
- **Scenario 04 — Bounded Soft Overdraft Spike (`overdraft_percent: 5`) vs Hard Flood Blocking**: Verify minor burst overshoot passes within grace while sustained floods trip HTTP `429` cleanly.
- **Scenario 05 — Multi-SoT Shadow Sync Overhead (`pbww: 20`, `sync_interval_ms: 200`)**: Measure Frontline P99 isolation while Shadow Worker flushes dirty batches to Redis / PostgreSQL / ClickHouse.

---

## 3. Macro Cluster Benchmark Results

| Metric | Scenario 01 | Scenario 02 | Scenario 03 | Scenario 04 | Scenario 05 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Throughput (req/s)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Median Latency P50 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Tail Latency P99 (ms)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **Instructions / Request** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |
| **L1D Hit Rate (%)** | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* | *[TBD]* |

---

## 4. Release Assembly & Symbol Audit (`x86-64-v3`)

*[Pending Re-Audit]* — Run `RUSTFLAGS="-C target-cpu=x86-64-v3" cargo rustc -p velda-plugin --release --lib -- --emit=asm -C llvm-args=-x86-asm-syntax=intel` and inspect `RateLimitPlugin.*on_upstream_request` and `RateLimitArena::evaluate_key` to record verified instruction counts and zero-allocation checks.
