# Performance Benchmark Report: API Metering & Tenant Usage Accounting (`api_metering`)

Verified release assembly audit, isolated KVM load benchmark (`velda-gw` $\leftrightarrow$ `velda-obs`), and 304k-account memory scaling measurement for `api_metering`.

---

## 1. Verified Release Assembly & Symbol Audit (`x86-64-v3`)

Generated with:
```bash
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo rustc -p velda-plugin --release --lib -- --emit=asm
python3 .agent/skills/velda-benchmark/scripts/audit_asm.py --symbol "ApiMeteringPlugin.*on_upstream_request" target/release/deps/velda_plugin-*.s
```

| Symbol | Role | Heap Escape (`alloc::*`) | Stack Spills | Indirect Jumps | `ud2` Traps |
| :--- | :--- | :---: | :---: | :---: | :---: |
| `ApiMeteringPlugin::on_upstream_request` | Hot-path entrypoint (`408` insts) | **0** | `0 W / 0 R` | `0` | **0** |
| `ShardedMeteringState::evaluate_chain_compiled` | 2-phase hierarchical check & O(1) commit | **0** | `0 W / 0 R` | `0` | **0** |
| `refill_from_node_shard` | Node-Global shard escrow refill | **0** | `0 W / 0 R` | `0` | **0** |
| `handle_missing_account` | `#[cold]` `400 Bad Request` responder | **0** | `0 W / 0 R` | `0` | **0** |
| `handle_overage_allowed` | `#[cold]` overage header tagging (`115` insts) | **0** | `0 W / 0 R` | `0` | **0** |
| `handle_exhausted` | `#[cold]` `402` JSON / `429` builder (`401` insts) | `2` (cold path only) | `0 W / 0 R` | `0` | **0** |

---

## 2. Isolated KVM End-to-End Benchmark (`velda-gw` $\leftrightarrow$ `velda-obs`)

- **Testbed**:
  - Gateway (`velda-gw`): 2 vCPUs (11th Gen Intel Core i5-11400H @ 2.70GHz), 1.9 GB RAM, Linux 6.8.0, `velda-edge v0.1.0-release` (`2` pinned workers, HTTP/1.1 `0.0.0.0:8080`).
  - Client & Backend (`velda-obs`): 4 vCPUs, `velda-bench-backend` (`192.168.122.20:8081` `/ping`), `wrk -t4 -c64 -d10s --latency` (3s warmup + 2 × 10s runs per mode).

| Mode | Run 1 (req/s) | Run 2 (req/s) | Mean (req/s) | Delta vs Raw | Avg Latency | P50 Latency | P99 Latency |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Raw Proxy** (`plugins: []`) | 83,105.46 | 76,523.15 | **79,814.31** | — | 771.63–842.24 µs | 697–758 µs | 2.47–3.02 ms |
| **`api_metering` 1-level** (`x-tenant-id`) | 78,234.92 | 75,178.81 | **76,706.87** | **-3.89%** | 815.32–850.00 µs | 753–794 µs | 2.16–2.67 ms |
| **`api_metering` 3-level** (`tenant` + `team` + `user`) | 68,666.03 | 69,793.51 | **69,229.77** | **-13.26%** | 920.00–930.00 µs | 860–870 µs | 2.52–2.88 ms |

- **Process Memory & Stability**: Raw Proxy RSS `33,320 KB` vs `api_metering` RSS `33,804 KB` (`+484 KB` delta after ~3.38M requests), `0` socket/HTTP errors, `0` warnings/panics in `velda-edge` logs.

---

## 3. Memory Scaling & Fragmentation Probe (`--release`)

Measured via `/proc/self/status` (`VmRSS` / `VmHWM`) across `1,000` tenants × `3` tiers × `100` customers:

| Scenario | Total Tracked Accounts | Workers | `VmRSS` Delta | `VmHWM` vs `VmRSS` | Population Time |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `1k` tenants × `100` customers (`2`-level) | **101,000** | 2 | **45.01 MB** (`46,088 KB`) | `48,752 == 48,752 KB` (0 spike) | 51.22 ms |
| `1k` tenants × `3` tiers × `100` customers (`3`-level) | **304,000** | 2 | **144.58 MB** (`148,048 KB`) | `151,224 == 151,224 KB` (0 spike) | 214.79 ms |
| `1k` tenants × `3` tiers × `100` customers (`3`-level) | **304,000** | 8 | **188.01 MB** (`192,524 KB`) | `195,704 == 195,704 KB` (0 spike) | 105.38 ms |

- **External Fragmentation**: `PageSlab<T>` allocates contiguous `(k + 1) * 4096 B` 4 KiB-aligned extents (`alloc_zeroed`) and deallocates old slabs immediately on resize, keeping `VmHWM == VmRSS` with zero heap scatter.
