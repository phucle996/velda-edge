# Deep Bot Detection (`bot_detection`) — Test Matrix & Verification Report

All unit, compiler, and end-to-end live probe checks for `bot_detection` (and upstream bitfield / TLS dependencies) are executed via:

```bash
cargo fmt --check
cargo clippy -p velda-core -p velda-tls -p velda-plugin -p velda-edge --all-targets --all-features -- -D warnings
cargo test -p velda-core
cargo test -p velda-tls -- ja4
cargo test -p velda-plugin -- ja4_filter bot_detection
```

## 1. Core Layout & Bitfield Packing (`velda-core`)
Executed via `cargo test -p velda-core` (`106` unit tests + `5` + `6` + `3` integration tests passed; `0` failed):
- `bitfield::wire_traits::tests::test_pseudo_order_permutations_are_bijective`: Verifies HTTP/2 pseudo-header order permutations (`masp`, `mpas`, `mspa`, `msap`, `maps`) encode without collision into 5-bit `wire_traits`.
- `crates/velda-core/tests/layout_and_alloc.rs`: Verifies `size_of::<RequestState>() == 64` (single L1d cache line after packing `tls_traits: u16` + 40-bit `wire_traits`) and zero-allocation hot-path invariants.

## 2. Plugin Unit Tests (`crates/velda-plugin/src/pre_upstream/bot_detection/plugin.rs` — 3 Tests)
Executed via `cargo test -p velda-plugin -- ja4_filter bot_detection` (`5` passed total across `ja4_filter` + `bot_detection`; `441` other tests filtered out):
- `test_strict_schema_rejects_missing_keys_in_when_and_thresholds`: Verifies `parse_strict_bot_detection_config` rejects omitted keys in `thresholds`, `escalation`, or any rule's `when` object instead of silently defaulting them.
- `test_boot_fusion_merges_duplicate_predicates`: Verifies boot-time SoA rule compiler deduplicates shared header lookups (`<= 64` unique headers) and fuses identical rule predicates cleanly.
- `test_progressive_delay_only_mode_and_l1_l2_escalation`: Verifies delay-only mode (`block_score: null`), strict 64B L1 448-bit Cache-Line Bitfilter + L2 64-shard CLOCK tracking per `(client_ip, ja4)`, exponential tarpit doubling (`100ms -> 200ms -> 400ms -> 800ms -> 403`), persistence when a flagged `(client_ip, ja4)` subsequently sends a clean-looking request, isolation of an unflagged IP sharing the same Chrome JA4 (`Action::Continue`), and strike decay after `window_secs`.

## 3. Direct KVM Evasion, Zero-FP & Cache-Line Saturation Probes ([`bot/`](bot/))
All scripts target `velda-gw` (`192.168.122.10:8443`) directly with `plugins: ["bot_detection"]` (`ja4_filter` disabled):
- [`probe_bots.py`](bot/probe_bots.py) & [`probe_bots.go`](bot/probe_bots.go): Simple Python HTTP/1.1 & Go HTTP/2 Chrome-spoofing request scripts paired with honest API/Firefox baselines (`200 OK`, `< 80ms`).
- [`kvm_cross_gateway_matrix.go`](bot/kvm_cross_gateway_matrix.go) & [`kvm_cacheline_flood.go`](bot/kvm_cacheline_flood.go): Full cross-gateway comparison (`C1`–`C3`, `B1`–`B5`) and `2,000`-JA4 `L1BitFilter` saturation probes documented in [`BENCHMARK.md`](BENCHMARK.md).

Executed prior to compacting `L1BitFilter` to `64` bytes (`448`-bit filter + `8`-byte epoch) using `x86-64-v3` release builds of `velda-edge` and `velda-sync` on `velda-gw` (`4` vCPUs, Intel Core i5-11400H, Linux `6.8.0`) forwarding HTTPS (`:8443`) to `/opt/velda/velda-bench-backend` (`192.168.122.20:8081`), driven by [`kvm_cacheline_flood.go`](bot/kvm_cacheline_flood.go) and `wrk -t4 -c64 -d8s --latency` from `velda-obs` (`192.168.122.20`) with `window_secs: 120`, `max_entries: 512`, `delay_score: 35`, `min_delay_ms: 100`, `max_delay_ms: 1600`:

| Phase | Clean `wrk` RPS | Clean `p50` / `p99` | Clean Errors | `L1-dcache-load-misses` | `cache-misses` | `futex` Syscalls | Observed Bot / Overflow Behavior |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Pre-Overflow (`0/512` L1 bits)** | `49,783.00` | `0.97ms` / `5.61ms` | `0` (`398,638` OK) | `12.84%` (`967.8M` / `7.54B`) | `17.96%` | `49` | `bot#1`: `strike#0 = 679ms`, `strike#1 = 1355ms`; clean baseline `2ms` |
| **1,998 Unique Bot JA4 Flood (`8.26s`, `200` workers)** | — | — | — | — | — | — | `1,998 / 1,998` delayed (`>= 80ms`), `0` errors; saturates `512`-bit L1 cache line and overflows `512`-entry L2 (`2,000 > 512`) inside `120s` window |
| **Post-Overflow (`~512/512` L1 saturated + L2 overflowed)** | `51,182.28` | `0.99ms` / `5.36ms` | `0` (`414,577` OK) | `13.24%` (`1.01B` / `7.65B`) | `16.90%` | `223` | `200/200` clean probe requests passed at `0ms` delay (`0` false positives); oldest `bot#1` evicted cleanly (`1ms`), recent `bot#end` retained (`102ms` even on clean UA) |
