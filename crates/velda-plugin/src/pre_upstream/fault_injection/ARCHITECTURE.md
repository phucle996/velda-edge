# Unified Fault Injection Architecture (`fault_injection`)

## 1. Execution Model & Cacheline Layout

`FaultInjectionPlugin` is laid out with `#[repr(C, align(64))]` so its fast-path guard sits at offset `0` of L1D cache line 0:

```text
+-------------------------------------------------------------------------+
|                    FaultInjectionPlugin (64-Byte Aligned)               |
+-------------------------------------------------------------------------+
| offset 0..8   : active_mask (u64)                                       |
|                 ├─ bits 0..32  : delay_threshold (u32) | override_bit   |
|                 └─ bits 32..64 : abort_threshold (u32)                  |
| offset 8+     : condition (FaultCondition: IP intervals, HeaderKey)     |
|               : delay (Option<FaultDelayConfig>)                        |
|               : abort (Option<FaultAbortConfig { threshold, status }>)  |
+-------------------------------------------------------------------------+
```

- **Combined Delay + Abort Composition**: A single 64-bit thread-local `XorShift64*` entropy draw (`fast_random_u64()`) supplies the upper 32 bits for delay probability evaluation and the lower 32 bits for abort probability evaluation. Draws are skipped when a rule is `0%`, `100%` (`u32::MAX`), or overridden by header.
- **Non-Blocking Latency**: `Action::delay(dur, next_action)` schedules async task timer waits inside the edge pipeline without blocking the Tokio worker thread.

---

## 2. Protocol-Blind Status & Header Bypass Invariant

`FaultAbortConfig` stores only a 1-byte `velda_core::InternalStatus` alongside its `u32` threshold—zero pre-allocated `L7Response` bodies, zero HTTP status structs, and zero gRPC trailer templates inside `velda-plugin`.

When an abort triggers:
1. Control headers (`x-fault-delay-ms`, `x-fault-abort-status`) have already been stripped using pre-hashed `HeaderKey` lookups (`KEY_FAULT_DELAY_MS`, `KEY_FAULT_ABORT_STATUS`).
2. The plugin stamps `x-velda-fault: abort` using `HeaderWorkspace::insert_bypass`, setting the internal bypass flag bit.
3. It returns `Action::Status(status)` (or `Action::delay(dur, Action::Status(status))`).
4. Downstream pipelines call protocol renderers (`velda_http1::server::early_response::status_response`, `velda_http2::server::early_response::status_response`, `velda_http3::status_response`, or `velda_grpc::GrpcWire::prepare_status_headers`), all of which call `headers.retain_bypass_only()`, stripping upstream/client clutter while preserving `x-velda-fault: abort`.

---

## 3. IP Interval & Security Invariants

- **Disjoint Interval Compilation**: `IpRangeMatcher::from_cidrs` separates IPv4 and IPv6 CIDRs, merges overlapping or contiguous ranges (`s <= last.end + 1`), and stores boxed sorted slices (`1` range checked in two integer comparisons; `N > 1` ranges evaluated via `partition_point` in $O(\log N)$ with zero allocations).
- **Authoritative Client IP Priority**: `extract_client_ip` reads `req.state.client_ip` (downstream peer address) first before checking `x-real-ip` or right-most `x-forwarded-for`.
- **Control Header Sanitization**: `x-fault-delay-ms` and `x-fault-abort-status` are removed on every request that contains them, even if `active_mask == 0` or the request misses `scope`.
