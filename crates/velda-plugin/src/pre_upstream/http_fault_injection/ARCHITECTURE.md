# HTTP Fault Injection Plugin (`http_fault_injection`) — Architectural Specification

This document details the internal architecture, zero-allocation algorithms, memory layout, targeting scope, and execution pipeline of the **`http_fault_injection`** plugin in Velda Edge.

---

## 1. Architectural Philosophy & Invariants

The `http_fault_injection` plugin operates at the **`pre_upstream`** pipeline phase immediately after route selection and before upstream dispatch. It provides Chaos Engineering and resilience testing exclusively for HTTP traffic (HTTP/1.1, HTTP/2 Web/REST, HTTP/3).

```mermaid
flowchart TD
    Req["Incoming L7Request"] --> Scope{"Scope Match?\n(client_ips & header_match)"}
    Scope -->|No Match| Bypass["Action::Continue\n(< 5ns Fast-Path)"]
    
    Scope -->|Matches| DynamicOverride{"allow_header_override = true?"}
    DynamicOverride -->|Yes| ExtractHeaders["Extract & Clamp Headers\n(x-fault-delay-ms, x-fault-abort-code)"]
    DynamicOverride -->|No| EvalDelay
    
    ExtractHeaders --> PurgeHeaders["Security Hygiene:\nPurge Override Headers from L7Request"]
    PurgeHeaders --> EvalDelay["Evaluate Delay\n(Fixed / Range PRNG Roll)"]
    
    EvalDelay --> EvalAbort["Evaluate Abort\n(Status Code 400..=599 PRNG Roll)"]
    
    EvalAbort --> ActionBranch{"Fault Composition"}
    ActionBranch -->|Delay + Abort| DelayAbort["Action::delay(dur, Action::Respond(resp))"]
    ActionBranch -->|Delay Only| DelayOnly["Action::delay(dur, Action::Continue)"]
    ActionBranch -->|Abort Only| AbortOnly["Action::Respond(resp)\n(Pre-compiled Body in RAM)"]
    ActionBranch -->|No Fault Roll| Bypass
```

### Core Invariants:
1. **Precedence Rule (Delay First, Then Abort)**: When a request matches both delay and abort criteria, the gateway delays execution first, then aborts with the synthetic HTTP response. This accurately models cascading database timeouts and worker pool exhaustion.
2. **Zero-Syscall PRNG**: Uses a thread-local XorShift64* pseudo-random number generator running in ~1 nanosecond with zero locks, zero memory allocations, and zero syscalls.
3. **Disjoint Interval IP Matching**: All IPv4 and IPv6 CIDR prefixes are converted at configuration compile time into sorted, non-overlapping `[start, end]` integer ranges (`u32` for IPv4, `u128` for IPv6). Matching uses $O(\log N)$ binary search (`partition_point`) with zero heap allocations.
4. **Authoritative Anti-Spoofing Extraction**: Evaluates edge `x-real-ip` first, falling back to nearest right-most proxy in `x-forwarded-for`.
5. **Slowloris Defense**: Dynamic delay override is strictly clamped to `max_override_delay_ms` (defaults to 5,000ms). Un-scoped dynamic override is rejected fail-fast at configuration compile time.
6. **Anti-Leak Token Purging**: All test tokens (`x-fault-delay-ms` and `x-fault-abort-code`) are stripped from request headers before upstream dispatch.
7. **RFC 9110 Error Bounds**: Abort status codes are strictly clamped to `400..=599`. Non-error status codes (200, 204, 304) are rejected.

---

## 2. Memory Layout & Data Structures

```mermaid
classDiagram
    class HttpFaultInjectionPlugin {
        +String id
        +HttpFaultCondition condition
        +Option~HttpDelayConfig~ delay
        +Option~HttpAbortConfig~ abort
    }

    class HttpFaultCondition {
        +Option~IpRangeMatcher~ ip_matcher
        +Option~HeaderMatchRule~ header_match
        +bool allow_header_override
        +u64 max_override_delay_ms
    }

    class IpRangeMatcher {
        +Box~[(u32, u32)]~ v4_ranges
        +Box~[(u128, u128)]~ v6_ranges
    }

    class HeaderMatchRule {
        <<enumeration>>
        Present(HeaderName)
        Exact(HeaderName, String)
    }

    class HttpDelayConfig {
        +f64 percentage
        -u64 threshold
        +u64 min_ms
        +u64 max_ms
    }

    class HttpAbortConfig {
        -u64 threshold
        +StatusCode status
        +Bytes body
        -HeaderValue content_length_val
        +Vec~HeaderName, HeaderValue~ headers
        -L7Response default_response
    }

    HttpFaultInjectionPlugin *-- HttpFaultCondition
    HttpFaultInjectionPlugin *-- HttpDelayConfig
    HttpFaultInjectionPlugin *-- HttpAbortConfig
    HttpFaultCondition *-- IpRangeMatcher
    HttpFaultCondition *-- HeaderMatchRule
```

### 2.1 Pre-Allocated Abort Response (`HttpAbortConfig`)
The response body (`Bytes::from_static(b"fault filter abort\n")`), header map (`Content-Type`, `Content-Length`, `x-velda-fault: abort`), and default `L7Response` are created once during snapshot compilation:
- **Hot-Path Abort**: Clones the pre-warmed `default_response` via an atomic reference-count bump. Zero string formatting, zero heap allocation.

### 2.2 Disjoint Interval Binary Search (`IpRangeMatcher`)
- At startup, all CIDRs (e.g. `10.0.0.0/8`, `192.168.1.0/24`) are converted into `(start_bits, end_bits)` tuples, sorted by start bits, and contiguous/overlapping intervals are merged.
- At runtime:
  - If $N = 1$: Evaluates in ~0.5ns via two integer comparisons (`ip >= start && ip <= end`).
  - If $N > 1$: Evaluates in $O(\log N)$ using cache-friendly `partition_point`.

---

## 3. Protocol Isolation Invariant

- **Strict Boundary**: Declares `supported_protocols: SupportedProtocols::ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3).
- **gRPC Decoupling**: For gRPC length-prefixed streaming traffic, use [`grpc_fault_injection`](../grpc_fault_injection/). `http_fault_injection` never intercepts gRPC routes.
