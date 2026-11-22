# JA4 TLS Fingerprint Filter (`ja4_filter`)

```text
+-----------------------------------------------------------------------------------+
|                        JA4 FILTER IN VELDA EDGE PIPELINE                          |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  ClientHello ──► [velda-tls] Zero-Alloc JA4 Parser ──► u64 ID                     |
|                                                         │                         |
|  [velda-edge] DownstreamMeta::peek_tcp_ja4() ───────────┴──► RequestState.ja4     |
|                                                                   │               |
|  L7 Pipeline: decode -> pre_route -> route -> [pre_upstream: ja4_filter]          |
|                                                    │                              |
|                              ┌─────────────────────┴─────────────────────┐        |
|                              ▼                                           ▼        |
|                      ja4 ∈ denied_ids                           ja4 ∉ denied_ids  |
|                Action::Reject(Forbidden / 403)                  Action::Continue  |
+-----------------------------------------------------------------------------------+
```

## 1. What This Plugin Does

`ja4_filter` blocks automated bots, scrapers, and offensive security scanners on specific routes by matching each connection's **JA4 TLS ClientHello fingerprint** against a compiled `deny` list.

Unlike HTTP `User-Agent` checks (which any script can spoof in one header line), JA4 hashes the TLS handshake's protocol version, cipher suites, extensions, ALPN, and signature algorithms into a deterministic `64-bit` integer (`u64`) inside `RequestState`.

## 2. Where It Sits in Velda Edge

- **Execution Phase**: `pre_upstream` (runs **after** route matching, strictly on routes where `ja4_filter` is attached).
- **Thread Placement**: Inline on Frontline Traffic Workers (`velda-worker-{i}`).
- **Supported Protocols**: HTTP/1.1, HTTP/2, HTTP/3, and gRPC (`SupportedProtocols::ALL_HTTP | SupportedProtocols::GRPC`).
- **Capability**: `AccessLevel::None` for both Headers and Payload (zero-copy direct streaming remains fully active).

## 3. Requirements & Prerequisites

- TLS connections terminated by `velda-edge` (`http1`, `http2`, `grpc` TCP listeners) automatically populate `RequestState.ja4` via `TcpStream::peek` + `velda_tls::compute_ja4_u64`.
- Plaintext HTTP connections have `RequestState.ja4 == 0` (sentinel) and pass through (`Action::Continue`) without matching any rule.

## 4. Quick Start Configuration

Attach `ja4_filter` to sensitive routes (e.g., `/login`, `/checkout`) while leaving public API/webhook routes untouched:

```json
{
  "id": "ja4_filter",
  "config": {
    "deny": [
      "t13d1516h2_8daaf6152771_e5627efa2ab1",
      "t13i3012h2_1d37bd780c83_8537cf56674e"
    ]
  }
}
```

## 5. Documentation Directory

- [**RUNBOOK.md**](RUNBOOK.md): Configuration reference, entry formats, route attachment recipes, and incident triage.
- [**ARCHITECTURE.md**](ARCHITECTURE.md): Zero-allocation `ClientHello` parser, `RequestState` 64B cache-line layout, and ASM audit.
- [**BENCHMARK.md**](BENCHMARK.md): 5-tier performance specification and KVM measurement results.
- [**TEST.md**](TEST.md): Verified unit and end-to-end test cases.
