# JA4 TLS Fingerprint Filter (`ja4_filter`)

```text
+-----------------------------------------------------------------------------------+
|                        JA4 FILTER IN VELDA EDGE PIPELINE                          |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  TCP ClientHello  ──► [velda-tls] LazyConfigAcceptor (1-pass + multi-record 0x16) |
|  QUIC ClientHello ──► [velda-tls] VeldaQuicSession::read_handshake                |
|                                                         │                         |
|                                                         ▼                         |
|                                              RequestState.{is_tls, ja4}           |
|                                                         │                         |
|  L7 Pipeline: decode -> pre_route -> route -> [pre_upstream: ja4_filter]          |
|                                                    │                              |
|                       ┌────────────────────────────┼────────────────────┐         |
|                       ▼                            ▼                    ▼         |
|                !is_tls (plain)          is_tls && (ja4==0        is_tls && clean  |
|                Action::Continue          || ja4 ∈ denied)       Action::Continue  |
|                                          Action::Reject (403)   (-> bot_detection)|
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

## 3. Requirements, Fail-Closed Semantics & Pipeline Pairing

- **Unified TCP + QUIC Extraction**: TLS TCP listeners (`http1`, `http2`, unified HTTP, `grpc-tcp`) capture the complete `ClientHello` inside `TlsServerEngine::accept`, while QUIC/UDP listeners (`http3`, `grpc-udp`) extract it inside `VeldaQuicSession::read_handshake` (`velda_tls::extract_quic_signals`).
- **Fail-Closed on TLS vs Plaintext Bypass**: Plaintext listeners (`req.state.is_tls == false`) pass through (`Action::Continue`). Any request on a TLS listener (`is_tls == true`) with `ja4 == 0` is strictly rejected (`403 Forbidden`) with no fallback/fail-open bypass.
- **Recommended Pairing (`ja4_filter` -> `bot_detection`)**: Run `ja4_filter` **before** [`bot_detection`](../bot_detection/) (`"plugins": ["ja4_filter", "bot_detection"]`). `ja4_filter` eliminates known bot signatures at ~60 instructions before `bot_detection` inspects headers or applies progressive tarpit delays against JA4-spoofing bots.

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
