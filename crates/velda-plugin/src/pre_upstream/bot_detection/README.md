# Deep Bot Detection & Progressive Tarpit (`bot_detection`)

```text
+-----------------------------------------------------------------------------------+
|                 RECOMMENDED TWO-TIER ANTI-BOT CHAIN (PRE_UPSTREAM)                |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  Downstream (HTTP/1.1, HTTP/2, HTTP/3)                                            |
|    ├── velda-tls  ──► RequestState.{ja4: u64, tls_traits: u16, is_tls: bool}      |
|    └── H1/H2/H3   ──► RequestState.wire_traits (u40 packed inside 64B line)       |
|          │                                                                        |
|          ▼                                                                        |
|  [1] ja4_filter (AccessLevel::None)                                               |
|        ├── Known bad JA4 or missing JA4 on TLS ──► 403 Forbidden (Short-Circuit)  |
|        └── Clean / Spoofed Browser JA4 ──► passes to [2]                          |
|          │                                                                        |
|          ▼                                                                        |
|  [2] bot_detection (AccessLevel::Read Headers, AccessLevel::None Payload)         |
|        ├── 1-Pass Feature Extract + Boot-Fused SoA Rule Scan ──► score (0..=100)  |
|        ├── L1 448-bit Cache-Line Bitfilter (+ 8B epoch = 64B) ──► Bypass L2 when clean |
|        └── L2 64-Shard Exact (client_ip, ja4) CLOCK Arena                         |
|              ├── score >= block_score / strike cap ──► 403 Forbidden              |
|              ├── score >= delay_score / active     ──► Progressive Action::Delay  |
|              └── Clean                             ──► Action::Continue           |
+-----------------------------------------------------------------------------------+
```

## 1. What This Plugin Does

`bot_detection` provides **Layer 7 cross-layer bot detection and progressive tarpit throttling** for bots that evade static JA4 denylists (such as `uTLS`, `curl-impersonate`, spoofed `User-Agent` scripts, or mixed clean/dirty traffic bots).

Instead of binary signature blocking (which risks false positives on uncommon proxies or custom clients), `bot_detection` aggregates weighted cross-layer contradictions on a **`0..=100` risk score** across TLS traits, HTTP/2 & HTTP/3 wire framing, and HTTP request headers, and mitigates offenders primarily via **progressive delay (`Action::Delay`)** — reserving hard `403` rejection for optional high-confidence thresholds (`block_score` / `strike_block_threshold`).

## 2. Where It Sits in Velda Edge

- **Execution Phase**: `pre_upstream` (runs after route matching on sensitive routes).
- **Recommended Pipeline Order**: Always attach **`ja4_filter` BEFORE `bot_detection`** (`"plugins": ["ja4_filter", "bot_detection"]`). Requests rejected by `ja4_filter` short-circuit immediately inside `run_pre_upstream_plugins_async`, preventing scanners and known bots from consuming header scans or tarpit slots in `bot_detection`.
- **Thread Placement**: Inline on Frontline Traffic Workers (`velda-worker-{i}`).
- **Supported Protocols**: `SupportedProtocols::ALL_HTTP` (HTTP/1.1, HTTP/2, HTTP/3; gRPC M2M routes are intentionally excluded at protocol capability validation, whereas [`ja4_filter`](../ja4_filter/) retains `ALL_HTTP | GRPC`).
- **Capability**: `AccessLevel::Read` for Headers (`AccessLevel::None` for Payload — zero-copy body streaming stays intact).

## 3. Requirements & Prerequisites

- **`bot_detection` enforces a strict explicit schema**: EVERY field inside `thresholds`, `escalation`, and every custom rule's `when` object must be explicitly declared in JSON (pass `null` for unused fields). Missing or unknown keys fail configuration validation at startup/reload.
- TLS structural signals (`tls_traits`) and wire framing signals (`wire_traits`) are automatically populated by `velda-tls`, `velda-http1`, `velda-http2`, and `velda-http3` inside the existing `64`-byte `RequestState`.

## 4. Quick Start Configuration

```json
{
  "id": "bot_detection",
  "config": {
    "mode": "enforce",
    "preset": "balanced_browser",
    "thresholds": {
      "delay_score": 45,
      "min_delay_ms": 150,
      "max_delay_ms": 2000,
      "block_score": 90
    },
    "escalation": {
      "window_secs": 30,
      "decay_secs": 10,
      "max_entries": 65536,
      "strike_block_threshold": null
    },
    "rules": []
  }
}
```

## 5. Documentation Directory

- [**RUNBOOK.md**](RUNBOOK.md): Mandatory schema reference, `when` field catalog, presets, deployment recipes, and incident triage.
- [**ARCHITECTURE.md**](ARCHITECTURE.md): 56-bit `RequestState` dictionary, boot-time SoA rule fusion, L1 448-bit (strict 64B) Cache-Line Bitfilter + L2 exact `(client_ip, ja4)` arena, and security invariants.
- [**BENCHMARK.md**](BENCHMARK.md): Isolated KVM comparison (`bot_detection` alone vs Nginx, HAProxy, Envoy, Apache APISIX, Kong), 8-vector evasion vs zero-false-positive matrix, PMU counters, and `64B` L1 overflow measurements.
- [**TEST.md**](TEST.md): Verified `bot_detection` unit tests and escalating local `bot/` harness matrix.
