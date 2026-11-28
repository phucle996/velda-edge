# Deep Bot Detection (`bot_detection`) — Operations Runbook

> [!IMPORTANT]
> **Recommended Ordering (`ja4_filter` -> `bot_detection`)**:
> Attach `"plugins": ["ja4_filter", "bot_detection"]` on protected routes (`/login`, `/checkout`). `ja4_filter` drops known botnet TLS signatures immediately with `403`, so `bot_detection` only evaluates traffic that passed the TLS denylist.

> [!NOTE]
> **Strict Explicit Config Philosophy**:
> `bot_detection` rejects both omitted keys and unknown keys inside `thresholds`, `escalation`, and every rule's `when` object (`parse_strict_bot_detection_config`). Set unused fields explicitly to `null`.

---

## 1. Top-Level & Threshold Parameter Reference

| Field | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `mode` | `String` | `"enforce"` \| `"monitor"` | `"monitor"` computes scores without delaying/blocking; `"enforce"` applies `Delay`/`Reject`. |
| `preset` | `String` | `"none"` \| `"balanced_browser"` \| `"strict_browser"` | Built-in cross-layer ruleset expanded before boot-time fusion. |
| `thresholds.delay_score` | `Option<u8>` | `1..=100` or `null` | Minimum risk score triggering progressive delay (`< block_score`). |
| `thresholds.min_delay_ms` | `Option<u64>` | `1..=10000` or `null` | Base delay in milliseconds at `delay_score` (all 3 delay knobs must be non-null or null together). |
| `thresholds.max_delay_ms` | `Option<u64>` | `min_delay_ms..=10000` | Upper cap in milliseconds (hard-capped at `10_000 ms` to avoid socket exhaustion). |
| `thresholds.block_score` | `Option<u8>` | `1..=100` or `null` | Optional immediate `403` threshold; `null` keeps route strictly delay-only. |
| `escalation` | `Option<Object>` | object or `null` | Enables strict 64B L1 448-bit + L2 `(client_ip, ja4)` repeat-offender delay bitshift escalation. |
| `escalation.window_secs` | `u64` | `1..=3600` | TTL window (in seconds) for tracking strikes per `(client_ip, ja4)`. |
| `escalation.decay_secs` | `u64` | `1..=window_secs` | Elapsed seconds per 1-step decay on stored strike count. |
| `escalation.max_entries` | `usize` | `256..=1048576` | Fixed L2 CLOCK slot capacity allocated once at boot across 64 shards. |
| `escalation.strike_block_threshold` | `Option<u8>` | `1..=15` or `null` | Optional repeat strike count elevating mitigation to `403`, or `null` for delay-only. |
| `rules` | `Vec<BotRuleConfig>` | non-empty if `preset == "none"` | Custom rules (`id`, `score` `1..=100`, `when` with all 26 keys). Same `id` overrides preset rule. |

---

## 2. Uniform `BotRuleWhen` Catalog (26 Mandatory Keys)

Every rule in `rules` must specify all 26 keys inside `when` (`null` = unconstrained):

| Group | Keys (`null` if unused) | Accepted Values |
| :--- | :--- | :--- |
| **Transport / L7** | `is_tls`, `http_version_in`, `method_in` | `bool`; `["http/1.1", "h2", "h3", "other"]`; `["GET", "POST", ...]` |
| **TLS Handshake** | `tls_version_in`, `tls_alpn_in`, `tls_alpn_not`, `tls_has_sni`, `tls_has_grease`, `tls_has_cert_compress`, `tls_has_alps`, `tls_cipher_bucket_in`, `tls_ext_bucket_in` | `["none", "legacy", "tls1.2", "tls1.3"]`; `["none", "http/1.1", "h2", "h3", "other"]`; `bool`; cipher/ext buckets `0..=3` |
| **Wire Framing** | `wire_pseudo_order_in`, `wire_pseudo_order_not_in`, `wire_odd_settings`, `wire_no_rfc7540_priorities`, `wire_init_window_in`, `wire_conn_window_in` | `["masp", "mpas", "mspa", "msap", "maps", "other"]`; `bool`; `["absent", "65535", "128k", "1m", "2m", "4m", "6m", "16m", "odd"]`; `["absent", "small", "firefox", "chrome", "large", "odd"]` |
| **User-Agent** | `ua_family_in`, `ua_family_not_in`, `ua_min_major`, `ua_max_major` | `UaFamily` names (`"chrome"`, `"firefox"`, `"curl"`, `"go-http-client"`, ...); `u16` |
| **Headers & JA4** | `headers_present`, `headers_absent`, `ja4_in`, `ja4_not_in` | HTTP header names (at most `64` unique headers across entire ruleset); 36-char JA4 or `"0x..."` |

---

## 3. Configuration Recipes

### Recipe A: Delay-Only Canary Rollout (Zero False-Positive 403s)
Use `mode: "enforce"` with `"block_score": null` and `"strike_block_threshold": null`: suspected spoofed bots experience `150ms -> 300ms -> 600ms -> 1200ms -> 2000ms` progressive latency while no legitimate client ever receives a `403`.

### Recipe B: Custom Strict Rule Template (Explicit `null`s)
```json
{
  "id": "bot_detection",
  "config": {
    "mode": "enforce",
    "preset": "balanced_browser",
    "thresholds": {
      "delay_score": 40,
      "min_delay_ms": 100,
      "max_delay_ms": 2000,
      "block_score": 90
    },
    "escalation": {
      "window_secs": 60,
      "decay_secs": 15,
      "max_entries": 65536,
      "strike_block_threshold": 6
    },
    "rules": [
      {
        "id": "custom_safari_wrong_pseudo",
        "score": 40,
        "when": {
          "is_tls": true,
          "http_version_in": ["h2"],
          "method_in": null,
          "tls_version_in": ["tls1.3"],
          "tls_alpn_in": null,
          "tls_alpn_not": null,
          "tls_has_sni": true,
          "tls_has_grease": null,
          "tls_has_cert_compress": null,
          "tls_has_alps": null,
          "tls_cipher_bucket_in": null,
          "tls_ext_bucket_in": null,
          "wire_pseudo_order_in": null,
          "wire_pseudo_order_not_in": ["mspa"],
          "wire_odd_settings": null,
          "wire_no_rfc7540_priorities": null,
          "wire_init_window_in": null,
          "wire_conn_window_in": null,
          "ua_family_in": ["safari"],
          "ua_family_not_in": null,
          "ua_min_major": 16,
          "ua_max_major": null,
          "headers_present": null,
          "headers_absent": null,
          "ja4_in": null,
          "ja4_not_in": null
        }
      }
    ]
  }
}
```

---

## 4. Observability Signals & Incident Triage

| Signal / Symptom | Cause | Action |
| :--- | :--- | :--- |
| **HTTP `403 Forbidden`** | Request hit `thresholds.block_score` or `escalation.strike_block_threshold` in `enforce` mode. | Switch to `"mode": "monitor"` or set `"block_score": null` to isolate false positives while preserving progressive tarpit delay. |
| **Validation error: `missing mandatory field ...`** | A rule omitted one of the 26 `when` keys or a required `thresholds`/`escalation` key. | Add the missing key explicitly with value `null`. |
| **Validation error: `more than 64 unique headers`** | Total unique header names across `headers_present` + `headers_absent` exceeded the 64-bit bitmap limit. | Consolidate rules to the highest-signal browser headers (`sec-fetch-site`, `sec-fetch-mode`, `accept-language`, etc.). |
| **High open socket count during bot flood** | Large `max_delay_ms` holding bot sockets open across delayed Tokio tasks. | Ensure `ja4_filter` precedes `bot_detection` so known bots drop at `403`, and reduce `max_delay_ms` / enable `block_score`. |
