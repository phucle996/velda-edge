# Response Compression Plugin (`response_compression`)

High-performance, content-negotiated HTTP response body compression plugin evaluated during the `post_response` phase. Automatically compresses response payloads (Zstandard, Brotli, Gzip, Deflate) based on downstream client capabilities (`Accept-Encoding`), while coordinating seamlessly with in-memory cache zones.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **Exact Request Workspace Borrowing**: Inspects `Accept-Encoding` directly from the borrowed in-flight request headers workspace (`req_headers`), guaranteeing zero cross-request contamination in pipelined and multiplexed connections.
>
> **Never Specify `phase` in User Configuration**: The execution phase is statically inferred by Velda as `post_response`. Execution order strictly follows the route's `plugins` array order (FIFO).

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `response_compression` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability { header: AccessLevel::Write, payload: AccessLevel::Write }` |
| **Compression Codecs** | `zstd` (RFC 8878), `br` (RFC 7932), `gzip` (RFC 1952), `deflate` (RFC 1951) |
| **Standards Compliance** | RFC 9110 § 12.5.3 (Negotiation), § 8.4.1 (Content-Encoding), § 12.5.5 (Vary) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `algorithms` | `array[string]` | **Yes** | Non-empty array of `"zstd"`, `"brotli"` / `"br"`, `"gzip"`, `"deflate"` | `["zstd", "br", "gzip"]` | Enabled compression algorithms in server preference order. |
| `min_length_bytes` | `integer` | **Yes** | `64..=10485760` (64 bytes to 10MB) | `256` | Minimum response body byte length to trigger compression. Payloads below this are skipped. |
| `gzip_level` | `integer` | **Yes** | `1..=9` | `1` | Gzip/Deflate compression level (Level 1 Pareto sweet-spot with SIMD acceleration). |
| `brotli_level` | `integer` | **Yes** | `0..=11` | `2` | Brotli compression quality (Level 2 sub-millisecond edge latency). |
| `zstd_level` | `integer` | **Yes** | `1..=22` | `1` | Zstandard compression level (Level 1 ~800MB/s per core). |
| `zstd_dict` | `string \| null` | **Yes** | Base64-encoded dictionary bytes or `null` | `null` | Optional pre-trained Zstandard dictionary (RFC 8878) for high-ratio structured JSON compression. |
| `content_types` | `array[string]` | **Yes** | Non-empty array of MIME type prefixes | `["text/", "application/json", "application/javascript", "image/svg+xml"]` | MIME content-type prefixes eligible for compression. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "edge_compression",
      "plugin": "response_compression",
      "config": {
        "algorithms": [
          "zstd",
          "br",
          "gzip"
        ],
        "min_length_bytes": 256,
        "gzip_level": 1,
        "brotli_level": 2,
        "zstd_level": 1,
        "zstd_dict": null,
        "content_types": [
          "text/",
          "application/json",
          "application/javascript",
          "image/svg+xml"
        ]
      }
    }
  ]
}
```

---

## 4. Route Attachment (`routes.json`)

Attach the plugin to an HTTP route via its unique identifier in the `plugins` array:

```json
{
  "routes": [
    {
      "id": "route_api",
      "match": {
        "path": {
          "prefix": "/api/"
        }
      },
      "plugins": [
        "edge_cache_store",
        "edge_compression"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Smart Variant Matching (RFC 9110 § 12.5.3)

When coordinated with `http_cache_lookup`, the edge gateway implements **Smart Variant Matching** across client preferences and in-memory cache variants:

```
                  Client sends: Accept-Encoding (Multiple algorithms)
                                       │
                                       ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │ TIER 1: "HIT OVER MISS" PRINCIPLE (Zero-IO Bias - Bypass Upstream)        │
 │ • Find intersection: [Client Accepted] ∩ [Existing Variants in RAM Cache] │
 │ • If intersection has >= 1 variant ──► INSTANT ZERO-CPU CACHE HIT!        │
 └───────────────────────────────────────────────────────────────────────────┘
                                       │ (If 2+ variants match)
                                       ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │ TIER 2: CLIENT QUALITY FACTOR (Client q-factor)                           │
 │ • Choose variant with highest `q` weight (e.g. `br;q=1.0, gzip;q=0.8`)     │
 └───────────────────────────────────────────────────────────────────────────┘
                                       │ (If q weights are equal)
                                       ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │ TIER 3: SERVER CODEC PREFERENCE ORDER                                      │
 │                 zstd  >  br  >  gzip  >  identity                         │
 └───────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Invariants & Safeguards

1. **Zero Double-Compression**: If upstream or cache already emitted a `Content-Encoding` header, compression is bypassed unconditionally.
2. **Status Code Safeguard**: Responses with status `204 No Content`, `304 Not Modified`, or `1xx Informational` are never compressed.
3. **Inflation Safeguard**: If the compressed payload size is equal to or larger than the raw uncompressed body, the uncompressed body is retained.
4. **Cache Poisoning Prevention**: Automatically injects or appends `Vary: Accept-Encoding` (RFC 9110 § 12.5.5) on all compressed responses.
