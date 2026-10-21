# Response Compression Plugin (`response_compression`)

High-performance, content-negotiated HTTP response body compression plugin evaluated during the `post_response` phase. Automatically compresses response payloads (Zstandard, Brotli, Gzip, Deflate) based on downstream client capabilities (`Accept-Encoding`), while coordinating seamlessly with in-memory cache zones.

> [!IMPORTANT]
> **Pristine Cache Coordination & Dual-Mode Pipeline Strategies**:
> - **Eager Pipeline (`[http_cache_store, response_compression]`)**: Cache zones store canonical uncompressed payloads alongside pre-computed compressed variants (`gzip`, `br`, `zstd`). On Cache Hit, pre-compressed variants are served with zero CPU overhead, and `response_compression` immediately bypasses execution.
> - **Lazy Pipeline (`[response_compression, http_cache_store]`)**: `response_compression` compresses payloads dynamically on demand. `http_cache_store` follows behind to cache the specific compressed variant into RAM, enabling demand-driven lazy caching with minimal memory footprint.
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
| **Reverse Capability** | `PluginCapability { header: AccessLevel::Write, payload: AccessLevel::Write }` |
| **Compression Codecs** | `zstd` (RFC 8878), `br` (RFC 7932), `gzip` (RFC 1952), `deflate` (RFC 1951) |
| **Standards Compliance** | RFC 9110 § 12.5.3 (Negotiation), § 8.4.1 (Content-Encoding), § 12.5.5 (Vary) |

---

## 2. Configuration Schema

```json
{
  "id": "response_compression",
  "config": {
    "algorithms": ["zstd", "br", "gzip", "deflate"],
    "min_length": 256,
    "gzip_level": 1,
    "brotli_level": 2,
    "zstd_level": 1,
    "zstd_dict_base64": null,
    "content_types": [
      "application/json",
      "text/plain",
      "text/html",
      "text/css",
      "application/javascript",
      "text/javascript",
      "application/xml",
      "text/xml",
      "application/graphql+json",
      "image/svg+xml"
    ]
  }
}
```

### Parameter Reference

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `algorithms` | `string[]` | `[]` (Required) | Enabled compression algorithms in server preference order. Must be explicitly declared; if omitted/empty, compression is disabled. |
| `min_length` | `integer` | `256` | Minimum uncompressed byte threshold. Payloads below this threshold are skipped to prevent framing inflation. |
| `gzip_level` | `integer` | `1` | Gzip/Deflate compression level (1-9). Defaults to Pareto Level 1 (~3x faster than L6 with SIMD acceleration via `zlib-rs`). |
| `brotli_level` | `integer` | `2` | Brotli compression quality (0-11). Defaults to Pareto Level 2 (~4x faster than L4, sub-millisecond edge latency). |
| `zstd_level` | `integer` | `1` | Zstandard compression level (1-22). Defaults to Pareto Level 1 (ultra-fast throughput ~800MB/s per core). |
| `zstd_dict_base64` | `string \| null` | `null` | Optional pre-trained Zstandard dictionary (RFC 8878) base64-encoded for structured JSON API compression. |
| `content_types`| `string[]` | `[]` (Required) | MIME type prefixes eligible for compression. Must be explicitly declared; if omitted/empty or unmatched, responses return uncompressed. |

---

## 3. Smart Variant Matching (RFC 9110 § 12.5.3)

When coordinated with `http_cache_lookup`, the edge gateway implements **Smart Variant Matching** across client preferences and in-memory cache variants:

```
                  Client gửi: Accept-Encoding (Nhiều thuật toán)
                                       │
                                       ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │ CẤP 1: NGUYÊN TẮC "HIT TRÊN HẾT" (The "Zero-IO" Bias - Tránh Upstream)     │
 │ • Tìm giao điểm: [Client chấp nhận] ∩ [Các biến thể ĐANG CÓ trong RAM]    │
 │ • Nếu giao điểm CÓ ÍT NHẤT 1 BIẾN THỂ ──► CHỐT LUÔN CACHE HIT!            │
 └───────────────────────────────────────────────────────────────────────────┘
                                       │ (Nếu có từ 2 biến thể trở lên)
                                       ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │ CẤP 2: TRỌNG SỐ ƯU TIÊN CỦA CLIENT (Client q-factor)                      │
 │ • Ưu tiên biến thể có trọng số `q` cao nhất (ví dụ: `br;q=1.0, gzip;q=0.8`)│
 └───────────────────────────────────────────────────────────────────────────┘
                                       │ (Nếu các `q` bằng nhau hoặc không khai báo)
                                       ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │ CẤP 3: THỨ TỰ PHÂN XỬ CỦA GATEWAY (Server Codec Hierarchy)                │
 │                 zstd  >  br  >  gzip  >  identity                         │
 └───────────────────────────────────────────────────────────────────────────┘
```

1. **Zero-IO / Hit Over Miss**: If the client advertises that it accepts multiple codecs (e.g., `Accept-Encoding: gzip, deflate, br, zstd`) and the cache zone currently holds *any* of those variants (e.g., only `br` is populated), the gateway immediately serves the available cached variant as a **Zero-CPU Cache HIT (103k+ RPS)** without triggering an unnecessary upstream roundtrip.
2. **Upstream Mitigation**: Even under Lazy demand-driven caching, 95%+ of modern web browsers accept both `zstd` and `br`, meaning that the first client's variant automatically satisfies subsequent clients without upstream multiplications.

---

## 4. Invariants & Safeguards

1. **Explicit Declaration Invariant**: `algorithms` and `content_types` **must be explicitly declared** in plugin config. If omitted or empty (`"config": {}`), the server does not support compression and returns uncompressed payloads.
2. **Zero Double-Compression**: If upstream or cache already emitted a `Content-Encoding` header, compression is bypassed unconditionally.
3. **Status Code Safeguard**: Responses with status `204 No Content`, `304 Not Modified`, or `1xx Informational` are never compressed.
4. **Inflation Safeguard**: If the compressed payload size is equal to or larger than the raw uncompressed body, the uncompressed body is retained.
5. **Cache Poisoning Prevention**: Automatically injects or appends `Vary: Accept-Encoding` (RFC 9110 § 12.5.5) on all compressed responses.
