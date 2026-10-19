# Response Compression Plugin (`response_compression`)

High-performance, content-negotiated HTTP response body compression plugin evaluated during the `post_response` phase. Automatically compresses response payloads (Zstandard, Brotli, Gzip, Deflate) based on downstream client capabilities (`Accept-Encoding`), while coordinating seamlessly with in-memory cache zones.

> [!IMPORTANT]
> **Pristine Cache Coordination**: When used in conjunction with `http_cache_store`, cache zones always store canonical **uncompressed** payloads. Compression occurs downstream at the exit gate, eliminating cache fragmentation across clients with different codec support.
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
    "gzip_level": 6,
    "brotli_level": 4,
    "zstd_level": 3,
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
| `gzip_level` | `integer` | `6` | Gzip/Deflate compression level (1-9). |
| `brotli_level` | `integer` | `4` | Brotli compression quality (0-11). Level 4 is recommended for edge performance. |
| `zstd_level` | `integer` | `3` | Zstandard compression level (1-22). Level 3 offers optimal speed/ratio balance. |
| `content_types`| `string[]` | `[]` (Required) | MIME type prefixes eligible for compression. Must be explicitly declared; if omitted/empty or unmatched, responses return uncompressed. |

---

## 3. Invariants & Safeguards

1. **Explicit Declaration Invariant**: `algorithms` and `content_types` **must be explicitly declared** in plugin config. If omitted or empty (`"config": {}`), the server does not support compression and returns uncompressed payloads.
2. **Zero Double-Compression**: If upstream or cache already emitted a `Content-Encoding` header, compression is bypassed unconditionally.
3. **Status Code Safeguard**: Responses with status `204 No Content`, `304 Not Modified`, or `1xx Informational` are never compressed.
4. **Inflation Safeguard**: If the compressed payload size is equal to or larger than the raw uncompressed body, the uncompressed body is retained.
5. **Cache Poisoning Prevention**: Automatically injects or appends `Vary: Accept-Encoding` (RFC 9110 § 12.5.5) on all compressed responses.
