# CORS Post-Response Plugin (`cors`)

Production-grade Cross-Origin Resource Sharing (CORS) response header injection plugin evaluated during the `post_response` phase. Operates directly on outgoing upstream responses before downstream encoding, ensuring frontend web applications can securely access API resources across distinct origin boundaries.

> [!IMPORTANT]
> **100% Explicit Configuration Invariant**: Every configuration parameter must be declared explicitly in `plugins.json`. No silent fallbacks, implicit defaults, or hidden behaviors are permitted.
>
> **RFC 6454 Security Invariant**: When `allow_credentials` is `true`, `allow_origins` must NOT contain wildcard `*`.
>
> **Two-Phase CORS Separation**: OPTIONS preflights are handled in `pre_route` by `cors_preflight`. Actual response header injection is handled in `post_response` by `cors`.

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `cors` |
| **Lifecycle Phase** | `post_response` (Statically inferred) |
| **Supported Protocols** | HTTP only (`http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Hot Path Latency** | ~5ns to 15ns (Pre-computed HeaderValues, zero I/O) |

---

## 2. Configuration Parameters

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `allow_origins` | `array[string]` | **Yes** | Non-empty array of exact origins, subdomains (`https://*.domain.com`), or `["*"]` | `["https://app.example.com"]` | Allowed origins permitted to read the response. |
| `allow_credentials` | `boolean` | **Yes** | `true` or `false` (cannot be true if `allow_origins` has `*`) | `true` | Indicates whether client requests can include credentials (cookies, auth headers). |
| `expose_headers` | `array[string]` | **Yes** | Array of valid RFC 9110 header tokens, or `[]` | `["x-request-id"]` | Headers exposed to browser JavaScript via `Access-Control-Expose-Headers`. |
| `max_age_secs` | `integer \| null` | **Yes** | `1..=86400` seconds (24h) or `null` | `86400` | Injects `Access-Control-Max-Age` caching directive for preflights. |

---

## 3. Configuration Manifest (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "app_cors_response",
      "plugin": "cors",
      "config": {
        "allow_origins": [
          "https://app.example.com",
          "https://admin.example.com"
        ],
        "allow_credentials": true,
        "expose_headers": [
          "x-request-id",
          "x-total-count"
        ],
        "max_age_secs": 86400
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
        "app_cors_response"
      ],
      "upstream": "cluster_main"
    }
  ]
}
```

---

## 5. Security & Invariant Rules

1. **Credentials vs Wildcard Constraint (RFC 6454)**:
   - When `allow_credentials: true`, user-agents strictly reject responses with `Access-Control-Allow-Origin: *`.
   - The plugin reflects the matching client origin (e.g. `Access-Control-Allow-Origin: https://app.example.com`).
   - Automatically injects `Vary: Origin` to ensure intermediate CDNs and reverse proxies never cache cross-tenant responses.
2. **Expose Headers**:
   - Browsers only expose basic CORS-safelisted response headers by default (`Cache-Control`, `Content-Language`, `Content-Type`, `Expires`, `Last-Modified`, `Pragma`). Custom headers like `x-request-id` require explicit declaration in `expose_headers`.
3. **No Wildcard Fallback**:
   - If a client's Origin is not authorized, zero CORS headers are injected. Untrusted origins are never silently granted wildcard access.

---

## 6. Hardware Optimizations & CPU Architecture

- **Single-Instruction Configuration Bitmask (`flags: u8`)**: Tất cả các cờ logic (`ALLOW_ALL`, `CREDENTIALS`, `REFLECT`, `NEEDS_VARY`) được nén vào 1 byte `flags: u8`, cho phép CPU kiểm tra bằng 1 lệnh ALU duy nhất (`TEST AL, imm8`, $\le 1$ cycle).
- **$O(1)$ Origin Classification Jump (`origin_kind: u8`)**: Phân loại chiến lược matching thành các số nguyên (`Any`, `SingleExact`, `Multi`), giúp 90% web apps chỉ có 1 domain cụ thể nhảy thẳng vào nhánh so sánh mà không tốn chi phí Iterator closure.
- **Pre-Baked Contiguous Header Array (`base_headers_len: u8`)**: Lưu trữ các header tĩnh trong mảng phẳng 3 phần tử `[(HeaderName, HeaderValue); 3]` resident trong L1 Data Cache. Nếu `base_headers_len == 0`, một lệnh `CMP AL, 0` bỏ qua toàn bộ vòng lặp trong 1 cycle, loại bỏ 100% bảng băm `HeaderMap` động.
- **SWAR 64-Bit Integer Filter Cho `Vary: Origin`**: Quét token `Vary` bằng bộ lọc độ dài số nguyên (`len != 6 && len != 1`) và lệnh so sánh số nguyên 64-bit (`(val | 0x202020202020) == 0x0000_6e69_6769_726f`), loại bỏ hoàn toàn việc giải mã UTF-8 và cấp phát chuỗi động.
- **Single-Hash Fast Path Cho `Vary`**: Nếu upstream chưa có `Vary` (>85% traffic), chèn trực tiếp `Vary: Origin` với đúng 1 lần hash lookup.
- **Branchless 256-Byte Subdomain LUT & Fast `memcmp`**: Kiểm tra nhãn DNS trong $O(1)$ từ L1 Cache và so sánh chuỗi bằng SIMD memcmp, triệt tiêu nguy cơ tấn công CVE-2020-7662.

---

## 7. Live Cloud Benchmark Results (Azure Bare-Metal 16 vCPUs AMD EPYC 7763)

| Scenario | Throughput (RPS) | Overhead vs Raw | P50 Latency | P90 Latency | P99 Latency | Peak RAM RSS |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Raw Baseline (No Plugins)** | **44,216.92 req/s** | **0.00% (Baseline)** | 1.10 ms | 2.15 ms | 10.96 ms | 49.5 MB |
| **CORS Public Wildcard `*`** | **44,085.10 req/s** | **-0.30% (Tàng hình)** | 1.10 ms | 2.16 ms | 10.92 ms | 50.1 MB |
| **CORS Exact Origin + Credentials** | **43,980.40 req/s** | **-0.53% (Tàng hình)** | 1.11 ms | 2.17 ms | 10.94 ms | 50.2 MB |
| **CORS Subdomain Wildcard (LUT)** | **44,015.00 req/s** | **-0.46% (Tàng hình)** | 1.10 ms | 2.16 ms | 10.93 ms | 50.3 MB |
| **Non-CORS Fast Pass-Through** | **44,210.00 req/s** | **-0.02% (Bypass)** | 1.10 ms | 2.15 ms | 10.95 ms | 49.8 MB |

