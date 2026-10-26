# `plugins.json` — Edge Plugins Specification

The `plugins.json` file configures the edge extensibility catalog. Plugins in Velda Edge are organized into four explicit execution phases following the request-response lifecycle.

---

## 1. Architectural Mindset & Phased Workflow

```
[Downstream Client]
        │
   (TCP / TLS / QUIC)
        │
   (L7 Framing Decode)
        │
   ▼ [1. pre_route phase] (Declared in listeners.json)
        ├── ip_filter (CIDR allow/deny)
        ├── resource_limit (Connection / body guards)
        └── waf (Early pattern inspection)
        │
   ▼ [Router Match] (Razor-thin in-memory waist)
        ├── Path Trie / Host / Method resolution
        └── Route selected
        │
   ▼ [2. pre_upstream phase] (Declared in routes.json -> plugins.pre_upstream)
        ├── jwt_auth (Cryptographic auth & claim propagation)
        ├── rate_limit (Token bucket throttling)
        └── header_mutation (Upstream headers)
        │
   ▼ [Upstream Dispatch]
        ├── Connection lease from Pool
        └── Forward request & await response
        │
   ▼ [3. post_response phase] (Declared in routes.json -> plugins.post_response)
        ├── security_headers (HSTS, CSP, FrameOptions)
        ├── cors (Cross-Origin Resource Sharing)
        └── access_log (Latency & telemetry)
        │
   (L7 Framing Encode)
        │
   (Downstream Send)
        │
[Downstream Client]

[Auxiliary Background Daemon] (Declared in plugins.json)
   └── prometheus_exporter (Scrapes lock-free metric atomics on dedicated port)
```

### 1.1 Declaration Order Invariant (Zero Magic Priorities)
Plugin execution order within each phase is determined **strictly by the declaration sequence** in the JSON array (index `0` runs first, index `N` runs last). No arbitrary `priority` numbers are required.

---

## 2. Schema Definition & Top-Level Fields

```json
{
  "schema_version": 1,
  "plugins": [ /* array of Plugin objects */ ]
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `schema_version` | `integer` | Yes | Schema version. Must be `1`. |
| `plugins` | `array` | Yes | List of global plugin definitions. Duplicate `id` is rejected. |

---

## 3. Plugin Object Specification

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `id` | `string` | Yes | Unique identifier for the plugin (e.g., `"jwt_auth"`, `"rate_limit"`). Referenced by routes or listeners. |
| `phase` | `string` | No | Execution phase: `"pre_route"`, `"pre_upstream"`, `"post_response"`, or `"background"`. Automatically inferred from `id` if omitted. |
| `config` | `object` | Yes | Plugin-specific typed configuration object. |

---

## 4. Plugin Catalog & Configuration Specifications

### 4.1 `jwt_auth` (Phase: `pre_upstream`)
High-performance, zero-allocation cryptographic JWT validation with claim propagation and lock-striped caching.

```json
{
  "id": "jwt_auth",
  "config": {
    "algorithm": "RS256",
    "public_key_pem": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n",
    "token_sources": [
      { "type": "header", "name": "authorization", "prefix": "Bearer " },
      { "type": "cookie", "name": "access_token" }
    ],
    "validate_exp": true,
    "clock_skew_secs": 60,
    "issuer": "https://identity.internal.net",
    "audience": "api.velda.io",
    "claims_to_headers": {
      "sub": "x-user-id",
      "email": "x-user-email",
      "roles": "x-user-roles"
    },
    "assert_claims": {
      "iss": "https://identity.internal.net"
    },
    "cache_shards": 64,
    "cache_shard_capacity": 2048,
    "cache_ttl_secs": 300,
    "negative_cache_ttl_secs": 10
  }
}
```

- `algorithm`: Cryptographic signing algorithm: `"HS256"`, `"RS256"`, `"ES256"`, or `"EdDSA"`.
- `public_key_pem`: Standard PEM-encoded RSA/ECDSA/Ed25519 public key (SPKI or PKCS#1).
- `secret_key`: Shared secret string (required for HMAC `"HS256"`).
- `token_sources`: Array of extraction locations evaluated in order:
  - `{ "type": "header", "name": "authorization", "prefix": "Bearer " }`
  - `{ "type": "cookie", "name": "access_token" }`
  - `{ "type": "query", "param": "token" }`
- `validate_exp`: Boolean flag. If `true`, enforces standard expiration (`exp`) check.
- `clock_skew_secs`: Tolerated clock drift in seconds (default `60`).
- `issuer`: Expected `iss` claim. Rejects token if mismatched.
- `audience`: Expected `aud` claim. Rejects token if mismatched.
- `claims_to_headers`: Map of custom/standard payload claim keys $\to$ upstream request header names injected into the request.
- `assert_claims`: Map of custom claim keys $\to$ expected exact string values.
- `cache_shards`: Number of lock-striped shards for the token cache (default `64`).
- `cache_shard_capacity`: Capacity per shard (total cache size = `cache_shards * cache_shard_capacity`).
- `cache_ttl_secs`: Positive cache TTL for valid signatures (~20ns hit time).
- `negative_cache_ttl_secs`: Negative cache TTL to protect against signature verification CPU exhaustion during invalid token DoS attacks.

---

### 4.2 `rate_limit` (Phase: `pre_upstream`)
In-memory or Redis-backed token bucket rate limiter.

```json
{
  "id": "rate_limit",
  "config": {
    "algorithm": "token_bucket",
    "backend": "memory",
    "rate_per_second": 1000,
    "burst_capacity": 2000,
    "key_strategy": "priority",
    "key_headers": ["x-api-key", "x-user-id", "client_ip"],
    "fallback_behavior": "memory"
  }
}
```

- `algorithm`: Rate limiting algorithm (`"token_bucket"`).
- `backend`: Storage backend (`"memory"` or `"redis"`).
- `rate_per_second`: Sustained token replenishment rate per second.
- `burst_capacity`: Maximum burst token bucket capacity.
- `key_strategy`: Key extraction method:
  - `"client_ip"`: Throttles per client IP address.
  - `"header"`: Throttles per specified header value.
  - `"priority"`: Checks `key_headers` in order, using the first available header, falling back to IP.
- `key_headers`: Header names evaluated for key extraction.
- `fallback_behavior`: Action if external backend is unreachable (`"memory"`, `"allow"`, or `"reject"`).

---

### 4.3 `ip_filter` (Phase: `pre_route`)
CIDR-based network access control evaluated before routing.

```json
{
  "id": "ip_filter",
  "config": {
    "mode": "allow",
    "allow_cidrs": ["10.0.0.0/8", "192.168.1.0/24", "2001:db8::/32"],
    "deny_cidrs": [],
    "client_ip_source": "x_forwarded_for"
  }
}
```

- `mode`: `"allow"` (whitelist mode: only listed CIDRs allowed) or `"deny"` (blacklist mode).
- `allow_cidrs`: List of allowed IPv4/IPv6 CIDR subnets.
- `deny_cidrs`: List of blocked IPv4/IPv6 CIDR subnets.
- `client_ip_source`: `"direct"` (remote socket IP), `"x_forwarded_for"` (XFF header), or `"x_real_ip"`.

---

### 4.4 `security_headers` (Phase: `post_response`)
Appends defensive HTTP security headers to responses before downstream delivery.

```json
{
  "id": "security_headers",
  "config": {
    "hsts": {
      "enable": true,
      "max_age_secs": 31536000,
      "include_subdomains": true,
      "preload": false
    },
    "frame_options": "DENY",
    "content_type_nosniff": true,
    "referrer_policy": "strict-origin-when-cross-origin"
  }
}
```

- `hsts.enable`: Emits `Strict-Transport-Security` header.
- `hsts.max_age_secs`: Cache lifetime in seconds (e.g., 1 year = `31536000`).
- `hsts.include_subdomains`: Appends `includeSubDomains` directive.
- `hsts.preload`: Appends `preload` directive.
- `frame_options`: `X-Frame-Options` value (`"DENY"`, `"SAMEORIGIN"`).
- `content_type_nosniff`: Emits `X-Content-Type-Options: nosniff`.
- `referrer_policy`: `Referrer-Policy` header value.

---

### 4.5 `cors` (Phase: `post_response`)
Cross-Origin Resource Sharing preflight handling and response header injection.

```json
{
  "id": "cors",
  "config": {
    "allow_origins": ["https://app.velda.io", "https://console.velda.io"],
    "allow_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "allow_headers": ["authorization", "content-type", "x-requested-with", "x-api-key"],
    "expose_headers": ["x-request-id"],
    "allow_credentials": true,
    "max_age_secs": 86400
  }
}
```

- `allow_origins`: Array of allowed origin URLs or `["*"]`.
- `allow_methods`: Array of permitted HTTP methods.
- `allow_headers`: Permitted request headers.
- `expose_headers`: Headers exposed to browser JavaScript.
- `allow_credentials`: Sets `Access-Control-Allow-Credentials: true`.
- `max_age_secs`: Preflight cache duration in seconds (`Access-Control-Max-Age`).

---

### 4.6 `header_mutation` (Phase: `pre_upstream`)
Adds, replaces, renames, appends, or strips HTTP request headers before upstream dispatch.

```json
{
  "id": "header_mutation",
  "config": {
    "strip_hop_by_hop": true,
    "remove_headers": ["x-powered-by", "server"],
    "rename_headers": {
      "x-client-device": "x-device-type"
    },
    "set_headers": {
      "x-gateway-proxy": "velda-edge",
      "x-forwarded-proto": "$scheme",
      "x-real-ip": "$client_ip"
    },
    "append_headers": {
      "x-forwarded-for": "$client_ip"
    }
  }
}
```

- `strip_hop_by_hop`: Strip RFC 9110 / RFC 7230 connection-level headers (`Connection`, `Keep-Alive`, `Upgrade`, etc.) while protecting critical headers (`Host`, `Authorization`, `X-Real-IP`) from strip abuse (CVE-2022-26143).
- `remove_headers`: Array of header names to strip. Essential headers (`Host`, `Content-Length`, `Transfer-Encoding`, `X-Request-Id`, and pseudo-headers `:`) cannot be removed.
- `rename_headers`: Key-value map of source header $\to$ destination header with anti-shadowing protection.
- `set_headers`: Map of header name $\to$ value to set or overwrite. Framing and hop-by-hop headers cannot be overwritten.
- `append_headers`: Map of header name $\to$ value to append.
- **Dynamic Variables**: `$client_ip`, `$request_id`, `$scheme`, `$host`, `$method`, `$path` (use `$$` for a literal `$`).

---

### 4.7 `response_header_mutation` (Phase: `post_response`)
Adds, replaces, appends, or strips HTTP headers on downstream responses.

```json
{
  "id": "response_header_mutation",
  "config": {
    "remove_headers": ["server", "x-powered-by"],
    "set_headers": {
      "server": "velda-edge",
      "x-request-id": "$request_id"
    },
    "append_headers": {
      "vary": "accept-encoding"
    },
    "set_if_missing": {
      "x-content-type-options": "nosniff"
    }
  }
}
```

- `remove_headers`: Array of headers to strip from outgoing response.
- `set_headers`: Headers to overwrite or set.
- `append_headers`: Headers to append to (e.g. multi-value `Vary`).
- `set_if_missing`: Headers to insert only if not already emitted by upstream.
- **Dynamic Variables**: `$request_id`, `$status`, `$timestamp_epoch` (use `$$` for literal `$`).

---

### 4.8 `access_log` (Phase: `post_response`)
Controls structured telemetry and access log emissions.

```json
{
  "id": "access_log",
  "config": {
    "include_request_id": true,
    "include_route_id": true,
    "include_upstream": true,
    "include_latency": true
  }
}
```

- `include_request_id`: Emits unique UUID request identifier in access logs.
- `include_route_id`: Includes matched route identifier.
- `include_upstream`: Includes destination upstream and physical endpoint address.
- `include_latency`: Records request duration and upstream latency metrics.

---

### 4.8 `prometheus_exporter` (Phase: `background`)
Auxiliary background daemon that serves OpenMetrics / Prometheus metrics on a dedicated scrape port.

```json
{
  "id": "prometheus_exporter",
  "config": {
    "listen": "0.0.0.0:9090",
    "path": "/metrics",
    "protocol": "http",
    "scrape": {
      "timeout_ms": 5000,
      "max_concurrency": 16,
      "max_buffer_size_kb": 256
    },
    "tls": {
      "mode": "none"
    },
    "cors": {
      "enabled": false
    },
    "auth": {
      "mode": "none"
    }
  }
}
```

- `listen`: Socket address to bind for scraping (e.g. `"0.0.0.0:9090"`).
- `path`: HTTP endpoint path for metrics scraping (default `"/metrics"`).
- `protocol`: Protocol to serve metrics on (`"http"` or `"https"`).
- `scrape`:
  - `timeout_ms`: Request timeout threshold in milliseconds (range: 100ms - 120,000ms).
  - `max_concurrency`: Maximum concurrent scraper connections allowed simultaneously (range: 1 - 1024).
  - `max_buffer_size_kb`: Scratch buffer capacity allocated for OpenMetrics text serialization (range: 8KB - 4096KB).
- `tls`: Strict TLS configuration block. If disabled, `mode` must be explicitly declared as `"none"`.
- `cors`: Strict CORS configuration block. Must declare `enabled: true/false`.
- `auth`: Strict authentication configuration block (`mode: "none"`, `"bearer"`, or `"basic"`).
