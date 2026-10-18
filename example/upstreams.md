# `upstreams.json` — Backend Upstreams Specification

The `upstreams.json` file configures the bottom cone of the Hourglass Architecture. It defines logical backend target pools, dynamic discovery resolution, endpoint topologies, load balancing strategies, connection reuse pools, active/passive health checks, and upstream TLS transport.

---

## 1. Architectural Mindset & Subsystem Boundary

### 1.1 The Canonical `Endpoint` Invariant
The term `Endpoint` (`velda_core::Endpoint`) is strictly reserved across the codebase to denote a **physical backend destination target** (`address: SocketAddr`, `weight: u32`). It is never used for ingress listeners or API routes.

### 1.2 Upstream vs Protocol Subsystems (Lifecycle vs Mechanics)
- **Upstream Subsystem (`velda-upstream`, `velda-connection-pool`, `velda-lb`)**:
  - Owns: Target physical endpoint selection (**WHO**), connection reuse lifecycle (**WHEN**), passive/active health monitoring, circuit breaker state, idle connection eviction, and connection lease handles.
  - Zero socket syscalls, zero setsockopt, zero TLS handshakes, zero wire frame decoding.
- **Protocol Subsystems (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`, `velda-edge::tcp`)**:
  - Owns: Raw socket instantiation, kernel socket flags (`TCP_NODELAY`, `TCP_FASTOPEN`), TLS handshakes, ALPN negotiation, and wire protocol framing (**HOW**).

---

## 2. Schema Definition & Top-Level Fields

```json
{
  "schema_version": 1,
  "upstreams": [ /* array of Upstream objects */ ]
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `schema_version` | `integer` | Yes | Schema version. Must be `1`. |
| `upstreams` | `array` | Yes | Array of upstream definitions. Duplicate `id` is rejected. |

---

## 3. Upstream Object Specification

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `id` | `string` | Yes | Unique identifier for this upstream pool (e.g., `"users"`, `"grpc-users"`). Referenced by routes via `route.upstream`. |
| `mode` | `string` | Yes | Discovery mode: `"dns"` or `"endpoints"`. |
| `protocol` | `object` | Yes | Upstream transport protocol, application framing, and streaming capabilities. |
| `target` | `object` | Conditional | DNS target (`host`, `port`). Required when `mode == "dns"`. |
| `resolver` | `object` | No | DNS resolver settings (`mode`, `nameservers`, `timeout_ms`, `retries`). Applicable when `mode == "dns"`. |
| `endpoints` | `array` | Conditional | Static endpoint list. Required when `mode == "endpoints"`. |
| `load_balancer` | `object` | Yes | In-memory load balancing algorithm selection. |
| `timeouts` | `object` | Yes | Connection and request timeout policies. |
| `pool` | `object` | No | Connection pool sizing and idle reclamation tuning. |
| `health_check` | `object` | No | Active probing and passive error-rate circuit breaker policies. |
| `tls` | `object` | No | Upstream TLS / mTLS configuration for encrypted backend connections. |

---

## 4. Sub-Object Specifications

### 4.1 Discovery Mode & Target

#### Mode: `"dns"` (Dynamic DNS Discovery)
Discovers physical endpoints dynamically by resolving hostnames via asynchronous DNS (`velda-discovery`). Resolved endpoints are maintained in-memory with zero disk or runtime RPC overhead on the request hot path.
```json
{
  "mode": "dns",
  "target": {
    "host": "users.default.svc.cluster.local",
    "port": 8080
  },
  "resolver": {
    "mode": "custom",
    "nameservers": ["10.96.0.10:53"],
    "timeout_ms": 200,
    "retries": 2
  }
}
```
- `target.host`: Domain name or Service FQDN to resolve.
- `target.port`: Target destination TCP/UDP port.
- `resolver.mode`: `"system"` (uses `/etc/resolv.conf`) or `"custom"` (uses explicit nameservers).
- `resolver.nameservers`: Array of DNS nameserver socket addresses (`IP:Port`).
- `resolver.timeout_ms`: DNS query timeout in milliseconds.
- `resolver.retries`: Number of query retries upon failure.

#### Mode: `"endpoints"` (Static Endpoint Topologies)
Declares an explicit list of backend endpoints with weights.
```json
{
  "mode": "endpoints",
  "endpoints": [
    { "address": "10.0.1.10:8080", "weight": 100 },
    { "address": "10.0.1.11:8080", "weight": 50 }
  ]
}
```
- `address`: Physical backend target socket address (`IP:Port`).
- `weight`: Integer weight for weighted load balancing (must be $\ge 1$).

---

### 4.2 Protocol (`protocol`)

```json
{
  "transport": "tcp",
  "application": "http1",
  "streaming": ["server"]
}
```
- `transport`: Underlying transport socket type: `"tcp"`, `"udp"`, or `"quic"`.
- `application`: Upstream wire protocol:
  - `"http1"`: HTTP/1.1 with connection keep-alive pooling.
  - `"http2"`: Multiplexed HTTP/2 streams over pooled persistent connections.
  - `"http3"`: HTTP/3 over QUIC datagrams.
  - `"grpc"`: Length-prefixed gRPC over HTTP/2 framing.
  - `"raw"`: Raw L4 TCP/UDP byte forwarding.
- `streaming`: Array of enabled streaming directions:
  - `[]`: Buffered mode (in-memory buffering).
  - `["client"]`: Client request streaming.
  - `["server"]`: Server response streaming (SSE, chunked feeds).
  - `["client", "server"]`: Full bidirectional duplex (WebSocket, gRPC duplex).

---

### 4.3 Load Balancer (`load_balancer`)

All load balancing algorithms in `velda-lb` are zero-allocation on the request serving hot path.
```json
{
  "algorithm": "round_robin"
}
```

| Algorithm | Value | Best Used For |
| :--- | :--- | :--- |
| **Round Robin** | `"round_robin"` | Homogeneous backend servers with equal capacity. |
| **Weighted Round Robin** | `"weighted_round_robin"` (or `"wrr"`) | Heterogeneous servers with differing CPU/memory specifications. |
| **Least Connections** | `"least_connections"` (or `"least_conn"`) | Long-lived requests, heavy computation, or varying request duration. |
| **IP / Hash Affinity** | `"ip_hash"` (or `"hash"`) | Session stickiness, consistent cache affinity. |
| **Power of Two Choices** | `"p2c"` (or `"power_of_two_choices"`) | High-throughput distributed backends avoiding herd behavior. |
| **Random** | `"random"` | Low overhead, stateless distribution across large pools. |

---

### 4.4 Connection Pool (`pool`)

Connection reuse is handled by `velda-connection-pool` using sharded containers to eliminate thread contention.

```json
{
  "concurrency_shards": 4,
  "max_idle_per_key": 32,
  "max_concurrent_streams": 256,
  "idle_timeout_ms": 30000,
  "max_lifetime_ms": 1800000
}
```

- `concurrency_shards`: Number of lock-striped pool shards. Automatically clamped to hardware core count if omitted.
- `max_idle_per_key`: Maximum idle persistent connections maintained per backend destination endpoint.
- `max_concurrent_streams`: Maximum active concurrent streams per connection (HTTP/2, HTTP/3, gRPC).
- `idle_timeout_ms`: Idle duration after which an unused connection is closed and evicted.
- `max_lifetime_ms`: Absolute maximum lifetime of a persistent connection before clean retirement.

---

### 4.5 Health Checks (`health_check`)

Velda Edge supports dual-layer health checking: real-time passive tracking and proactive active probing.

```json
{
  "health_check": {
    "passive": {
      "enabled": true,
      "max_consecutive_failures": 3,
      "cooldown_ms": 10000
    },
    "active": {
      "enabled": true,
      "interval_ms": 5000,
      "timeout_ms": 1000,
      "unhealthy_threshold": 3,
      "healthy_threshold": 2,
      "check_type": "http",
      "path": "/healthz",
      "expected_statuses": [200, 204]
    }
  }
}
```

#### Passive Health Check (`passive`)
- `enabled`: Enables inline circuit breaking on runtime I/O errors.
- `max_consecutive_failures`: Number of consecutive connection/request failures before marking an endpoint unhealthy.
- `cooldown_ms`: Quarantine duration before the endpoint is allowed trial traffic.

#### Active Health Check (`active`)
- `enabled`: Runs proactive background health probes.
- `interval_ms`: Frequency of probes per endpoint.
- `timeout_ms`: Maximum duration to wait for a probe response.
- `unhealthy_threshold`: Consecutive failed probes required to mark an endpoint unhealthy.
- `healthy_threshold`: Consecutive successful probes required to restore an endpoint to healthy state.
- `check_type`: `"http"` (evaluates HTTP response status) or `"tcp"` (verifies socket handshake).
- `path`: HTTP health check URI path (e.g., `"/healthz"`).
- `expected_statuses`: Acceptable HTTP status codes (e.g., `[200, 204]`).

---

### 4.6 Upstream TLS (`tls`)

Configures TLS encryption between Velda Edge and backend services.

```json
{
  "tls": {
    "ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
    "client_cert_pem": null,
    "client_key_pem": null,
    "versions": ["tls1.2", "tls1.3"],
    "alpn": ["h2", "http/1.1"],
    "insecure_skip_verify": false
  }
}
```

- `ca_pem`: Custom Certificate Authority (CA) PEM certificate string for validating upstream backend certificates.
- `client_cert_pem`: Client certificate PEM string for mutual TLS (mTLS).
- `client_key_pem`: Client private key PEM string for mutual TLS (mTLS).
- `versions`: Allowed TLS versions: `["tls1.2", "tls1.3"]`.
- `alpn`: Negotiated ALPN protocol strings (e.g., `["h2"]`, `["http/1.1"]`).
- `insecure_skip_verify`: Set to `true` ONLY in development/lab environments to disable certificate authority validation.
