# `listeners.json` — Ingress Listeners Specification

The `listeners.json` file configures the edge ingress layer (top cone of the Hourglass Architecture). Each listener binds a physical socket (`IP:Port`) and declares transport protocols, application pipelines, TLS termination, protocol tuning parameters, and early `pre_route` edge guards.

---

## 1. Schema Definition & Top-Level Fields

```json
{
  "schema_version": 1,
  "listeners": [ /* array of Listener objects */ ]
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `schema_version` | `integer` | Yes | Schema version. Must be `1`. |
| `listeners` | `array` | Yes | List of listener configurations. Duplicate `id` or `address` is rejected. |

---

## 2. Listener Object Specification

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `id` | `string` | Yes | Unique identifier for this listener (e.g., `"web-in"`, `"grpc-in"`). |
| `address` | `string` | Yes | Socket address to bind (e.g., `"127.0.0.1:443"`, `"[::]:443"`). Must be valid `SocketAddr`. |
| `transport` | `object` | Yes | L4 transport configuration (TCP / UDP). |
| `application` | `object` | Yes | L7 application protocol family and streaming mode. |
| `tls` | `object` | Yes | TLS enablement flag. If `true`, certificates are loaded from `tls.json`. |
| `pre_route` | `array` | No | List of `pre_route` plugins evaluated on incoming requests before route lookup. |
| `http1` | `object` | No | Protocol tuning options for HTTP/1.1 connections. |
| `http2` | `object` | No | Protocol tuning options for HTTP/2 multiplexed connections. |
| `http3` | `object` | No | Protocol tuning options for HTTP/3 over QUIC connections. |
| `grpc` | `object` | No | Protocol tuning options for gRPC RPC connections. |
| `raw` | `object` | No | Protocol tuning options for raw L4 passthrough connections. |

---

## 3. Sub-Object Details

### Transport (`transport`)
```json
{ "protocol": "tcp" } // or "udp"
```
* `protocol`: Transport protocol. Must be `"tcp"` or `"udp"`.
  * HTTP/1.1, HTTP/2, and gRPC strictly require `"tcp"`.
  * HTTP/3 strictly requires `"udp"`.
  * Raw passthrough supports both `"tcp"` and `"udp"`.

### Application (`application`)
```json
{
  "family": "http",
  "streaming": ["server"]
}
```
* `family` (or `protocol`): Canonical application protocol family:
  * `"http"`: Serves HTTP/1.1, HTTP/2, and HTTP/3 Web/REST traffic through `HttpRouter`.
  * `"grpc"`: Dedicated gRPC binary RPC pipeline through `GrpcRouter`.
  * `"raw"`: Direct L4 byte stream forwarding without L7 parsing.
* `streaming`: Array of enabled streaming directions:
  * `[]`: Buffered (in-memory buffering with constant footprint).
  * `["client"]`: Streaming upload (client body passthrough).
  * `["server"]`: Streaming download (SSE, chunked response passthrough).
  * `["client", "server"]`: Full duplex (WebSocket, gRPC bidirectional streaming).

### Early Edge Guards (`pre_route`)
```json
"pre_route": [
  "ip_filter"
]
```
* Plugins declared in `pre_route` execute immediately after downstream frame decoding, **before** route resolution.
* Declaration order determines execution sequence.
* Ideal for IP blacklisting (`ip_filter`), WAF inspection (`waf`), and listener connection limits (`resource_limit`).

### Protocol Tuning Sub-Objects

#### `http1` (HTTP/1.1 Parameters)
* `max_body_size` (`integer`): Enforced for buffered mode (e.g., `10485760` bytes / 10MB).
* `max_header_size` (`integer`): Maximum request header block size (default `65536`).
* `max_headers` (`integer`): Maximum number of request headers (default `64`).
* `idle_timeout_ms` (`integer`): Downstream connection keep-alive timeout in milliseconds.
* `max_keepalive_requests` (`integer`): Maximum requests served per TCP connection before closing.
* `header_read_timeout_ms` (`integer`): Timeout for reading HTTP request headers.

#### `http2` (HTTP/2 Parameters)
* `max_concurrent_streams` (`integer`): Maximum active concurrent streams per HTTP/2 connection (default `256`).
* `max_consecutive_resets` (`integer`): Defense threshold against Rapid Reset attacks (RFC 7540 abuse).
* `max_pending_control_frames` (`integer`): Upper bound for queued control frames (PING, SETTINGS, WINDOW_UPDATE).
* `max_continuation_frames` (`integer`): Maximum CONTINUATION frames allowed per request (defense against Continuation Flood).

#### `grpc` (gRPC Parameters)
* `max_message_size` (`integer`): Maximum size of an individual gRPC length-prefixed message frame (default `8388608` / 8MB).
* `keepalive_ping_interval_ms` (`integer`): Interval for sending HTTP/2 PING frames to verify client liveness.
* `keepalive_ping_timeout_ms` (`integer`): Timeout waiting for PING ACK before terminating the connection.
* `max_call_duration_ms` (`integer`): Maximum duration for a long-lived RPC stream.

#### `http3` (HTTP/3 / QUIC Parameters)
* `max_concurrent_streams` (`integer`): Maximum bidirectional QUIC streams.
* `max_concurrent_uni_streams` (`integer`): Maximum unidirectional QUIC streams.
* `max_qpack_table_capacity` (`integer`): QPACK dynamic table capacity (default `8192` bytes).

#### `raw` (L4 Passthrough Parameters)
* `copy_buffer_size` (`integer`): Size of the zero-copy splice / copy buffer (default `16384` bytes).
* `idle_timeout_ms` (`integer`): Inactivity timeout before dropping idle L4 connections.
