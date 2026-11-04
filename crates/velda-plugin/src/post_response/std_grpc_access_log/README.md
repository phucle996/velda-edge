# Standard Stream gRPC Access Log Plugin (`std_grpc_access_log`)

---

## 1. What is `std_grpc_access_log`?

`std_grpc_access_log` is Velda Edge's canonical gRPC RPC observability and access logging engine. Operating in the `post_response` phase immediately following gRPC stream completion or fast error rejection, it captures complete RPC execution audit logs without inducing hot-path latency, without allocating heap memory, and without polluting CPU caches.

Built strictly around the **Hourglass Architecture**, **AGENTS.md Rule 2.7 (HTTP vs gRPC Isolation)**, and **Rule 2.9 (Frontline Traffic vs Shadow Logistics)**, this plugin isolates all formatting, serialization, and disk/terminal I/O onto companion background threads while frontline workers record RPC metrics using hardware SIMD vector registers in under 12 nanoseconds.

---

## 2. Core Architectural Invariants

1. **Strict Protocol Isolation (Rule 2.7)**:
   Declares `SupportedProtocols::GRPC`. The plugin binds strictly to gRPC routes (`grpc/tcp` and `grpc/udp`) and is prohibited from being attached to HTTP REST or Raw TCP routes.

2. **Zero-Allocation Hot Path (`0 B / 0 allocs`)**:
   Frontline request processing workers allocate exactly zero heap memory. All log records are constructed on the CPU stack inside a cacheline-aligned 256-byte POD structure (`PackedGrpcRecord`) and transferred directly into physical RAM via non-temporal vector streaming stores (`VMOVNTDQ`).

3. **Status-Aware Single-Cycle Sampling (O(1) Branchless)**:
   Canonical gRPC status codes (0..=16) directly index into a 17-element array of precomputed threshold integers. Status codes configured with a rate of `0.0` (or omitted from the config) trigger an instant hot-path escape in **under 1.0 nanosecond**.

4. **Frontline vs Shadow Logistics Worker Separation (Rule 2.9)**:
   Frontline workers never perform standard stream I/O or acquire mutexes. A background Shadow Logistics Worker pinned to an auxiliary core drains SPSC ring buffer shards and serializes batches directly to `stdout` or `stderr` via single `libc::write` syscalls.

5. **Trailers & Streaming Message Awareness**:
   Unlike generic HTTP loggers, `std_grpc_access_log` natively tracks:
   - Canonical `grpc-status` (0 = OK, 1 = CANCELLED, ..., 16 = UNAUTHENTICATED).
   - `grpc-message` error phrase.
   - Separate RPC Service and Method names parsed from `/package.ServiceName/MethodName`.
   - Bidirectional stream message counts (`messages_sent`, `messages_received`).
   - Transport identity (`grpc-tcp` over HTTP/2 vs `grpc-udp` over QUIC).

---

## 3. Configuration Contract

The plugin is declared under the `plugins` array in `plugins.json` and attached to gRPC routes in `routes.json`:

```json
{
  "id": "std_grpc_logger",
  "plugin": "std_grpc_access_log",
  "phase": "post_response",
  "config": {
    "target": "stdout",
    "format": "json",
    "include_request_id": true,
    "include_route_id": true,
    "include_upstream": true,
    "include_peer_addr": true,
    "include_latency": true,
    "buffer_capacity": 4096,
    "sampling": {
      "0": 0.05,
      "OK": 0.05,
      "UNAVAILABLE": 1.0,
      "DEADLINE_EXCEEDED": 1.0,
      "RESOURCE_EXHAUSTED": 1.0,
      "INTERNAL": 1.0
    }
  }
}
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `target` | `string` | `"stdout"` | Output stream: strictly `"stdout"` or `"stderr"`. Direct file logging is forbidden. |
| `format` | `string` | `"json"` | Serialization format: `"json"` or `"text"`. |
| `include_request_id` | `bool` | `true` | Extracts and records the canonical `x-request-id` header. |
| `include_route_id` | `bool` | `true` | Extracts and records the matched route identifier. |
| `include_upstream` | `bool` | `true` | Extracts and records the target upstream endpoint (`IP:Port`). |
| `include_peer_addr` | `bool` | `true` | Extracts and records the client peer address. |
| `include_latency` | `bool` | `true` | Extracts and records RPC duration in microseconds. |
| `buffer_capacity` | `int` | `4096` | Capacity per frontline worker lane shard (bounded between 128 and 65,536). |
| `sampling` | `map<string, float>` | Errors 100%, OK 10% | Sampling rate per canonical status code (`0..=16` or names). Range: `[0.0, 1.0]`. |

---

## 4. Log Output Examples

### JSON Format (Default)
```json
{"timestamp":1710000000000,"transport":"grpc-tcp","service":"order.OrderService","method":"StreamOrders","grpc_status":0,"grpc_status_name":"OK","duration_us":4500,"messages_sent":10,"messages_received":1,"bytes":2048,"request_id":"req-grpc-001","route_id":"orders","upstream":"10.0.1.5:50051","peer":"192.168.1.10:48292"}
```

### JSON Error Format
```json
{"timestamp":1710000000000,"transport":"grpc-udp","service":"payment.PaymentService","method":"Charge","grpc_status":14,"grpc_status_name":"UNAVAILABLE","duration_us":12500,"messages_sent":0,"messages_received":1,"bytes":128,"request_id":"req-pay-881","route_id":"payments","upstream":"10.0.1.8:50051","peer":"192.168.1.25:51234","error":"connection reset by upstream host"}
```

### Text Format
```text
[1710000000000] transport=grpc-tcp rpc=/order.OrderService/StreamOrders status=OK(0) duration_us=4500 msg_sent=10 msg_recv=1 bytes=2048 req_id=req-grpc-001 route=orders upstream=10.0.1.5:50051 peer=192.168.1.10:48292
```
