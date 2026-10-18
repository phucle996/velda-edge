# `runtime.json` — Edge Node Runtime Profile Specification

The `runtime.json` file configures node-level operational parameters: asynchronous DNS discovery tuning, kernel transport defaults, TLS session resumption limits, and autonomous memory overload shedding thresholds.

---

## 1. Architectural Mindset & Hardware Probing Invariant

### 1.1 Kernel Hardware Probing
Physical CPU cores, NUMA topologies, and total available memory are **never configured via JSON**. At bootstrap, `velda-core` inspects system `/sys/devices/system/cpu` and `/proc/meminfo` via `global_hardware_topology()`.
- Thread pools (`io_workers`) and pool shard counts scale dynamically proportional to physical cores.
- JSON cannot override or inject fabricated hardware metrics.

### 1.2 Zero-IO Autonomous Operation
Once loaded into RAM as `RuntimeProfile`, these tuning values govern runtime behavior without requiring disk access or Control Plane queries.

---

## 2. Schema Definition & Top-Level Fields

```json
{
  "version": 1,
  "discovery": { /* DNS discovery parameters */ },
  "transport": { /* L4 transport & kernel tuning */ },
  "tls": { /* TLS session cache & handshake parameters */ },
  "overload": { /* Memory overload shedding thresholds */ }
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `version` | `integer` | Yes | Schema version. Must be `1`. |
| `discovery` | `object` | Yes | Global asynchronous DNS resolver and LKG cache limits. |
| `transport` | `object` | Yes | Maximum active connections and default TCP kernel socket parameters. |
| `tls` | `object` | Yes | TLS session resumption cache and handshake timeout policies. |
| `overload` | `object` | Yes | Autonomous overload shedding memory watermarks. |

---

## 3. Section Specifications

### 3.1 DNS Discovery (`discovery`)

Controls the background asynchronous DNS resolver (`velda-discovery`) for dynamic upstream resolution:

| Field | Type | Description |
| :--- | :--- | :--- |
| `max_dns_cache_capacity` | `integer` | Maximum resolved domain entries held in RAM. |
| `max_lkg_capacity` | `integer` | Maximum Last-Known-Good (LKG) fallback entries retained if nameservers become unreachable. |
| `query_timeout_ms` | `integer` | Timeout in milliseconds for DNS queries. |
| `positive_ttl_secs` | `integer` | Duration in seconds to cache successful DNS records. |
| `negative_ttl_secs` | `integer` | Duration in seconds to cache NXDOMAIN or query error responses (DoS protection). |
| `max_negative_ttl_secs` | `integer` | Upper ceiling for negative TTL caching. |

---

### 3.2 Transport & Kernel Socket Tuning (`transport`)

Governs L4 ingress engines (`velda-transport`):

| Field | Type | Description |
| :--- | :--- | :--- |
| `max_active_connections` | `integer` | Global ceiling for active concurrent client connections before reject. |
| `reconcile_channel_capacity` | `integer` | Internal event channel capacity between reactor threads. |
| `tcp.backlog` | `integer` | `listen(2)` socket backlog queue depth. |
| `tcp.nodelay` | `boolean` | Enables `TCP_NODELAY` (disables Nagle algorithm) for immediate packet delivery. |
| `tcp.keepalive_secs` | `integer` | TCP keep-alive probe interval in seconds. |
| `tcp.recv_buffer_size` | `integer` | `SO_RCVBUF` kernel receive buffer size in bytes. |
| `tcp.send_buffer_size` | `integer` | `SO_SNDBUF` kernel send buffer size in bytes. |
| `tcp.copy_buffer_size` | `integer` | Buffer size in bytes allocated for streaming pipes (e.g., `16384` / 16KB). |

---

### 3.3 Ingress TLS Defaults (`tls`)

Configures TLS termination engine defaults (`velda-tls`):

| Field | Type | Description |
| :--- | :--- | :--- |
| `session_cache_capacity` | `integer` | Maximum cached TLS sessions in memory for fast TLS session resumption. |
| `max_early_data_size` | `integer` | Maximum 0-RTT early data payload accepted under TLS 1.3. |
| `send_tls13_tickets` | `integer` | Number of TLS 1.3 NewSessionTicket messages sent to clients upon connection. |
| `handshake_timeout_secs` | `integer` | Maximum allowed duration for a client TLS handshake before socket drop. |

---

### 3.4 Autonomous Overload Shedding (`overload`)

Velda Edge continuously tracks memory utilization. When system memory pressure reaches configured watermarks, the data plane sheds traffic autonomously without crashing or exhausting OS swap:

| Field | Type | Description |
| :--- | :--- | :--- |
| `shedding_high_watermark` | `float` | Memory utilization ratio (e.g., `0.70` = 70%) triggering progressive traffic shedding (rejecting new non-critical connections). |
| `shedding_low_watermark` | `float` | Memory utilization ratio (e.g., `0.60` = 60%) where shedding ceases. |
| `critical_high_watermark` | `float` | Critical memory watermark (e.g., `0.80` = 80%) where the node rejects all new connections and aggressively evicts idle pooled connections. |
| `critical_low_watermark` | `float` | Watermark where the critical state is disarmed. |
