# Prometheus Metrics Exporter Plugin (`prometheus_exporter`)

Production-grade background service exposing zero-allocation OpenMetrics / Prometheus scrape endpoints on an isolated auxiliary port.

---

## 1. Overview & Service Specification

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `prometheus_exporter` |
| **Canonical Phase** | `background` |
| **Execution Runtime** | Isolated auxiliary thread with cooperative scheduling |
| **Auxiliary Socket** | `0.0.0.0:9090` (Auxiliary Port) |
| **Hot-Path Overhead** | **Zero**. Runs completely out-of-band; zero overhead on traffic workers |
| **Format** | Prometheus Text-based format 0.0.4 / OpenMetrics standard |
| **Security** | Supports Optional TLS/mTLS, Bearer Token / HTTP Basic Authentication, and CORS |

---

## 2. 100% Explicit Configuration Invariant

> [!IMPORTANT]
> **Strict Configuration Contract**: Every configuration block and parameter must be explicitly declared in `plugins.json`. No silent fallbacks or implicit defaults exist. Fields for disabled features must be explicitly declared as `"none"`, `false`, `[]`, or `null`.

| Field | Type | Required | Constraints / Allowed Values | Recommended Value | Description |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `listen` | String | **Yes** | Valid `IP:PORT` socket address (must not collide with ingress listeners) | `"0.0.0.0:9090"` | Dedicated auxiliary IP:Port socket to bind and expose metrics. |
| `path` | String | **Yes** | Must start with `/` | `"/metrics"` | HTTP URI path where scraper fetches metrics. |
| `protocol` | String | **Yes** | `"http"` or `"https"` | `"http"` | Transport protocol for the auxiliary exporter server. |
| `scrape.timeout_ms` | Integer (u64) | **Yes** | `100 ..= 60000` ms | `5000` | Exporter scraping request timeout threshold in milliseconds. |
| `scrape.max_concurrency` | Integer (usize) | **Yes** | `1 ..= 1024` | `16` | Maximum concurrent scraper connections allowed simultaneously. |
| `scrape.max_buffer_size_kb` | Integer (usize) | **Yes** | `16 ..= 16384` KB | `256` | Maximum scratch buffer capacity allocated for OpenMetrics serialization. |
| `tls.mode` | String | **Yes** | `"none"`, `"tls"`, or `"mtls"` | `"none"` | TLS operating mode. If `"none"`, certificate paths must be `null`. |
| `tls.cert_file` | String / null | **Yes** | Non-empty string when mode is `"tls"`/`"mtls"`, otherwise `null` | `null` | Path to server TLS certificate file in PEM format. |
| `tls.key_file` | String / null | **Yes** | Non-empty string when mode is `"tls"`/`"mtls"`, otherwise `null` | `null` | Path to server private key file in PEM format. |
| `tls.client_ca_file` | String / null | **Yes** | Non-empty string when mode is `"mtls"`, otherwise `null` | `null` | Path to client CA certificate bundle for mTLS verification. |
| `cors.enabled` | Boolean | **Yes** | `true` or `false` | `false` | Whether Cross-Origin Resource Sharing is enabled for Web UIs (e.g. Grafana). |
| `cors.allow_origins` | Array of strings | **Yes** | Allowed origin patterns (or `[]` if disabled) | `[]` | Permitted origins for CORS requests. |
| `cors.allow_methods` | Array of strings | **Yes** | Permitted HTTP methods (or `[]` if disabled) | `[]` | Allowed HTTP methods (e.g. `["GET", "OPTIONS"]`). |
| `cors.allow_headers` | Array of strings | **Yes** | Permitted HTTP headers (or `[]` if disabled) | `[]` | Allowed request headers (e.g. `["Authorization"]`). |
| `auth.mode` | String | **Yes** | `"none"`, `"bearer"`, or `"basic"` | `"none"` | Exporter authentication mode. |
| `auth.token` | String / null | **Yes** | Non-empty string if mode is `"bearer"`, otherwise `null` | `null` | Secret bearer token required for scraper access. |
| `auth.username` | String / null | **Yes** | Non-empty string if mode is `"basic"`, otherwise `null` | `null` | HTTP Basic Auth username for scraper access. |
| `auth.password` | String / null | **Yes** | Non-empty string if mode is `"basic"`, otherwise `null` | `null` | HTTP Basic Auth password for scraper access. |

---

## 3. Auxiliary Port Isolation Invariant

> [!CAUTION]
> **Auxiliary Port Isolation Invariant**: The Prometheus exporter socket (`listen`) runs as an auxiliary background service and **MUST NEVER** collide with any Data Plane traffic listener ports (e.g., ports `80`, `443`, `8080`, `8443`, `50051`).
> 
> The configuration compiler validates that the set of auxiliary ports and traffic listener ports are strictly disjoint:
> $$\text{ListenerPorts} \cap \text{AuxiliaryPorts} = \emptyset$$

---

## 4. Configuration Manifest Example (`plugins.json`)

```json
{
  "schema_version": 1,
  "plugins": [
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
          "mode": "none",
          "cert_file": null,
          "key_file": null,
          "client_ca_file": null
        },
        "cors": {
          "enabled": false,
          "allow_origins": [],
          "allow_methods": [],
          "allow_headers": []
        },
        "auth": {
          "mode": "none",
          "token": null,
          "username": null,
          "password": null
        }
      }
    }
  ]
}
```

> [!NOTE]
> Background plugins are not attached to individual HTTP/gRPC routes. They run as standalone auxiliary daemons managed by the runtime lifecycle engine.

---

## 5. Prometheus Scrape Configuration (`prometheus.yml`)

```yaml
scrape_configs:
  - job_name: 'velda_edge'
    scrape_interval: 5s
    scrape_timeout: 4s
    static_configs:
      - targets: ['127.0.0.1:9090']
    metrics_path: /metrics
```
