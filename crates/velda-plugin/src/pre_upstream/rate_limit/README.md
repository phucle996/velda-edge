# Pre-Upstream Rate Limiting Plugin (`rate_limit`)

## Architecture Overview

```text
  Inbound L7 Request ──► RateLimitPlugin::on_upstream_request() [Hot Path: Zero IO / Zero Alloc]
                               │
                               ├─► 1. Lock-free sot_health check (Memory | Allow | Deny)
                               ├─► 2. CompiledKeyExtractor (CSPRNG boot_salt SWAR hash)
                               │       └─ Single strategy OR Hierarchical 4-tier cascade [H0, H0|H1, ...]
                               ├─► 3. RateLimitArena:
                               │       ├─ L1 CoreLane CAS micro-lease (~ns, zero lock)
                               │       └─ 64-shard ShardSlab (16-way set-associative CLOCK eviction)
                               ├─► Allowed (within burst or bounded overdraft grace)
                               │       └─ Continue + x-ratelimit-limit / x-ratelimit-remaining
                               └─► Exhausted (exceeds burst + overdraft_percent)
                                       └─ Reject(TooManyRequests / 429) + bypass headers

  Shadow Logistics Worker (when etp_id != null, pbww > 0):
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │ collect_dirty_deltas() ──► RateLimitSotProvider (pure dispatch across 9 SoT ETPs):     │
  │   redis | memcached | dynamodb | postgres | mysql | nats | clickhouse |                │
  │   elasticsearch | s3   ──► atomic push + reconcile_cluster_totals()                    │
  └────────────────────────────────────────────────────────────────────────────────────────┘
```

## 1. What This Plugin Does

`rate_limit` protects backend upstreams from traffic spikes, spam bursts, and credential/IP rotation floods across both single-node and multi-node deployments:
- **Hierarchical Multi-Header Rate Limiting**: Evaluates single keys (`client_ip`, `header`, `route`, `priority`, `composite`) or cumulative cascading header tiers (`hierarchical`) such as `[x-tenant-id, x-api-key, client_ip]` so attackers rotating IPs or keys cannot bypass tenant quotas.
- **Core-Local Micro-Leasing + Bounded Soft Overdraft**: Uses lock-free per-core L1 `CoreLane` micro-leases (`lease_batch` up to 16 when `burst_capacity >= 64`) over a 64-shard `CLOCK` `ShardArena`, plus configurable bounded `overdraft_percent` grace above `burst_capacity` to prevent false positives from cross-core skew or asynchronous cluster sync jitter.
- **Multi-SoT ETP Storage Support**: Optionally syncs dirty request deltas and reconciles cluster-wide totals on the Shadow Worker (`pbww > 0`) against any of **9 State-of-Truth (SoT) storage ETPs**: `redis`, `memcached`, `dynamodb`, `postgres`, `mysql`, `nats`, `clickhouse`, `elasticsearch`, and `s3`.

## 2. Where It Sits in Velda Edge

- **Pipeline Stage**: `pre_upstream` (`PreUpstreamPlugin`, executed after routing and authentication/WAF filters, before upstream connection acquisition).
- **Worker Placement**:
  - Request decision runs strictly synchronously on **Frontline Traffic Workers** (`on_upstream_request`) with zero network calls and zero heap allocations.
  - Distributed SoT counter push/reconciliation runs asynchronously on **Shadow Logistics Workers** (`run_background`, when `etp_id` is configured and `pbww > 0`).
- **Supported Protocols**: `SupportedProtocols::ALL_L7` (HTTP/1.1, HTTP/2, HTTP/3, gRPC).

## 3. Requirements & Prerequisites

- **Local-only mode (`etp_id: null`)**: Zero external dependencies; runs completely in RAM with `pbww = 0`.
- **Cluster SoT mode (`etp_id: "<id>"`)**: Requires a matching entry in `etp.json` / `etp.bin` whose `kind` belongs to the 9 supported SoT storage ETPs (`redis`, `memcached`, `dynamodb`, `postgres`, `mysql`, `nats`, `clickhouse`, `elasticsearch`, `s3`). Non-SoT ETPs such as `kafka`, `loki`, or identity providers are rejected during `velda-sync` validation and plugin ETP attachment.

## 4. Quick Start Configuration

```json
{
  "id": "rate_limit",
  "config": {
    "algorithm": "token_bucket",
    "etp_id": "redis-main",
    "rate_per_second": 1000,
    "burst_capacity": 2000,
    "overdraft_percent": 5,
    "key_strategy": "hierarchical",
    "key_headers": ["x-tenant-id", "x-api-key", "client_ip"],
    "fallback_behavior": "memory",
    "sync_interval_ms": 200
  }
}
```

## 5. Documentation Directory

- [RUNBOOK.md](./RUNBOOK.md) — Complete configuration schema, scenarios, telemetry signals, and incident triage.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — Internal memory geometry (`CoreLane`, `ShardSlab`), CSPRNG SWAR hashing, and provider dispatch design.
- [BENCHMARK.md](./BENCHMARK.md) — Standardized 5-tier benchmark matrix and x86-64-v3 assembly verification plan.
- [TEST.md](./TEST.md) — Executed unit and integration test verification report.
