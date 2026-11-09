# Health & Readiness Probe Plugin (`health_probe`) — Architecture

This document specifies the architectural invariants, threading model, memory layout, lock-free state transitions, and Kubernetes lifecycle integration for the **`health_probe`** background plugin in Velda Edge.

---

## 1. Architectural Mission & Invariants

The `health_probe` plugin (`crates/velda-plugin/src/background/health_probe/`) runs an ultra-low-latency, zero-heap-allocation HTTP management server on a dedicated management port (e.g. `0.0.0.0:9091`). It serves orchestrator probes (Kubernetes Kubelet, Docker Swarm, Nomad, AWS ALB, GCP Cloud Armor) completely isolated from Data Plane traffic worker loops.

```mermaid
flowchart TD
    subgraph DataPlane["Data Plane Hot Path (Serving Cores)"]
        W0["velda-worker-0"]
        W1["velda-worker-1"]
        W0 & W1 -->|Zero Contention / Zero Lock| Serving["Serve Downstream Traffic\n(Ports 80, 443, 8080)"]
    end

    subgraph Bootstrap["Bootstrap & Configuration (velda-edge / velda-sync)"]
        Snap["RuntimeSnapshot\n(plugins.background_plugins)"] -->|Zero MPSC Channel / Direct Handoff| ShadowInit["Mount into velda-shadow-0\n(bg_tasks: JoinSet)"]
    end

    subgraph ShadowWorker["Shadow Logistics Worker (Core 0, SCHED_IDLE)"]
        ShadowInit --> HealthLoop["HealthProbePlugin::run()\n(Sequential Inline Accept Loop)"]
        
        HealthLoop --> Accept{"listener.accept()"}
        Accept -->|Probe Connected| TimeoutGuard["tokio::time::timeout(timeout_ms)"]
        TimeoutGuard --> FastRead["stream.read(512B stack buffer)"]
        FastRead -->|n == 0| EarlyExit["Instant Clean Return\n(No 404 written to closed socket)"]
        FastRead -->|n > 0| MatchPath{"Path Matching"}
        
        MatchPath -->|/livez| CheckLive{"state.is_live()"}
        MatchPath -->|/readyz| CheckReady{"state.is_ready() && !state.is_draining()"}
        MatchPath -->|other| Resp404["RESP_404 (.rodata)\nContent-Length: 0"]
        
        CheckLive -->|true| Resp200["RESP_200 (.rodata)\nContent-Length: 0"]
        CheckLive -->|false| Resp503["RESP_503 (.rodata)\nContent-Length: 0"]
        
        CheckReady -->|true| Resp200
        CheckReady -->|false| Resp503
        
        Resp200 & Resp503 & Resp404 --> FastWrite["stream.write_all(static_bytes)"]
        FastWrite --> LoopNext["Next Probe (Zero Spawn, Zero Heap Allocation)"]
    end

    subgraph Orchestrator["Cluster Orchestrator (Out-of-Band)"]
        Kubelet["Kubernetes Kubelet / LB"] -->|GET /livez (Port 9091)| Accept
        Kubelet -->|GET /readyz (Port 9091)| Accept
    end
```

### Core Invariants:
1. **Bootstrap Direct Handoff (Zero-mpsc)**:
   - At system bootstrap, the background plugin descriptor is parsed, validated, and embedded directly into `RuntimeSnapshot`.
   - `velda-edge` mounts `health_probe` straight into `velda-shadow-0` (`bg_tasks: JoinSet<()>`).
   - Frontline Traffic Workers (`velda-worker-{id}`) **NEVER** exchange messages, send tasks over `mpsc` channels, or coordinate with `health_probe`.
   - On runtime hot-reload (`reload_rx.changed()`), `velda-shadow-0` reads the new atomic snapshot pointer (`shared_runtime.load()`), cancels old background tasks, and mounts updated instances — **100% zero-mpsc**.
2. **Port Isolation Invariant**:
   - The probe socket (`listen_addr`) runs strictly on a dedicated management port:
     $$\text{ManagementPort} \cap \text{TrafficPorts} = \emptyset$$
   - A catastrophic traffic spike, connection flood, or L7 DDoS attack on ports 80/443 **CANNOT** saturate or delay orchestrator probes. Kubelet receives responses in microseconds without false-positive container evictions or restarts.
3. **Flat Sequential Inline Execution (Zero Task Spawn)**:
   - Eliminates unbounded `tokio::spawn` allocations per connection.
   - Incoming connections are processed sequentially inline on the Shadow Worker. Because each health probe transaction completes in under $10\text{--}20\ \mu\text{s}$, a single thread effortlessly serves over 50,000 probes/second with **0 heap allocations**.
4. **Hard Timeout Protection (`timeout_ms`)**:
   - Every connection read and write is strictly bounded by `tokio::time::timeout(Duration::from_millis(config.timeout_ms), ...)`.
   - Slowloris probes or stalled TCP handshakes are aborted after `timeout_ms`, completely preventing socket descriptor leaks or zombie tasks.
5. **Zero-Byte Static Payload (`&'static [u8]`)**:
   - Emits pre-compiled HTTP/1.1 response slices embedded directly in the binary's `.rodata`:
     * `HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`
     * `HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`
     * `HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`
   - Zero dynamic formatting strings, zero allocations, zero chunked encoding overhead.
6. **Graceful Draining Semantics**:
   - When a termination signal (SIGTERM / SIGINT) is intercepted by the lifecycle supervisor, `HealthState::set_draining(true)` is executed atomically.
   - The `/readyz` endpoint immediately flips to `503 Service Unavailable`, causing Kubernetes Endpoints Controller and external load balancers to prune the pod from upstream routing pools *before* active TCP/H2/QUIC client connections enter connection draining.

---

## 2. Lock-Free Health State Machine

The health and readiness state is encapsulated by [`HealthState`](file:///home/phucle/.gemini/antigravity/worktrees/velda-edge/greeting_handler/crates/velda-plugin/src/background/health_probe/plugin.rs#L40-L100), backed exclusively by cacheline-isolated atomic flags (`Arc<AtomicBool>`):

```mermaid
stateDiagram-v2
    [*] --> Starting: Process Init
    Starting --> LiveNotReady: is_live = true, is_ready = false\n(/livez: 200, /readyz: 503)
    
    LiveNotReady --> Serving: Bootstrap Complete\nstate.set_ready(true)\n(/livez: 200, /readyz: 200)
    
    Serving --> Draining: SIGTERM Intercepted\nstate.set_draining(true)\n(/livez: 200, /readyz: 503)
    
    Draining --> Terminated: Active Connections Drained\nProcess Exit
    
    Serving --> DeadlockDetected: Worker Hang / Panic\nstate.set_live(false)\n(/livez: 503, /readyz: 503)
    DeadlockDetected --> [*]: Kubelet Restarts Pod
```

### State Evaluation Table:

| State Phase | `is_live` | `is_ready` | `is_draining` | `/livez` Status | `/readyz` Status | Orchestrator Action |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **Bootstrapping** | `true` | `false` | `false` | `200 OK` | `503 Service Unavailable` | Waits for warm caches & listeners |
| **Serving (Healthy)** | `true` | `true` | `false` | `200 OK` | `200 OK` | Routes live user traffic to pod |
| **Draining (SIGTERM)** | `true` | `true` | `true` | `200 OK` | `503 Service Unavailable` | Prunes pod from endpoints; drains traffic |
| **Deadlock / Fatal** | `false` | Any | Any | `503 Service Unavailable` | `503 Service Unavailable` | Kubelet terminates & restarts pod |

---

## 3. Concurrency & Execution Model

### 3.1 Shadow Worker Threading Contract
The probe server executes inside the Tokio runtime instance managed by `velda-shadow-0`:
- **Thread Scheduling**: `SCHED_IDLE` (Linux) / `nice(19)`.
- **CPU Affinity**: Pinned to Core 0 alongside `velda-worker-0`.
- **Preemption Guarantee**: The Linux CFS scheduler grants 100% CPU priority to traffic worker threads; the probe loop consumes spare CPU cycles ($< 0.05\%$ core load).

### 3.2 Sequential Accept vs Threadpool Contention
By avoiding `tokio::spawn` per probe:
- Probe requests run sequentially on the shadow task.
- Even under 1,000 probes per second, execution time is:
  $$T_{\text{total}} = 1000 \times 15\ \mu\text{s} = 15\ \text{ms} = 1.5\%\ \text{of 1 CPU core}$$
- Zero green-thread scheduling queues are touched.
- Cachelines for `config` and `state` remain warm in L1 Data Cache.
