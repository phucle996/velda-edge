# velda-ingress

Top-Cone Edge Traffic Ingress Engine (Hourglass Model) coordinating multi-port traffic intake across OS kernel sockets (`velda-sock`) and kernel-bypass drivers (`velda-xdp`) for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `velda-ingress` is strictly reserved for **Edge Traffic Ingress Coordination & Declarative Listener Lifecycle**.
> - **Must Never** inspect, encode, or decode L7 protocol frames (HTTP/1.1, HTTP/2, HTTP/3, gRPC). Handlers pass raw connections directly to `velda-edge` pipeline runners.
> - **Must Never** own or spawn OS worker threads (worker lifecycle and CPU pinning are strictly owned by [`velda-edge`](../velda-edge)).
> - **Must Never** perform upstream routing or backend load balancing (owned by [`velda-upstream`](../velda-upstream) and [`velda-lb`](../velda-lb)).
> - **Protocol-Agnostic**: Treats listeners as opaque TCP, UDP, or XDP streams/datagrams.

---

## 1. Subsystem Architecture: The Ingress Triangle

In Velda Edge, Ingress is decoupled into a clean 3-crate triangle:

```text
                             Client Ingress Traffic
                                  (TCP / UDP)
                                       │
                                       ▼
                   ╔═══════════════════════════════════════╗
                   ║             velda-ingress             ║
                   ║        (Top-Cone Orchestrator)        ║
                   ╚═══════════════════╤═══════════════════╝
                                       │
                 ┌─────────────────────┴─────────────────────┐
                 ▼                                           ▼
  ╔═════════════════════════════╗             ╔═════════════════════════════╗
  ║         velda-sock          ║             ║          velda-xdp          ║
  ║ (OS Kernel Socket Mechanics)║             ║(AF_XDP Kernel-Bypass Driver)║
  ║  - SO_REUSEPORT Shards      ║             ║  - AF_XDP / UMEM Rings      ║
  ║  - BBR / FastOpen / TCP     ║             ║  - BPF Sockmap / PassFilter ║
  ║  - UDP GRO / GSO Batching   ║             ║  - Zero-Copy Slab Arena     ║
  ║  - Connection & splice      ║             ║  - Hardware Mode Probe      ║
  ╚═════════════════════════════╝             ╚═════════════════════════════╝
                 │                                           │
                 └─────────────────────┬─────────────────────┘
                                       │
                                       ▼
                       Frontline Traffic Worker Thread
                      (velda-edge: run_ingress_shard)
                                       │
                 ┌─────────────────────┴─────────────────────┐
                 ▼                                           ▼
        TcpHandler(Connection)                     UdpHandler(Datagram)
                 │                                           │
                 ▼                                           ▼
      velda-edge pipeline runner                  velda-edge pipeline runner
       (L4 Forward / H1 / H2)                      (L4 Session / H3 / gRPC)
```

---

## 2. Core Execution Model & Components

### 2.1 Declarative Ingress Bindings (`binding.rs`)
Ingress listeners are declared as strongly typed, protocol-agnostic bindings:
- **`IngressBinding::Tcp(TcpBinding)`**: Standard OS kernel TCP stream listener.
- **`IngressBinding::Udp(UdpBinding)`**: Standard OS kernel UDP datagram socket.
- **`IngressBinding::XdpTcp(TcpBinding, XdpConfig)`**: Hardware-accelerated XDP filter coupled with companion TCP accept listener.
- **`IngressBinding::XdpUdp(UdpBinding, XdpConfig)`**: Hardware-accelerated XDP filter coupled with companion UDP receive socket.

### 2.2 Symmetrical Sharding & Context (`ShardContext` & `split_shards`)
Rather than relying on ad-hoc thread creation, `velda-ingress` follows the network reality of **hardware queue sharding**:
- **`IngressEngine::split_shards(shard_count)`**:
  Partitions the configured ingress declarations into $N$ per-core execution contexts:
  ```rust
  let (initial_bindings, engine_handle, shard_contexts) = engine.split_shards(worker_count);
  ```
- **`ShardContext`**:
  Encapsulates the complete coordination state for a single hardware lane:
  - `shard_id`: Shard index matching CPU core / NIC RX queue.
  - `total_shards`: Total number of active worker shards.
  - `reconcile_rx`: Dedicated channel receiving dynamic configuration reloads broadcast from `IngressHandle`.
  - `resolved_publisher` / `resolved_subscriber`: Cross-shard coordination channels allowing Shard 0 to resolve ephemeral ports (`port: 0`) and publish them to Shards $1..N-1$ to bind identical ports via `SO_REUSEPORT`.

### 2.3 Flat Execution Runner (`run_ingress_shard`)
Each Frontline Traffic Worker thread (`velda-worker-{id}`) in `velda-edge` drives its assigned shard by invoking:
```rust
velda_ingress::run_ingress_shard(
    shard_ctx,
    bindings,
    shutdown_rx,
    tcp_dispatcher,
    udp_dispatcher,
).await
```
- Contiguous, top-to-bottom workflow without micro-function fragmentation (Rule 2.2).
- Accepts connections (batch drained up to 32 connections per tick) or receives UDP packets (drained via contiguous 64 KB slab buffers).
- Immediately invokes the protocol dispatcher without intermediate allocations or proxy wrappers.

### 2.4 Declarative Atomic Reconciliation (`reconcile.rs` & `IngressHandle`)
- When configuration updates arrive over IPC from `velda-sync`, `IngressHandle::reconcile(desired)` broadcasts the new listener topology across all active shards.
- **Pre-flight LKG Qualification**: Candidate configurations (such as XDP interface names or driver modes) are validated before touching live sockets. If any candidate fails validation, the batch is rejected and active listeners remain completely undisturbed.
- **Differential Updates**:
  - **New listeners**: Bound and added to the local task set.
  - **Removed listeners**: Gracefully signaled via `watch::Sender<bool>`, closing listening ports cleanly.
  - **Unchanged listeners**: Kept serving continuously with **zero dropped connections and zero downtime**.

---

## 3. Usage Example

```rust
use velda_ingress::{IngressEngine, IngressBinding, TcpBinding};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = IngressEngine::new();
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    
    // 1. Declare ingress listener
    engine.add_binding(IngressBinding::Tcp(TcpBinding::new("http-public", addr, false)))?;

    // 2. Split engine into 4 core-local shard contexts
    let (bindings, handle, shard_contexts) = engine.split_shards(4);

    println!(
        "Ingress configured with {} bindings across {} shards",
        bindings.len(),
        shard_contexts.len()
    );

    // Each frontline worker thread runs its assigned shard_ctx via `run_ingress_shard`
    // Dynamic config reloads can be submitted anytime via `handle.reconcile(new_bindings)`

    Ok(())
}
```

---

## 4. Verification Commands

Run standard quality gate checks from the repository root:

```bash
# Code Style & Formatting
cargo fmt --check -p velda-ingress

# Strict Clippy Linting
cargo clippy -p velda-ingress --all-targets --all-features -- -D warnings

# Unit & Integration Tests (10 unit tests, 2 integration test suites)
cargo test -p velda-ingress
```
