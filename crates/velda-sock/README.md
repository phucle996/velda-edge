# velda-sock

Linux OS Kernel Network Stack Mechanics (`sk_buff` path, `SO_REUSEPORT`, `TCP_FASTOPEN`, `BBR`, `UDP_GRO`/`GSO`, `splice`) for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Boundary & Invariant**:
> `velda-sock` is strictly reserved for **Linux OS Kernel Socket Mechanics & L4 Transport Primitives**.
> - **Must Never** contain ingress accept/receive loops, task orchestration, or `JoinSet` workers (owned by [`velda-ingress`](../velda-ingress)).
> - **Must Never** inspect, encode, or decode L7 protocol frames (HTTP/1.1, HTTP/2, HTTP/3, gRPC).
> - **Must Never** execute eBPF/AF_XDP operations (owned by [`velda-xdp`](../velda-xdp)).
> - **Must Never** perform upstream endpoint selection or load balancing (owned by [`velda-upstream`](../velda-upstream) and [`velda-lb`](../velda-lb)).

---

## 1. The Ingress Triangle & Architecture

In Velda Edge, the Ingress subsystem forms a symmetrical 3-crate triangle:

```text
                           ┌─────────────────────────┐
                           │      velda-ingress      │
                           │ (Top-Cone Orchestrator) │
                           │  - IngressEngine        │
                           │  - run_ingress_shard    │
                           │  - Declarative Reconcile│
                           └────────────┬────────────┘
                                        │
                 ┌──────────────────────┴──────────────────────┐
                 ▼                                             ▼
  ╔═════════════════════════════╗               ╔═════════════════════════════╗
  ║         velda-sock          ║               ║          velda-xdp          ║
  ║ (OS Kernel Socket Mechanics)║               ║(AF_XDP Kernel-Bypass Driver)║
  ║  - sk_buff path             ║               ║  - AF_XDP / UMEM Rings      ║
  ║  - SO_REUSEPORT Shards      ║               ║  - BPF Sockmap / PassFilter ║
  ║  - TCP BBR / FastOpen       ║               ║  - Zero-Copy Slab Arena     ║
  ║  - UDP GRO / GSO Batching   ║               ║  - Hardware Mode Probe      ║
  ║  - Connection & splice      ║               ║  - Userspace L3/L4 State    ║
  ╚═════════════════════════════╝               ╚═════════════════════════════╝
```

`velda-sock` provides the low-level, high-performance Linux kernel socket abstractions that power the standard OS networking path.

---

## 2. Core Primitives & Modules

### 2.1 TCP Mechanics (`tcp/`)
- **`TcpListener`**:
  - `bind_shards(addr, config)`: Pre-binds $N$ independent kernel listening sockets on the exact same port using `SO_REUSEPORT`.
  - `bind_shard(addr, config, shard_id)`: Binds a single dedicated shard listener affined to a specific CPU core.
  - Zero-mutex, independent socket handles distributed across worker cores without accept lock contention.
- **Kernel Tuning & Acceleration (`TcpListenerConfig`, `TcpAccelerationOptions`)**:
  - **`SO_INCOMING_CPU`**: Directs incoming SYN packets directly to the listening socket assigned to the hardware NIC's receive queue core.
  - **BBR Congestion Control**: Dynamically enables `TCP_CONGESTION = "bbr"` on Linux kernels with BBR support.
  - **`TCP_FASTOPEN`**: Enables zero-RTT TCP Fast Open listener queues (`TCP_FASTOPEN = 1024`).
  - **Low Latency & Buffers**: `TCP_NODELAY`, `TCP_QUICKACK`, `TCP_DEFER_ACCEPT`, continuous receive buffer sizing (`SO_RCVBUF`).

### 2.2 UDP Mechanics (`udp/`)
- **`UdpSocket`**:
  - Multi-shard listener instantiation using `SO_REUSEPORT`.
  - Continuous socket receive/send buffer tuning (`SO_RCVBUF`, `SO_SNDBUF`).
- **`Datagram`**:
  - Immutable, zero-allocation datagram envelope wrapping `Bytes`, peer address, and local socket address.
- **Kernel Packet Batching & Offloads**:
  - **`UDP_GRO` (Generic Receive Offload)**: Aggregates consecutive UDP packets in kernel space, reducing userspace interrupts.
  - **`UDP_GSO` (Generic Segmentation Offload)**: Transmits large packet payloads up to 64 KB in a single syscall, letting the NIC hardware handle segmentation.

### 2.3 L4 Connection & Tracking (`connection.rs`)
- **`Connection`**:
  - Wraps `tokio::net::TcpStream` with a unique, monotonically generated `ConnectionId`.
  - Atomic transfer accounting: tracks total bytes read and written via lock-free `TransferStats` (`AtomicU64`).
  - Implements `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`.
- **`ConnectionReader` & `ConnectionWriter`**:
  - Zero-copy stream decomposition via `connection.split()`, allowing concurrent read and write loops with zero mutex contention.

### 2.4 High-Performance Forwarding & Splice (`forward.rs`)
- **`forward_bidirectional(client, upstream)`**:
  - Full-duplex raw byte proxying with atomic byte tracking and clean cancellation propagation.
- **`forward_connection(client, upstream, buffer_size)`**:
  - Tunable buffer copying sized to continuous hardware profiles.
- **`splice_bidirectional(stream_a, stream_b, pipe_capacity)`**:
  - Linux `splice(2)` zero-copy kernel pipe forwarding, transferring data directly between socket buffers without crossing the kernel-userspace boundary.

---

## 3. Usage Example

```rust
use velda_sock::{TcpListener, TcpListenerConfig, IncomingCpuMode};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    
    // Configure hardware-tuned TCP socket options
    let mut config = TcpListenerConfig::default();
    config.backlog = 4096;
    config.acceleration.fast_open = true;
    config.acceleration.nodelay = true;
    config.incoming_cpu = IncomingCpuMode::CpuPinning;

    // Bind shard 0 affined to CPU core 0
    let listener = TcpListener::bind_shard(addr, config, 0)?;
    println!("Bound TCP shard on {}", listener.local_addr());

    // Accept raw connection and wrap into L4 Connection
    let (conn, peer) = listener.accept().await?;
    println!("Accepted connection #{} from {}", conn.id(), peer);

    Ok(())
}
```

---

## 4. Verification Commands

Run standard quality gate checks from the repository root:

```bash
# Code Style & Formatting
cargo fmt --check -p velda-sock

# Strict Clippy Linting
cargo clippy -p velda-sock --all-targets --all-features -- -D warnings

# Unit & Integration Tests (12 unit tests, 4 integration suites)
cargo test -p velda-sock
```
