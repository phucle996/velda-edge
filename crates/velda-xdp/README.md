# velda-xdp

Kernel-Bypass eBPF / XDP / `AF_XDP` Ingress Mechanics (UMEM Slab Arena, SPSC Rings, userspace RFC 793 TCP / RFC 768 UDP state machine) for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Boundary & Invariant**:
> `velda-xdp` is strictly reserved for **Kernel-Bypass Driver Mechanics, AF_XDP Rings, and eBPF Hook Lifecycle**.
> - **Must Never** contain ingress accept/receive loops, task orchestration, or `JoinSet` workers (owned by [`velda-ingress`](../velda-ingress)).
> - **Must Never** inspect, encode, or decode L7 protocol frames (HTTP/1.1, HTTP/2, HTTP/3, gRPC).
> - **Must Never** manage OS kernel sockets (`TcpListener`, `UdpSocket`) (owned by [`velda-sock`](../velda-sock)).
> - **Must Never** perform upstream endpoint selection or load balancing (owned by [`velda-upstream`](../velda-upstream) and [`velda-lb`](../velda-lb)).

---

## 1. Subsystem Architecture & Engine Kinds

`velda-xdp` enables kernel-bypass packet ingestion through three distinct engine execution modes:

```text
                               NIC Hardware RX Queue
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
          [ Standard sk_buff ]                       [ eBPF / XDP Hook ]
          (Handled by velda-sock)                              │
                                           ┌───────────────────┼───────────────────┐
                                           ▼                   ▼                   ▼
                                    1. PassFilter         2. Sockmap           3. AfXdp
                                 (Fast-path filter/    (Transport sockmap     (Zero-copy
                                  drop; pass to OS)     socket redirect)      UMEM DMA)
                                           │                   │                   │
                                           ▼                   ▼                   ▼
                                     OS Socket        Direct Socket       AF_XDP SPSC Rings
                                    (velda-sock)       Redirection        (XdpSlabArena)
```

### The 3 Engine Modes (`XdpEngineKind`)

| Engine Kind | Execution Mechanism | Hardware Requirement | Ideal Workload |
|---|---|---|---|
| **`PassFilter`** | Evaluates eBPF byte filters directly inside the NIC driver hook. Drops unauthorized traffic with zero CPU wakeups; passes qualified traffic to companion OS socket. | Standard Linux network driver | Anti-DDoS, IP whitelisting, edge access protection |
| **`Sockmap`** | Accelerates established TCP connection streaming by redirecting packets directly at the socket layer (`BPF_MAP_TYPE_SOCKMAP`), bypassing TCP/IP re-encapsulation. | Linux Kernel 5.4+ | High-throughput East-West proxying, local forwarding |
| **`AfXdp`** | Complete kernel bypass using `AF_XDP` sockets (XSK). DMA transfers raw ethernet frames directly into a contiguous userspace memory arena (`XdpSlabArena`). | NIC driver with AF_XDP Zero-Copy (ZCX) support | Millions of packets/sec (Mpps) stateless UDP and low-latency RPC |

---

## 2. Core Primitives & Components

### 2.1 XDP Listener & Driver Attachment (`listener.rs`)
- **`XdpListener`**:
  - Binds to a specific network interface (e.g. `eth0`, `lo`) and queue ID.
  - Supports multiple driver attachment flags:
    - **`XdpDriverMode::Driver` (`XDP_FLAGS_DRV_MODE`)**: Native driver mode executed in the network device driver before allocating kernel `sk_buff`s.
    - **`XdpDriverMode::Skb` (`XDP_FLAGS_SKB_MODE`)**: Generic fallback mode for testing and environments where the driver lacks native XDP hooks.
    - **`XdpDriverMode::Offload` (`XDP_FLAGS_HW_MODE`)**: Hardware SmartNIC offload executing directly on the NIC SoC processor.
  - Safe attachment and clean teardown releasing BPF links on drop.

### 2.2 UMEM Memory Arena (`pool/arena.rs`)
- **`XdpSlabArena`**:
  - Pre-allocates a contiguous, memory-pinned slab arena (UMEM) for zero-copy DMA packet transfer.
  - Aligned to 4096-byte chunk frames (`UMEM_CHUNK_SIZE`).
  - Lock-free, bitmapped chunk allocation yielding `ChunkHandle`s with monotonic generation tracking, preventing use-after-free and double-free anomalies.

### 2.3 SPSC Ring Buffers (`rings/`)
`velda-xdp` implements the 4 standard single-producer single-consumer (SPSC) ring queues required by Linux `AF_XDP`:
1. **`FillRing`**: Userspace deposits chunk addresses into this ring, giving the NIC memory slots to write incoming packets into.
2. **`RxRing`**: NIC delivers chunk addresses populated with incoming packet data for userspace intake.
3. **`TxRing`**: Userspace deposits packet addresses to be transmitted directly onto the wire.
4. **`CompletionRing`**: NIC returns chunk addresses whose transmission has completed, allowing userspace to recycle memory.

### 2.4 Minimal Userspace L3/L4 State Machine (`stack/`)
- **`stack::frame`**: Zero-copy dissection of Ethernet (IEEE 802.3), IPv4 (RFC 791), UDP (RFC 768), and TCP (RFC 793) packet headers directly from UMEM chunk slices.
- **`stack::tcp`**: Lightweight RFC 793 TCP state machine simulation:
  - Validates sequence and acknowledgement numbers.
  - Handles 3-way handshake (SYN $\to$ SYN-ACK $\to$ ESTABLISHED).
  - Supports seamless connection state transitions without requiring the Linux kernel TCP stack.

### 2.5 Hardware Probing & LKG Validation (`probe.rs`)
- **`validate_xdp_config(config)`**:
  - Verifies interface existence, queue boundaries, and driver support before attempting attachment.
  - **LKG Preservation**: If a new configuration specifies an invalid interface or unsupported driver mode, the validation fails immediately, preserving active listeners with zero downtime.

---

## 3. Usage Example

```rust
use velda_xdp::{XdpConfig, XdpEngineKind, XdpDriverMode, XdpListener};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure AF_XDP listener for interface eth0 on queue 0
    let config = XdpConfig::new("eth0", 0)
        .with_engine(XdpEngineKind::PassFilter)
        .with_driver_mode(XdpDriverMode::Skb);

    // 2. Pre-flight validate hardware before binding
    velda_xdp::probe::validate_xdp_config(&config)?;

    // 3. Bind XDP listener
    let listener = XdpListener::bind(config)?;
    println!(
        "Bound XDP listener on {} (engine: {:?})",
        listener.config().if_name,
        listener.config().engine_kind
    );

    Ok(())
}
```

---

## 4. Verification Commands

Run standard quality gate checks from the repository root:

```bash
# Code Style & Formatting
cargo fmt --check -p velda-xdp

# Strict Clippy Linting
cargo clippy -p velda-xdp --all-targets --all-features -- -D warnings

# Unit & Integration Tests (5 unit tests, 4 integration suites)
cargo test -p velda-xdp
```
