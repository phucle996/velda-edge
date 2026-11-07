# velda-rdma

Kernel-Bypass Remote Direct Memory Access (RoCEv2 & InfiniBand) Transport Engine for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Boundary (`AGENTS.md` Section 2.8)**:
> - **`velda-upstream`**: Owns connection lifecycle, sharded pool state, health tracking, and physical endpoint selection (**"WHEN & WHO"**).
> - **`velda-rdma`**: Owns InfiniBand verbs, Queue Pairs, memory registration, credit-based flow control, and wire protocol execution (**"HOW"**).
> - **Thread Topology (`AGENTS.md` Section 2.9)**:
>   - **Frontline Traffic Worker (`velda-worker-{id}`)**: Exclusively serves hot-path traffic by leasing pre-warmed Queue Pairs from its core-local lane shard (< 10 ns) without locks. Never spawns threads or blocks on the hot path.
>   - **Shadow Logistics Worker (`velda-shadow-{id}`)**: Autonomous background companion thread pinned to the same core under `SCHED_IDLE`, exclusively responsible for Out-of-Band (OOB) handshakes, hardware QP initialization, and replenishing warm handles into the lane shard.

---

## 1. Hardware Architecture & Kernel-Bypass Mechanism

```text
                  Frontline Traffic Worker (velda-worker-{id})
                                      │
                                      ▼
                        Acquire Pre-Warmed Stream
                      (velda-connection-pool lane shard)
                                      │
                     ┌────────────────┴────────────────┐
                     ▼                                 ▼
             RdmaWriteHalf                    RdmaReadHalf
             (RdmaSender)                     (RdmaReceiver)
                     │                                 │
                     ▼                                 ▼
         [ Pre-Registered Slabs ]           [ Pre-Allocated Slabs ]
        (ibv_reg_mr pinned memory)         (Zero-copy recv buffers)
                     │                                 │
                     └────────────────┬────────────────┘
                                      │
                                      ▼
                   Hardware Host Channel Adapter (rNIC / HCA)
                          (Mellanox / RoCEv2 / IB)
                                      │
                        Direct DMA over RoCEv2 Fabric
                                      │
                                      ▼
                               Upstream Node
```

### Five Core Architectural Pillars

1. **Direct Memory Access (DMA)**:
   Transfers data directly between host user-space RAM and backend memory via the Host Channel Adapter (rNIC). Completely bypasses the Linux kernel network stack: zero syscalls, zero kernel socket buffers (`sk_buff`), and zero CPU memory copy overhead (`memcpy`).
2. **Protection Domain & Memory Registration**:
   Pre-allocated memory slabs (`PayloadSlab`) are pinned in physical RAM (`ibv_reg_mr`) and mapped into the NIC's onboard Memory Translation Table (MTT) with hardware access keys (`lkey`/`rkey`), enabling wire-speed DMA validation.
3. **Queue Pair & Credit-Based Flow Control**:
   Bidirectional communication executes over Reliable Connected Queue Pairs (RC QP). Atomic credit tracking strictly prevents Receiver Not Ready (RNR) hardware drops.
4. **Adaptive Hybrid Polling**:
   Direct fast-path busy polling during active traffic bursts coupled with slow-path hardware MSI-X interrupt sleep (`tokio::io::unix::AsyncFd` via `ibv_comp_channel`) during idle windows, eliminating idle CPU burn.
5. **Decoupled Send & Receive Flows ("Minh bạch và tách biệt 2 luồng")**:
   Transparent separation of Send (`RdmaSender`, `HardwareSender`, `RdmaWriteHalf`) and Receive (`RdmaReceiver`, `HardwareReceiver`, `RdmaReadHalf`) flows with zero mutex contention and independent half-split capabilities (`into_split`).

---

## 2. Core Primitives & Modules

### 2.1 Configuration & Tuning (`config.rs`)
`RdmaConfig` configures credit thresholds and slot sizing tuned to hardware profiles:
- `DEFAULT_RDMA_SLOT_SIZE`: 64 KB memory slot per transfer.
- Credit tiers:
  - `BASELINE_RDMA_CREDITS` (16 credits)
  - `DEFAULT_RDMA_CREDITS` (32 credits)
  - `HIGH_THROUGHPUT_RDMA_CREDITS` (64 credits)
  - `PEAK_RDMA_CREDITS` (128 credits)

### 2.2 Memory Pool & Slabs (`pool.rs`)
- **`PayloadSlab`**: Pre-registered pinned memory buffer with hardware local/remote keys.
- **`SlabPool`**: Core-local sharded slab allocator ensuring zero memory allocation on the request serving hot path.

### 2.3 Streaming Channels (`channel/`)
- **`RdmaChannel` / `RdmaStream`**: Active bidirectional RDMA communication channel.
- **`RdmaSender` / `RdmaReceiver`**: Independent half handles supporting `split()` for simultaneous reading and writing without lock contention.
- **`AsyncRead` & `AsyncWrite`**: Seamlessly integrates with Tokio async I/O pipelines.

### 2.4 Connector & Upstream Handshake (`connector.rs`)
- Coordinates the Out-of-Band (OOB) TCP connection to exchange Queue Pair numbers (QPN), Local Identifiers (LID), and initial credit tokens before transitioning the RC QP state machine to Ready-to-Receive (RTR) and Ready-to-Send (RTS).

### 2.5 Mock Environment (`mock.rs`)
- Full in-memory loopback RDMA emulation allowing complete test coverage and CI execution on standard developer machines without requiring physical RoCEv2/InfiniBand hardware.

---

## 3. Verification Commands

Run standard quality gate checks from the repository root:

```bash
# Code Style & Formatting
cargo fmt --check -p velda-rdma

# Strict Clippy Linting
cargo clippy -p velda-rdma --all-targets --all-features -- -D warnings

# Unit & Integration Tests (14 tests)
cargo test -p velda-rdma
```
