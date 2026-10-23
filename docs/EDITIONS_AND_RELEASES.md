# Velda Edge — Multi-Edition Release Architecture & Microarchitecture Guide

This document specifies the **Multi-Edition Release Architecture** of Velda Edge. It explains the technical rationale, microarchitecture targeting, hardware compatibility matrices, automated installation mechanisms, and local build configurations for each distribution tier.

---

## 1. Executive Summary & Design Philosophy

High-performance L7 reverse proxies and edge gateways spend a significant portion of CPU cycles executing:
- SIMD-accelerated string and byte-slice scanning (e.g. `memchr` for HTTP header framing, delimiter search, URI matching).
- Hashing and partition key computation (e.g. `ahash`, canary traffic distribution, consistent hashing).
- Cryptographic primitives and TLS frame processing.
- Memory block copying and buffer compaction.

### The Dilemma: Universal Baseline vs Peak Throughput
In compiled native systems (Rust, C++, Go), there is a classic trade-off:
1. **Dumbing down to the lowest common denominator (`x86-64-v1` or legacy baseline)**:
   - Compiling for maximum compatibility (CPUs from 2003+) restricts the compiler to 64-bit integer registers and SSE2 instructions.
   - 256-bit AVX2 vector pipelines, FMA (Fused Multiply-Add), and BMI1/BMI2 bit manipulation instructions remain completely disabled.
   - **Cost**: 15%–30% lower request throughput and higher CPU utilization per request on modern cloud servers.
2. **Dynamic Runtime CPU Dispatch (Fat Binaries / Multi-versioning)**:
   - Having one binary that detects CPU features at startup and branches dynamically via function pointers or `IFUNC` indirect calls.
   - **Cost**: Introduces indirect branch mispredictions, instruction cache bloat, and vtable-like calling overhead into zero-allocation hot paths where every CPU cycle matters.

### The Solution: Multi-Edition AOT Artifacts
Velda Edge adheres to **Strict AOT Performance**: zero indirect dynamic dispatch overhead in serving hot paths. We produce and distribute **specialized binary editions** on GitHub Releases:
- **`v3` (Cloud Performance Edition)**: Compiled with `-C target-cpu=x86-64-v3` (AVX2, FMA, BMI1/2) for modern cloud instances (AWS, Azure, GCP, modern bare-metal).
- **`v2` (Portable Compatibility Edition)**: Compiled with `-C target-cpu=x86-64-v2` (SSE4.2, Popcnt) for older VPS, legacy hypervisors, and nested virtualization environments.
- **`arm64` (Graviton & Ampere Edition)**: Compiled for `aarch64` (ARMv8-A + NEON) for modern ARM-based cloud servers (AWS Graviton, Ampere Altra, Apple Silicon).
- **`native` (Bare Metal Dedicated Build)**: Compiled with `-C target-cpu=native` for high-end bare-metal appliances.

---

## 2. The Edition Matrix

| Edition Name | Release Asset Suffix | ISA Target Level | Required CPU Extensions | Earliest Architecture | Target Cloud & Virtualization Platforms |
|:---|:---|:---|:---|:---|:---|
| **Cloud Performance (`v3`)** | `linux-amd64-v3.tar.gz` | `x86-64-v3` | **AVX2**, **FMA**, **BMI1**, **BMI2**, SSE4.2, Popcnt, MOVBE, AVX | Intel Haswell (2013+)<br>AMD Zen 1 / EPYC (2017+) | AWS EC2 (c5/m5/t3+), Azure (Dv3+, Fsv2+), GCP (N2, C2), Hetzner modern nodes, modern bare-metal |
| **Portable Baseline (`v2`)** | `linux-amd64-v2.tar.gz`<br>`linux-amd64.tar.gz` | `x86-64-v2` | **SSE4.2**, **Popcnt**, SSSE3, SSE3, CX16 | Intel Nehalem (2008+)<br>AMD Bulldozer (2011+) | Standard VPS (Linode, DigitalOcean basic, OVH), QEMU default `qemu64` virtual CPU, nested hypervisors |
| **ARM64 / Graviton** | `linux-arm64.tar.gz` | `aarch64` | **NEON**, FP16, Cryptography extensions | ARMv8-A (2011+) | AWS Graviton2/3/4 (c6g, m6g, c7g), Oracle Ampere A1, GCP Tau T2A, Apple Silicon Docker |
| **Bare Metal Dedicated** | *(Local Build Only)* | `native` | Full Host-Specific Pipeline | Host Dependent | High-frequency trading, private cloud bare metal, edge appliances |

---

## 3. Microarchitecture Comparison: `v3` vs `v2`

```text
x86-64-v2 (Portable Baseline)                x86-64-v3 (Cloud Performance)
┌─────────────────────────────────┐          ┌────────────────────────────────────────────────────────┐
│ 128-bit XMM Vector Registers    │          │ 256-bit YMM Vector Registers                           │
│ [ XMM0 ................. XMM15 ]│          │ [ YMM0 ........................................ YMM15 ]│
├─────────────────────────────────┤          ├────────────────────────────────────────────────────────┤
│ SSE4.2 String Search (16 bytes) │          │ AVX2 Vectorized Byte Search (32 bytes per instruction) │
│ Popcnt, CMPXCHG16B              │          │ BMI1 / BMI2 (Bit Manipulation: ANDN, BEXTR, BLSI, RORX)│
│ Baseline Glibc compatibility    │          │ FMA (Fused Multiply-Add), MOVBE                        │
└─────────────────────────────────┘          └────────────────────────────────────────────────────────┘
```

### Key Acceleration Pathways in `v3`:
1. **Vectorized Delimiter & Header Scanning (`memchr`)**:
   - `v2`: Scans up to 16 bytes per clock cycle using 128-bit SSE4.2 registers.
   - `v3`: Scans 32 bytes per clock cycle using 256-bit AVX2 registers (`_mm256_cmpeq_epi8`), cutting header boundary and carriage return searches in half.
2. **Bit Manipulation Accelerations (BMI1 / BMI2)**:
   - Bitwise mask extraction and shift instructions (`pext`, `pdep`, `tzcnt`, `andn`) execute in single clock cycles without clobbering general-purpose condition codes.
   - Accelerates WAF byte-token evaluation and CIDR prefix matching in IP filtering.
3. **Branch-Free Fast Hashing (`ahash`)**:
   - Uses AES-NI and AVX2 vector pipelines to hash user IDs, header values, and IP addresses with zero memory allocations and sub-nanosecond latencies.

---

## 4. Cloud & Hypervisor Compatibility Guide

### 4.1 Major Cloud Providers

| Provider | Instance Types | Recommended Edition | Notes |
|:---|:---|:---|:---|
| **Amazon Web Services (AWS)** | `c5`, `c6i`, `m5`, `m6i`, `t3`, `t3a`, `r5`, `c7i` | **`v3`** | Haswell/Skylake/Cascade Lake/Ice Lake CPUs fully support AVX2/BMI2. |
| | `c6g`, `c7g`, `m6g`, `m7g`, `t4g` | **`arm64`** | AWS Graviton instances require ARM64 NEON builds. |
| | `t2`, `c4`, `m4` (Legacy) | **`v2`** or **`v3`** | `m4`/`c4` (Haswell) supports `v3`. `t2` on older hosts should use `v2`. |
| **Microsoft Azure** | `Dv3`, `Dv4`, `Dv5`, `Fsv2`, `Esv3` | **`v3`** | 100% AVX2/BMI2 compliant. |
| | `Dpsv5`, `Epsv5` (Ampere Altra) | **`arm64`** | Azure ARM instances require ARM64 NEON builds. |
| **Google Cloud Platform (GCP)** | `N2`, `N2D`, `C2`, `C2D`, `C3` | **`v3`** | Cascade Lake, Milan, Ice Lake, Sapphire Rapids support `v3`. |
| | `T2A` (Ampere Altra) | **`arm64`** | GCP Tau ARM instances require ARM64 NEON builds. |
| | `E2`, `N1` (Shared/Legacy) | **`v2`** | Older N1 sandybridge/ivybridge backends lack AVX2; use `v2`. |
| **Hetzner Cloud** | `CX22`, `CX32`, `CPX`, `AX` (AMD EPYC) | **`v3`** | AMD EPYC Zen 2/Zen 3/Zen 4 instances fully support `v3`. |
| | `CAX` (Ampere Altra) | **`arm64`** | ARM instances require ARM64 NEON builds. |
| **DigitalOcean & Linode** | Premium Intel / Premium AMD Droplets | **`v3`** | Modern hypervisor passthrough enables AVX2. |
| | Basic / Legacy Droplets | **`v2`** | Basic hypervisors may mask AVX2; use `v2` for safety. |

### 4.2 Private Cloud & Hypervisors (Proxmox, VMware, KVM)
- **QEMU / KVM**: If the virtual machine CPU model is set to the default `qemu64`, AVX2 and BMI2 instructions are masked by the hypervisor! Running a `v3` binary on `qemu64` will cause `SIGILL` (Illegal Instruction: 4).
  - *Fix*: Set the VM CPU model to `host` or `host-passthrough` in Proxmox / virt-manager to unlock `v3`, or simply install the **`v2` (Portable)** edition.
- **VMware ESXi**: Supported if Hardware Compatibility is version 10+ and the host hardware is Haswell or newer with EVC (Enhanced vMotion Compatibility) set to Haswell or higher.

### 4.3 How to Check Your Host Machine's Capability
Run the following one-line command on any Linux machine:
```bash
if grep -qw avx2 /proc/cpuinfo && grep -qw bmi2 /proc/cpuinfo; then
    echo "Host supports: x86-64-v3 (Cloud Performance Edition)"
else
    echo "Host supports: x86-64-v2 (Portable Compatibility Edition)"
fi
```

---

## 5. Automated Installation via `install-systemd.sh`

The Velda Edge production installer (`install-systemd.sh`) features **Zero-Touch Microarchitecture Auto-Detection**.

### Auto-Detection Workflow
1. Detects OS and base architecture (`x86_64` vs `aarch64`).
2. On `x86_64`, inspects `/proc/cpuinfo` for `avx2` and `bmi2`:
   - If present $\to$ automatically downloads **`velda-edge-linux-amd64-v3.tar.gz`**.
   - If missing $\to$ automatically downloads **`velda-edge-linux-amd64-v2.tar.gz`**.
3. On `aarch64` $\to$ automatically downloads **`velda-edge-linux-arm64.tar.gz`**.
4. Verifies SHA256 integrity against the published `SHA256SUMS` file.
5. If an older GitHub release is specified that lacks edition suffixes, it automatically falls back to `velda-edge-linux-amd64.tar.gz`.

### Manual CLI Overrides
You can explicitly force an edition using CLI flags:
```bash
# Force Performance Edition (v3)
sudo ./install-systemd.sh --edition v3

# Force Portable Baseline Edition (v2)
sudo ./install-systemd.sh --edition v2

# For ARM64 hosts
sudo ./install-systemd.sh --arch arm64
```

---

## 6. Building from Source

### 6.1 Building via `build-systemd.sh`
The local build script (`build-systemd.sh`) automatically detects CPU capabilities and applies the appropriate compiler flags:
```bash
# Auto-detects host CPU and builds the optimal edition:
./build-systemd.sh --release

# Force Portable Edition (v2) for distribution to older servers:
./build-systemd.sh --release --edition v2

# Force Performance Edition (v3):
./build-systemd.sh --release --edition v3

# Build Dedicated Bare-Metal Edition (native CPU pipeline):
./build-systemd.sh --release --edition native
```

### 6.2 Building via Cargo Directly
You can compile any edition directly using standard Cargo and the `RUSTFLAGS` environment variable:

```bash
# 1. Cloud Performance Edition (v3)
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release -p velda-edge -p velda-sync

# 2. Portable Compatibility Edition (v2)
RUSTFLAGS="-C target-cpu=x86-64-v2" cargo build --release -p velda-edge -p velda-sync

# 3. Dedicated Bare Metal (Native Host)
RUSTFLAGS="-C target-cpu=native" cargo build --release -p velda-edge -p velda-sync

# 4. ARM64 (Cross-compile)
cargo build --release --target aarch64-unknown-linux-gnu -p velda-edge -p velda-sync
```

---

## 7. GitHub Actions Release Artifacts

Every official GitHub release tag (`v*`) publishes the following artifacts:

| Asset Name | Description | Target Use Case |
|:---|:---|:---|
| `velda-edge-linux-amd64-v3.tar.gz` | High-performance edge binaries (`velda-edge`, `velda-sync`) compiled with `x86-64-v3`. | AWS, Azure, GCP, modern bare-metal. |
| `velda-edge-linux-amd64-v2.tar.gz` | Broad compatibility edge binaries compiled with `x86-64-v2`. | Generic VPS, legacy hypervisors, QEMU. |
| `velda-edge-linux-amd64.tar.gz` | Canonical alias (matching `v2`) for backward compatibility with external scripts. | Safe default for legacy automation. |
| `velda-control-plane-linux-amd64.tar.gz` | Go Control Plane binary with embedded console UI. | Any Linux amd64 host. |
| `SHA256SUMS` | Cryptographic SHA256 checksums for all release assets. | Security & integrity validation. |
