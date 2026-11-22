---
trigger: always_on
---

# Code Architecture & Engineering Rules: ASCII Diagrams, Backward Compatibility, Comments & Multi-Target Invariants

These invariants are strictly enforced across the codebase (Rust Data Plane `crates/`, Go Control Plane `control-plane/`, and React Console `ui/`):

## 1. ASCII Mechanism Diagrams at File Headers
- Every core logic implementation file, pipeline strategy module, and workflow owner (`.rs`, `.go`, `.ts`) MUST include an ASCII architectural or operational flow diagram in its top-level module documentation (`//!` in Rust, `//` in Go/TypeScript).
- The ASCII diagram must clearly illustrate:
  - Inbound data/frame flow and phase transitions.
  - Core bitwise / memory layout / register / state machine mechanics.
  - Subsystem boundary interactions, error short-circuit paths, and decision trees.

## 2. Backward Compatibility & Aliases Invariant (Strict Pre-Approval)
- **Zero Silent Backward-Compatibility / Aliases**:
  - The agent MUST NEVER proactively create compatibility shims, re-exports, deprecated aliases, or legacy fallback wrappers (e.g., `pub use old_type as NewType`, compatibility function shims, or deprecated facades) on its own.
  - If a refactor breaks legacy APIs or changes naming, and there is a question of retaining an alias or backward-compatible adapter, the agent MUST explicitly inform and receive explicit acceptance (`accept`) from the USER BEFORE writing the code.
  - "Duplicate first, abstract second. Clean break over accidental coupling." Redundant aliases clutter API surface, IDE auto-completion, and code readability.

## 3. Strict Factual Comments (No Promises or Commitments)
- Comments in code, docstrings, commit messages, and design documents must remain strictly factual, technical, and descriptive of the current implementation.
- **Strictly Forbidden**:
  - Making unverified promises, forward-looking commitments, or promotional guarantees (e.g., "Will be implemented in next release", "Guaranteed to never fail under any circumstance", "Promises O(1) in future updates", "TODO: we will support XYZ soon").
  - Writing aspirational marketing language or speculative guarantees in code comments.
  - If a feature is not supported, state the exact current limitation or return an explicit typed error without promising future implementation.

## 4. Multi-Target Compilation Matrix Invariants (OS, Arch & CPU Features)

Velda Edge is engineered around an explicit, three-dimensional orthogonal compilation matrix:

```
+─────────────────────────────────────────────────────────────────────────────+
|               VELDA EDGE MULTI-TARGET COMPILATION MATRIX                   |
+─────────────────────────────────────────────────────────────────────────────+
| Dimension 1: Target OS (`target_os`)                                        |
|   ├─ "linux"     -> Production: AF_XDP, io_uring, splice, SO_REUSEPORT, BBR |
|   └─ not("linux")-> Workstation/Dev (macOS/BSD): POSIX sockets, stdlib IO   |
|                                                                             |
| Dimension 2: Target CPU Architecture (`target_arch`)                       |
|   ├─ "x86_64"    -> Intel/AMD 64-bit                                        |
|   ├─ "aarch64"   -> ARM64 (AWS Graviton, Apple Silicon, Ampere Altra)       |
|   └─ portable    -> Universal 64-bit ALU / SWAR fallback (RISC-V, etc.)     |
|                                                                             |
| Dimension 3: Target Microarchitecture & Features (`target-cpu` / `feature`) |
|   ├─ x86-64-v3   -> AVX2 (256-bit SIMD, 8 rules/cycle, BMI1/2, FMA)         |
|   ├─ x86-64-v1   -> SSE2 (128-bit SIMD, 4 rules/cycle, Baseline x86)        |
|   └─ aarch64     -> NEON / ARMv8 Crypto / 64-bit SWAR (2 rules/cycle)        |
+─────────────────────────────────────────────────────────────────────────────+
```

### 4.1 Target OS Invariants (`target_os`)
- **Linux Kernel Acceleration vs Developer Workstation Fallback**:
  - Production-grade Linux network stack optimizations (`AF_XDP`, `epoll`, `io_uring`, zero-copy `splice`, `SO_REUSEPORT`, `TCP_FASTOPEN`, `TCP_NOTSENT_LOWAT`, `TCP_DEFER_ACCEPT`, `BBR`, `UDP_GRO`/`GSO`, `sendmmsg`/`recvmmsg`, and core affinity `sched_setaffinity`) MUST be guarded strictly by `#[cfg(target_os = "linux")]`.
  - Every subsystem utilizing Linux-specific syscalls MUST provide a fully functional, compilation-clean fallback guarded by `#[cfg(not(target_os = "linux"))]` (utilizing standard Tokio runtime, POSIX BSD sockets, or standard user-space copies). This guarantees developer workstations (macOS / Darwin, BSD) compile and run unit tests with zero panics.
- **Strict Decoupling of Target OS from Target CPU Architecture**:
  - `target_os` (Linux kernel APIs / syscalls) MUST remain strictly decoupled from `target_arch` (`x86_64`, `aarch64`) and `target_feature`.
  - Code MUST NEVER assume `target_os = "linux"` implies `x86_64` (Linux runs natively on ARM64 and RISC-V), nor assume `target_arch = "x86_64"` implies Linux (macOS runs on x86_64).

### 4.2 Target CPU Architecture & Microarchitecture Invariants (`target_arch` & `target_feature`)
- **Compile-Time Target Matrix over Dynamic CPUID Sniffing**:
  - When building multi-version targets (`x86-64-v3`, `x86-64-v1`, `aarch64`), compile-time evaluation (`#[cfg(target_feature = "...")]` and `-C target-cpu=...`) MUST be prioritized over runtime CPUID sniffing (`is_x86_feature_detected!`).
  - Zero dynamic CPUID sniffing on hot request paths: LLVM dead-code elimination must prune non-applicable branches at compile time.
- **Mandatory Universal Portable Fallback (Non-x86 / ARM64 Invariant)**:
  - Every subsystem utilizing platform-specific SIMD vector intrinsics (e.g. `_mm256_...`, `_mm_...`, `avx2`, `sse2`, `sse4.2`) MUST provide an exhaustive, portable fallback branch for other architectures (`#[cfg(not(target_arch = "x86_64"))]` or `#[cfg(target_arch = "aarch64")]`).
  - Code MUST NEVER leave gaps where non-x86 architectures (such as `aarch64` AWS Graviton / Apple Silicon) have no matching implementation branch and fail compilation.
  - The portable fallback must utilize SWAR (SIMD Within A Register on 64-bit general ALU) or scalar operations, preserving architectural correctness without requiring x86 co-processors.
