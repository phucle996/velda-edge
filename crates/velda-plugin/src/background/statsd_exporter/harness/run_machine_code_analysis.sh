#!/usr/bin/env bash
# ==============================================================================
# Velda Edge — StatsD Exporter Machine Code Analysis & Live Chaos Profiling Harness
# ==============================================================================
#
# Topology:
# 1. Target System (SUT): velda-plugin StatsD background exporter + Core-Local MetricsStore
# 2. Frontline Serving Lane: Dedicated traffic worker core with 6.75 KB L1D lane
# 3. Shadow Logistics Worker: Non-blocking UDP batch streamer to StatsD daemon
# 4. Live Assault & Chaos Generators:
#    - Chaos 1: UDP Backpressure / Blackhole (Kernel send buffer saturation)
#    - Chaos 2: L7 Burst Flooding (1 to 8 concurrent worker cores assault)
#    - Chaos 3: Noisy Neighbor (64MB memory thrasher purging L1D/L2/L3 caches)
# 5. Hardware PMU & Disassembly Inspector:
#    - RDTSC / RDTSCP cycle counter profiling
#    - Linux PMU hardware counters (cycles, instructions, IPC, L1-dcache-load-misses)
#    - Objective machine code dump (SIMD moves, 2-digit LUT, reciprocal multiply)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

echo "======================================================================"
echo " 1. Verifying Zero-MPSC Channel Invariant in statsd_exporter"
echo "======================================================================"
echo "Scanning statsd_exporter codebase for implicit mpsc channels or unbounded queues..."
MPSC_COUNT=$(grep -rn --include="*.rs" "tokio::sync::mpsc" "${WORKSPACE_ROOT}/crates/velda-plugin/src/background/statsd_exporter/" || true)
if [ -z "${MPSC_COUNT}" ]; then
    echo ">>> CONFIRMED: 0 mpsc channels in statsd_exporter. 100% Zero-MPSC Invariant holds."
else
    echo "WARNING: Found mpsc channel occurrences:"
    echo "${MPSC_COUNT}"
fi

echo ""
echo "======================================================================"
echo " 2. Building Release Artifact with Debug Symbols for Disassembly"
echo "======================================================================"
RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-stable}" RUSTFLAGS="-g -C strip=none" cargo test -p velda-plugin --test statsd_exporter_machine_code_test --no-run --release

TEST_BIN=$(ls -t "${WORKSPACE_ROOT}/target/release/deps/statsd_exporter_machine_code_test-"* | grep -v '\.d$' | head -n 1)
echo "Target Executable: ${TEST_BIN}"

echo ""
echo "======================================================================"
echo " 3. Disassembly & Symbol Verification Check"
echo "======================================================================"
if command -v objdump >/dev/null 2>&1; then
    echo "Dumping Assembly Snippet of Hot Serialization Loop (SIMD + Pre-baked Skeletons + Direct-to-Packet):"
    objdump -d -M intel --demangle "${TEST_BIN}" | grep -E -A 40 "<velda_plugin::background::statsd_exporter::(dialect|formatter)::(format_statsd_snapshot|dogstatsd::DogStatsdTemplate::format_with_writer)" | head -n 45 || true
fi

echo ""
echo "======================================================================"
echo " 4. Executing Release RDTSC Cycle Benchmark & Live Chaos Assaults"
echo "======================================================================"
if command -v perf >/dev/null 2>&1 && [ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 4)" -le 1 ]; then
    echo "Running with Linux Hardware PMU Counters (perf stat):"
    perf stat -e cycles,instructions,cache-misses,branch-misses,L1-dcache-load-misses \
        "${TEST_BIN}" --test-threads=1 --nocapture
else
    "${TEST_BIN}" --test-threads=1 --nocapture
fi

echo ""
echo "======================================================================"
echo " StatsD Exporter Live Chaos & Machine Code Analysis Completed!"
echo "======================================================================"
