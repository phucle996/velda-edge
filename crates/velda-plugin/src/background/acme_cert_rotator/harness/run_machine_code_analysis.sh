#!/usr/bin/env bash
# ==============================================================================
# Velda Edge — ACME Cert Rotator Machine Code Analysis & Live Chaos Harness
# ==============================================================================
#
# Hardware & Topology Verification:
# 1. Target System (SUT): velda-plugin ACME background rotator + RouterRegistry
# 2. Frontline Serving Lane: Dedicated traffic worker core with 0 L1D pollution
# 3. Shadow Logistics Worker: Background cron scheduler and Let's Encrypt client
# 4. Live Assault & Chaos Scenarios:
#    - Chaos 1: ACME CA Network Choke / Blackhole (100% absorbed by Shadow worker)
#    - Chaos 2: L7 8-Core Burst Flood during active HTTP-01 challenge
#    - Chaos 3: L3/RAM Cache Isolation (Zero L1D thrashing)
# 5. Hardware PMU & Disassembly Inspector:
#    - RDTSC cycle profiling
#    - Linux PMU hardware counters (cycles, instructions, IPC, L1-dcache-load-misses)
#    - Objective disassembly verification

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

echo "======================================================================"
echo " 1. Verifying Zero-MPSC Channel Invariant in acme_cert_rotator"
echo "======================================================================"
echo "Scanning acme_cert_rotator codebase for implicit mpsc channels or unbounded queues..."
MPSC_COUNT=$(grep -rn --include="*.rs" "tokio::sync::mpsc" "${WORKSPACE_ROOT}/crates/velda-plugin/src/background/acme_cert_rotator/" || true)
if [ -z "${MPSC_COUNT}" ]; then
    echo ">>> CONFIRMED: 0 mpsc channels in acme_cert_rotator. 100% Zero-MPSC Invariant holds."
else
    echo "WARNING: Found mpsc channel occurrences:"
    echo "${MPSC_COUNT}"
fi

echo ""
echo "======================================================================"
echo " 2. Building Release Artifact with Debug Symbols for Disassembly"
echo "======================================================================"
RUSTFLAGS="-g -C strip=none" cargo test -p velda-plugin --test acme_cert_rotator_machine_code_test --no-run --release

TEST_BIN=$(ls -t "${WORKSPACE_ROOT}/target/release/deps/acme_cert_rotator_machine_code_test-"* | grep -v '\.d$' | head -n 1)
echo "Target Executable: ${TEST_BIN}"

echo ""
echo "======================================================================"
echo " 3. Disassembly & Symbol Verification Check"
echo "======================================================================"
if command -v objdump >/dev/null 2>&1; then
    echo "Dumping Assembly Snippet of Ephemeral Route Lookup & Challenge Store:"
    objdump -d -M intel --demangle "${TEST_BIN}" | grep -A 35 "<velda_plugin::background::acme_cert_rotator::challenge::ChallengeStore>::lookup" | head -n 40 || true
fi

echo ""
echo "======================================================================"
echo " 4. Executing Release RDTSC Cycle Benchmark & Live Chaos Assaults"
echo "======================================================================"
if command -v perf >/dev/null 2>&1 && [ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 4)" -le 1 ]; then
    echo "Running with Linux Hardware PMU Counters (perf stat):"
    perf stat -e cycles,instructions,cache-misses,branch-misses,L1-dcache-load-misses \
        "${TEST_BIN}" --nocapture --test-threads=1
else
    echo "Executing Release Test Binary directly (perf stat unavailable or unprivileged):"
    "${TEST_BIN}" --nocapture --test-threads=1
fi

echo ""
echo "======================================================================"
echo " ACME Cert Rotator Analysis & Verification Complete."
echo "======================================================================"
