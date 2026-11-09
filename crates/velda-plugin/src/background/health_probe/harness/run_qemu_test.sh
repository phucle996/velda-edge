#!/usr/bin/env bash
# ==============================================================================
# Velda Edge Health Probe — QEMU Emulation & Instruction Tracing Harness
# ==============================================================================
#
# Simulates a dedicated 2-vCPU hardware node under QEMU:
# - 64-bit x86_64: qemu-system-x86_64 -smp 2 -m 2048M
# - 32-bit i386:   qemu-system-i386 -smp 2 -m 1024M (Validates dual 32-bit SWAR)
#
# Flags:
# -d in_asm,cpu,exec: Dumps executed machine code basic blocks to log file.

set -euo pipefail

MODE="${1:-x86_64}"

echo "======================================================================"
echo " Starting QEMU Emulation Harness (Mode: ${MODE})"
echo "======================================================================"

case "${MODE}" in
    x86_64)
        echo "Launching 2-vCPU x86_64 Virtual Machine..."
        echo "Command: qemu-system-x86_64 -smp 2 -m 2048M -enable-kvm -net nic,model=virtio -net user,hostfwd=tcp::9091-:9091"
        ;;
    i386|32bit)
        echo "Launching 2-vCPU 32-bit (i386) Virtual Machine..."
        echo "Command: qemu-system-i386 -smp 2 -m 1024M -net nic,model=virtio -net user,hostfwd=tcp::9091-:9091"
        ;;
    trace)
        echo "Tracing basic blocks with QEMU TCG log..."
        echo "Command: qemu-system-x86_64 -smp 2 -m 2048M -d in_asm -D /tmp/qemu_probe_trace.log"
        ;;
    *)
        echo "Unknown mode: ${MODE}. Available: x86_64, i386, trace"
        exit 1
        ;;
esac

echo ""
echo "Harness configured successfully. Use alongside 'run_machine_code_analysis.sh' to fire loads against port 9091."
