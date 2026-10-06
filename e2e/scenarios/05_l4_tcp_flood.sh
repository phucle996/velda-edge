#!/usr/bin/env bash
# 05_l4_tcp_flood.sh — L4 Raw TCP High-Speed Bidirectional Flood & Splice Saturation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TARGET_ADDR="${1:-127.0.0.1:9000}"
BLASTER_BIN="$ROOT_DIR/target/release/velda-tcp-blaster"

echo "=========================================================================="
echo " [E2E] Scenario 05: L4 Raw TCP High-Speed Bidirectional Flood & Splice"
echo " Target: $TARGET_ADDR"
echo "=========================================================================="

if [ ! -x "$BLASTER_BIN" ]; then
    echo "[E2E] Building velda-tcp-blaster..."
    (cd "$ROOT_DIR/e2e/backend" && go build -o "$BLASTER_BIN" ./cmd/tcp-blaster)
fi

# Quick connectivity test
if ! echo "PING" | nc -w 2 "${TARGET_ADDR%:*}" "${TARGET_ADDR#*:}" > /dev/null 2>&1; then
    echo "[Error] Target $TARGET_ADDR is not reachable!" >&2
    exit 1
fi

echo ""
echo ">>> [Phase 1] Moderate Concurrency: 100 connections, 1KB messages, 5s..."
"$BLASTER_BIN" -target "$TARGET_ADDR" -conns 100 -size 1024 -duration 5s

echo ""
echo ">>> [Phase 2] High Concurrency Saturation: 500 connections, 1KB messages, 10s..."
"$BLASTER_BIN" -target "$TARGET_ADDR" -conns 500 -size 1024 -duration 10s

echo ""
echo ">>> [Phase 3] Extreme Connection Storm: 1,000 connections, 1KB messages, 10s..."
"$BLASTER_BIN" -target "$TARGET_ADDR" -conns 1000 -size 1024 -duration 10s

echo ""
echo ">>> [Phase 4] Bandwidth Pipe Saturation: 200 connections, 16KB payload chunks, 10s..."
"$BLASTER_BIN" -target "$TARGET_ADDR" -conns 200 -size 16384 -duration 10s

echo ""
echo "=========================================================================="
echo " [E2E] L4 TCP Flood test completed successfully."
echo "=========================================================================="
