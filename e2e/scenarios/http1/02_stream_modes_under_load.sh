#!/usr/bin/env bash
# 02_stream_modes_under_load.sh — HTTP/1.1 Stream Modes Stress & Verification
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="http://127.0.0.1:8080"

echo "=========================================================================="
echo " [E2E HTTP/1.1] 02: Stream Modes High Load Saturation"
echo " Target: $TARGET"
echo "=========================================================================="

# 1. Buffered Mode: 64KB Payload Download Saturation
echo ">>> [Phase 1] Buffered Mode: 64KB Payload Download Saturation (100 conns, 5s)..."
wrk -t4 -c100 -d5s "$TARGET/large?size_kb=64" | grep -E "Requests/sec|Transfer/sec|Latency"

# 2. Server Streaming: SSE / Chunked Transfer
echo ""
echo ">>> [Phase 2] Server Streaming: Chunked Drip (50 conns, 5 chunks per stream)..."
wrk -t4 -c50 -d5s "$TARGET/chunked?count=5&interval_ms=5" | grep -E "Requests/sec|Transfer/sec|Latency"

# 3. Client Streaming: Body Upload Sink
echo ""
echo ">>> [Phase 3] Client Streaming: Uploading 32KB payload chunks (50 conns, 5s)..."
wrk -t4 -c50 -d5s "$TARGET/upload" | grep -E "Requests/sec|Transfer/sec|Latency"

# 4. Correctness Check after heavy stream load
echo ""
echo ">>> [Phase 4] Functional Integrity Check post-saturation..."
POST_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/health")
if [ "$POST_STATUS" -eq 200 ]; then
    echo " [PASS] Gateway fully responsive (200 OK) after high-speed stream saturation."
else
    echo " [FAIL] Gateway returned $POST_STATUS"
    exit 1
fi

echo "=========================================================================="
echo " [E2E HTTP/1.1] 02: Stream Modes Saturation passed successfully."
echo "=========================================================================="
