#!/usr/bin/env bash
# 08_http1_streaming_stress.sh — HTTP/1.1 Chunked Transfer, Large Bodies & Streaming Backpressure
set -euo pipefail

TARGET_BASE="${1:-http://127.0.0.1:8080}"
WRK_BIN=$(which wrk || echo "$HOME/.local/bin/wrk")

echo "=========================================================================="
echo " [E2E] Scenario 08: HTTP/1.1 Streaming, Chunked & Large Payload Stress"
echo " Target: $TARGET_BASE"
echo "=========================================================================="

# 1. Verify endpoint health
if ! curl -sf -m 2 "$TARGET_BASE/health" > /dev/null 2>&1; then
    echo "[Error] Gateway $TARGET_BASE is not reachable!" >&2
    exit 1
fi

echo ""
echo ">>> [Phase 1] Chunked Streaming & Backpressure: 100 conns, 5 chunks per stream, 10s..."
"$WRK_BIN" -t4 -c100 -d10s --latency "$TARGET_BASE/chunked?count=5&interval_ms=5"

echo ""
echo ">>> [Phase 2] Large Payload Download: 200 conns, 64KB body download, 10s..."
"$WRK_BIN" -t8 -c200 -d10s --latency "$TARGET_BASE/large?size_kb=64"

echo ""
echo ">>> [Phase 3] Extreme Large Payload: 100 conns, 256KB body download, 10s..."
"$WRK_BIN" -t8 -c100 -d10s --latency "$TARGET_BASE/large?size_kb=256"

echo ""
echo ">>> [Phase 4] Streaming Upload Body: 50 conns, POST 32KB payload, 10s..."
UPLOAD_PAYLOAD_FILE="/tmp/velda_upload_32k.bin"
dd if=/dev/urandom of="$UPLOAD_PAYLOAD_FILE" bs=1024 count=32 status=none

LUA_UPLOAD_SCRIPT="/tmp/velda_wrk_upload.lua"
cat << 'EOF' > "$LUA_UPLOAD_SCRIPT"
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/octet-stream"
local f = io.open("/tmp/velda_upload_32k.bin", "rb")
if f then
    wrk.body = f:read("*all")
    f:close()
end
EOF

"$WRK_BIN" -t4 -c50 -d10s -s "$LUA_UPLOAD_SCRIPT" --latency "$TARGET_BASE/upload"
rm -f "$UPLOAD_PAYLOAD_FILE" "$LUA_UPLOAD_SCRIPT"

echo ""
echo ">>> [Phase 5] Upstream Abrupt TCP Drop (RST) Resilience Test..."
echo "Firing 100 abrupt drops to test connection teardown..."
DROP_SUCCESS=0
for i in {1..100}; do
    # When upstream sends TCP RST, curl should get 502 Bad Gateway or empty reply from gateway
    CODE=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET_BASE/drop" || echo "dropped")
    if [ "$CODE" = "502" ] || [ "$CODE" = "dropped" ] || [ "$CODE" = "000" ]; then
        DROP_SUCCESS=$((DROP_SUCCESS + 1))
    fi
done
echo "Handled $DROP_SUCCESS / 100 abrupt drops cleanly."

# Verify gateway is still 100% healthy
HEALTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET_BASE/health")
if [ "$HEALTH_CODE" = "200" ]; then
    echo "[PASS] Gateway remains fully responsive (200 OK) after upstream RST storm."
else
    echo "[FAIL] Gateway degraded after RST storm (status: $HEALTH_CODE)" >&2
    exit 1
fi

echo "=========================================================================="
echo " [E2E] HTTP/1.1 Streaming & Payload Stress completed successfully."
echo "=========================================================================="
