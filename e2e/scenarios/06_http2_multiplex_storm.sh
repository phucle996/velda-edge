#!/usr/bin/env bash
# 06_http2_multiplex_storm.sh — HTTP/2 Deep Multiplexing & Stream Storm Test
set -euo pipefail

TARGET_HOST="api.example.com"
TARGET_PORT="8443"
TARGET_PATH="/api/echo"
TARGET_URL="https://${TARGET_HOST}:${TARGET_PORT}${TARGET_PATH}"
CONNECT_TO="127.0.0.1:${TARGET_PORT}"

H2LOAD_BIN=$(which h2load || echo "$HOME/.local/bin/h2load")

if [ ! -x "$H2LOAD_BIN" ]; then
    echo "[Error] h2load not found at $H2LOAD_BIN" >&2
    exit 1
fi

echo "=========================================================================="
echo " [E2E] Scenario 06: HTTP/2 Deep Multiplexing Storm Test"
echo " Target:     $TARGET_URL"
echo " Connect-To: $CONNECT_TO"
echo " Tool:       $H2LOAD_BIN"
echo "=========================================================================="

# Quick reachability check with curl
if ! curl -k -sf -m 2 --http2 --resolve "${TARGET_HOST}:${TARGET_PORT}:127.0.0.1" "$TARGET_URL" > /dev/null 2>&1; then
    echo "[Error] Target $TARGET_URL is not responding to HTTP/2 requests!" >&2
    exit 1
fi

echo ""
echo ">>> [Phase 1] Moderate Multiplexing: 50 connections, 20 streams/conn, 20,000 requests..."
"$H2LOAD_BIN" -n 20000 -c 50 -m 20 --connect-to="$CONNECT_TO" "$TARGET_URL"

echo ""
echo ">>> [Phase 2] Deep Multiplexing Storm: 100 connections, 100 streams/conn (10,000 in-flight), 50,000 requests..."
"$H2LOAD_BIN" -n 50000 -c 100 -m 100 --connect-to="$CONNECT_TO" "$TARGET_URL"

echo ""
echo ">>> [Phase 3] Extreme Concurrency: 200 connections, 128 streams/conn (25,600 in-flight), 100,000 requests..."
"$H2LOAD_BIN" -n 100000 -c 200 -m 128 --connect-to="$CONNECT_TO" "$TARGET_URL"

echo ""
echo ">>> [Phase 4] Sustained Multiplexing Load: 100 connections, 64 streams/conn for 15s..."
"$H2LOAD_BIN" -D 15s -c 100 -m 64 --connect-to="$CONNECT_TO" "$TARGET_URL"

echo ""
echo "=========================================================================="
echo " [E2E] HTTP/2 Multiplexing Storm completed successfully."
echo "=========================================================================="
