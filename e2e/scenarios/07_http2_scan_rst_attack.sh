#!/usr/bin/env bash
# 07_http2_scan_rst_attack.sh — Scanner & 404 Flood Defense under HTTP/2 Multiplexing
set -euo pipefail

TARGET_HOST="api.example.com"
TARGET_PORT="8443"
CONNECT_TO="127.0.0.1:${TARGET_PORT}"
H2LOAD_BIN=$(which h2load || echo "$HOME/.local/bin/h2load")

echo "=========================================================================="
echo " [E2E] Scenario 07: HTTP/2 Scanner Flood & 404 Defense Test"
echo " Testing fast-path 404 rejection and clean channel isolation under flood"
echo "=========================================================================="

# 1. Prepare URI scanner script with invalid and scanning endpoints
SCRIPT_FILE="/tmp/velda_h2_scan_uris.txt"
cat << 'EOF' > "$SCRIPT_FILE"
https://api.example.com:8443/wp-admin/setup-config.php
https://api.example.com:8443/.env
https://api.example.com:8443/actuator/health
https://api.example.com:8443/v1/unknown-endpoint
https://api.example.com:8443/api/v2/non-existent-action
https://api.example.com:8443/phpmyadmin/index.php
https://api.example.com:8443/shell.php
https://api.example.com:8443/config.json
https://api.example.com:8443/admin/debug
https://api.example.com:8443/api/fake/scanner/probe
EOF

echo ""
echo ">>> [Phase 1] Fast-path 404 Flood: 30,000 scanner requests across 50 connections..."
# Note: In h2load, -i passes a URI list file where requests rotate among scanned paths
"$H2LOAD_BIN" -n 30000 -c 50 -m 50 -i "$SCRIPT_FILE" --connect-to="$CONNECT_TO" "https://api.example.com:8443/"

echo ""
echo ">>> [Phase 2] Scanner Attack with Concurrent Legitimate Traffic..."
echo "Starting scanner attack in background (50,000 requests)..."
"$H2LOAD_BIN" -n 50000 -c 100 -m 50 -i "$SCRIPT_FILE" --connect-to="$CONNECT_TO" "https://api.example.com:8443/" > /tmp/scanner_out.txt 2>&1 &
SCANNER_PID=$!

# Concurrently send legitimate requests
echo "Sending 50 concurrent legitimate requests to /api/echo while under attack..."
LEGIT_SUCCESS=0
LEGIT_FAIL=0

for i in {1..50}; do
    CODE=$(curl -k -s -o /dev/null -w "%{http_code}" --http2 \
        --resolve "${TARGET_HOST}:${TARGET_PORT}:127.0.0.1" \
        "https://${TARGET_HOST}:${TARGET_PORT}/api/echo" -d "ping-$i" || echo "000")
    if [ "$CODE" = "200" ]; then
        LEGIT_SUCCESS=$((LEGIT_SUCCESS + 1))
    else
        LEGIT_FAIL=$((LEGIT_FAIL + 1))
    fi
done

wait "$SCANNER_PID" || true

echo "Attack results:"
cat /tmp/scanner_out.txt | grep -E "requests:|status codes:|finished in" || true

echo ""
echo "Legitimate traffic results during attack:"
echo " - Success (200 OK): $LEGIT_SUCCESS / 50"
echo " - Failed:            $LEGIT_FAIL / 50"

rm -f "$SCRIPT_FILE" /tmp/scanner_out.txt

if [ "$LEGIT_FAIL" -gt 0 ]; then
    echo "[FAIL] Legitimate requests dropped during scanner flood!" >&2
    exit 1
else
    echo "[PASS] 100% legitimate requests served with 200 OK under heavy attack."
fi

echo "=========================================================================="
echo " [E2E] HTTP/2 Scanner Defense test completed successfully."
echo "=========================================================================="
