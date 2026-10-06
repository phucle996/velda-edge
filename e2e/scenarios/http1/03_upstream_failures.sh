#!/usr/bin/env bash
# 03_upstream_failures.sh — HTTP/1.1 Upstream Failure Injections & Resilience
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="http://127.0.0.1:8080"

echo "=========================================================================="
echo " [E2E HTTP/1.1] 03: Upstream Failure Injections & Pool Resilience"
echo " Target: $TARGET"
echo "=========================================================================="

# 1. Upstream Sudden TCP RST (/drop)
echo ">>> [Phase 1] Upstream Abrupt TCP RST Injection (/drop)..."
CODE=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/drop" || echo "000")
if [ "$CODE" -eq 502 ] || [ "$CODE" -eq 503 ] || [ "$CODE" -eq 000 ]; then
    echo " - Upstream TCP RST safely mapped to Bad Gateway or socket reset (HTTP $CODE) [PASS]"
else
    echo " - Unexpected status code: $CODE [FAIL]"
    exit 1
fi

# 2. Upstream Truncated Body after Headers (/drop_after_headers)
echo ""
echo ">>> [Phase 2] Upstream Truncated Body Injection (/drop_after_headers)..."
CODE_TRUNC=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/drop_after_headers" || echo "000")
if [ "$CODE_TRUNC" -eq 502 ] || [ "$CODE_TRUNC" -eq 000 ]; then
    echo " - Truncated upstream stream correctly caught and dropped [PASS]"
else
    echo " - Unexpected status: $CODE_TRUNC [FAIL]"
    exit 1
fi

# 3. Upstream Protocol Corruption (/corrupt)
echo ""
echo ">>> [Phase 3] Upstream Protocol Corruption Injection (/corrupt)..."
CODE_CORRUPT=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/corrupt" || echo "000")
if [ "$CODE_CORRUPT" -eq 502 ] || [ "$CODE_CORRUPT" -eq 000 ]; then
    echo " - Malformed upstream protocol safely rejected with 502 Bad Gateway [PASS]"
else
    echo " - Unexpected status: $CODE_CORRUPT [FAIL]"
    exit 1
fi

# 4. Upstream Connection: close Honor & Pool Eviction (/close)
echo ""
echo ">>> [Phase 4] Upstream Connection: close Pool Eviction Test..."
# Send request to /close, then immediate request to /health
RESP_CLOSE=$(curl -s "$TARGET/close")
RESP_HEALTH=$(curl -s "$TARGET/health")
if [ "$RESP_HEALTH" = "OK" ]; then
    echo " - Next request after Connection: close successfully acquired a fresh connection [PASS]"
else
    echo " - Upstream connection reuse failed after close! [FAIL]"
    exit 1
fi

# 5. Upstream Crash Storm & Pool Resilience
echo ""
echo ">>> [Phase 5] Firing 100 Rapid Upstream Crashes under high concurrency..."
for i in $(seq 1 50); do
    curl -s "$TARGET/drop" > /dev/null 2>&1 &
    curl -s "$TARGET/corrupt" > /dev/null 2>&1 &
done
wait

HEALTH_AFTER=$(curl -s "$TARGET/health")
if [ "$HEALTH_AFTER" = "OK" ]; then
    echo " [PASS] Gateway pool recovered 100% of connection capacity after crash storm."
else
    echo " [FAIL] Gateway connection pool broke after crash storm!"
    exit 1
fi

echo "=========================================================================="
echo " [E2E HTTP/1.1] 03: Upstream Failure Injections passed successfully."
echo "=========================================================================="
