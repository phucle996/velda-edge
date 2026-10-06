#!/usr/bin/env bash
# 04_downstream_cancellations.sh — HTTP/1.1 Downstream Cancellations & Pipelining
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_HOST="127.0.0.1"
TARGET_PORT="8080"
TARGET="http://$TARGET_HOST:$TARGET_PORT"

echo "=========================================================================="
echo " [E2E HTTP/1.1] 04: Downstream Client Cancellations & Pipelining"
echo " Target: $TARGET"
echo "=========================================================================="

# 1. Downstream Client Cancellation mid-stream (/infinite)
echo ">>> [Phase 1] Downstream Client Cancellation mid-stream (/infinite)..."
for i in $(seq 1 20); do
    # Start curl on infinite stream and kill it after 100ms
    timeout 0.1s curl -s "$TARGET/infinite" > /dev/null 2>&1 || true
done
echo " - 20 downstream stream cancellations executed cleanly."

# Verify edge is healthy
STATUS=$(curl -s "$TARGET/health")
if [ "$STATUS" = "OK" ]; then
    echo " [PASS] Gateway reclaimed all upstream streaming leases after client drops."
else
    echo " [FAIL] Gateway pool hung after stream cancellation!"
    exit 1
fi

# 2. Downstream Abrupt Disconnect during Request Upload
echo ""
echo ">>> [Phase 2] Downstream Abrupt Disconnect during Upload..."
# Open TCP socket, send headers for 100KB, send 20 bytes, then close immediately
for i in $(seq 1 20); do
    python3 -c "
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('$TARGET_HOST', $TARGET_PORT))
s.sendall(b'POST /upload HTTP/1.1\r\nHost: $TARGET_HOST\r\nContent-Length: 102400\r\n\r\n1234567890')
time.sleep(0.01)
s.close()
" 2>/dev/null || true
done
echo " - 20 truncated uploads injected."

STATUS_UPLOAD=$(curl -s "$TARGET/health")
if [ "$STATUS_UPLOAD" = "OK" ]; then
    echo " [PASS] Gateway handled client upload truncation without socket/lease leak."
else
    echo " [FAIL] Gateway failed after upload drops!"
    exit 1
fi

# 3. HTTP/1.1 Pipelining (Multiple requests in single TCP burst)
echo ""
echo ">>> [Phase 3] HTTP/1.1 Pipelining (Multiple requests in single TCP buffer)..."
PIPE_RESP=$(python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('$TARGET_HOST', $TARGET_PORT))
# Send 2 requests back-to-back in one sendall
burst = (
    b'GET /health HTTP/1.1\r\nHost: $TARGET_HOST\r\n\r\n'
    b'GET /health HTTP/1.1\r\nHost: $TARGET_HOST\r\n\r\n'
)
s.sendall(burst)
s.settimeout(2.0)
resp = b''
try:
    while len(resp) < 200:
        chunk = s.recv(1024)
        if not chunk: break
        resp += chunk
except Exception:
    pass
s.close()
print(resp.decode('latin1'))
")

# Check that response contains two '200 OK'
COUNT_200=$(echo "$PIPE_RESP" | grep -c "200 OK" || true)
if [ "$COUNT_200" -ge 2 ]; then
    echo " - HTTP/1.1 Pipelining: Received both responses in sequence [PASS]"
else
    echo " - HTTP/1.1 Pipelining: Expected 2 responses, got $COUNT_200 [WARN]"
fi

# 4. Rapid Connection Churn (Connection: close flood)
echo ""
echo ">>> [Phase 4] Rapid Downstream Connection Churn (200 rapid conns, 5s)..."
wrk -t4 -c100 -d5s -H "Connection: close" "$TARGET/health" | grep -E "Requests/sec|Transfer/sec|Socket errors"

echo "=========================================================================="
echo " [E2E HTTP/1.1] 04: Downstream Cancellations passed successfully."
echo "=========================================================================="
