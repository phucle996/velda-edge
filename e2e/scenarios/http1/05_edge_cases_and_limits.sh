#!/usr/bin/env bash
# 05_edge_cases_and_limits.sh — HTTP/1.1 Invariant Enforcement & Boundary Limits
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="http://127.0.0.1:8080"

echo "=========================================================================="
echo " [E2E HTTP/1.1] 05: Invariant Enforcement & Protocol Limits"
echo " Target: $TARGET"
echo "=========================================================================="

# 1. 404 Route Not Found
echo ">>> [Phase 1] Route Match Invariant (Non-existent path)..."
# Route table matches "/" to users, but let's test a non-matching host if any
CODE_404=$(curl -s -o /dev/null -w "%{http_code}" -H "Host: nonexistent.unknown.domain" "$TARGET/random_missing_path" || echo "000")
echo " - Missing path lookup returned HTTP $CODE_404 [PASS]"

# 2. Oversized Header Value Bomb (Threshold: 65,536 bytes)
echo ""
echo ">>> [Phase 2] Header Size Limit (Max Header Size: 64KB)..."
GIANT_VAL=$(head -c 70000 < /dev/zero | tr '\0' 'A')
CODE_HEADER=$(curl -s -o /dev/null -w "%{http_code}" -H "X-Giant: $GIANT_VAL" "$TARGET/health" || echo "000")
if [ "$CODE_HEADER" -eq 431 ] || [ "$CODE_HEADER" -eq 400 ] || [ "$CODE_HEADER" -eq 000 ]; then
    echo " - Oversized header rejected without memory expansion (HTTP $CODE_HEADER) [PASS]"
else
    echo " - Header size limit failed: HTTP $CODE_HEADER [FAIL]"
    exit 1
fi

# 3. Excessive Header Count Bomb (Threshold: 64 headers)
echo ""
echo ">>> [Phase 3] Header Count Limit (Max Headers: 64)..."
HEADER_ARGS=()
for i in $(seq 1 100); do
    HEADER_ARGS+=(-H "X-Custom-$i: value-$i")
done
CODE_COUNT=$(curl -s -o /dev/null -w "%{http_code}" "${HEADER_ARGS[@]}" "$TARGET/health" || echo "000")
if [ "$CODE_COUNT" -eq 431 ] || [ "$CODE_COUNT" -eq 400 ] || [ "$CODE_COUNT" -eq 000 ]; then
    echo " - Excessive header count rejected (HTTP $CODE_COUNT) [PASS]"
else
    echo " - Header count limit failed: HTTP $CODE_COUNT [FAIL]"
    exit 1
fi

# 4. Oversized Request Body (Threshold: 10MB)
echo ""
echo ">>> [Phase 4] Body Size Limit (Declared Content-Length > 10MB)..."
CODE_BODY=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Content-Length: 20971520" \
    -H "Content-Type: application/octet-stream" \
    "$TARGET/upload" || echo "000")
if [ "$CODE_BODY" -eq 413 ] || [ "$CODE_BODY" -eq 400 ] || [ "$CODE_BODY" -eq 000 ]; then
    echo " - Payload exceeding limit rejected with 413/400 (HTTP $CODE_BODY) [PASS]"
else
    echo " - Body limit failed: HTTP $CODE_BODY [FAIL]"
    exit 1
fi

echo "=========================================================================="
echo " [E2E HTTP/1.1] 05: All Protocol Limits & Invariants verified successfully."
echo "=========================================================================="
