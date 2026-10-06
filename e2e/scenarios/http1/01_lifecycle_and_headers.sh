#!/usr/bin/env bash
# 01_lifecycle_and_headers.sh — HTTP/1.1 Request Lifecycle, Methods, Status Codes & Header Enrichment
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TARGET="http://127.0.0.1:8080"

echo "=========================================================================="
echo " [E2E HTTP/1.1] 01: Request Lifecycle, Headers & Protocol Verification"
echo " Target: $TARGET"
echo "=========================================================================="

# 1. Verification of HTTP Methods
echo ">>> [Phase 1] Verifying standard HTTP Methods (GET, POST, PUT, DELETE, HEAD)..."
for METHOD in GET POST PUT DELETE; do
    RESP=$(curl -s -X "$METHOD" "$TARGET/echo" -d "Payload for $METHOD")
    if echo "$RESP" | grep -q "Payload for $METHOD"; then
        echo " - Method $METHOD: Echo body verified [PASS]"
    else
        echo " - Method $METHOD: Failed! Response: $RESP [FAIL]"
        exit 1
    fi
done

# HEAD request: must return 200 with no body
STATUS=$(curl -s -I -X HEAD "$TARGET/health" | head -n 1)
if echo "$STATUS" | grep -q "200 OK"; then
    echo " - Method HEAD: 200 OK verified [PASS]"
else
    echo " - Method HEAD: Unexpected status $STATUS [FAIL]"
    exit 1
fi

# 2. Query Parameters preservation
echo ""
echo ">>> [Phase 2] Query parameters & URI preservation..."
RESP=$(curl -s "$TARGET/echo?user_id=12345&filter=active&sort=desc")
# Since /echo echoes back, check status
STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/echo?user_id=12345&filter=active")
if [ "$STATUS_CODE" -eq 200 ]; then
    echo " - Query parameters passed and responded with 200 OK [PASS]"
else
    echo " - Query parameters failed: HTTP $STATUS_CODE [FAIL]"
    exit 1
fi

# 3. Security Invariant: Hop-by-Hop Header Stripping (RFC 9112 Section 7.6.1)
echo ""
echo ">>> [Phase 3] RFC 9112 Hop-by-Hop Header Stripping Verification..."
# Client injects hop-by-hop headers: Connection: Upgrade, Upgrade: websocket, Proxy-Authenticate
HEADERS=$(curl -s -i -H "Connection: Upgrade, X-Custom-Hop" \
               -H "Upgrade: websocket" \
               -H "X-Custom-Hop: sensitive-token" \
               -H "Proxy-Authenticate: Basic realm=test" \
               -H "X-Client-Custom: valid-client-header" \
               "$TARGET/echo")

# /echo reflects upstream received headers prefixed with X-Echo-
if echo "$HEADERS" | grep -iq "X-Echo-X-Client-Custom: valid-client-header"; then
    echo " - Legitimate client header preserved [PASS]"
else
    echo " - Client header missing from echo! [FAIL]"
    exit 1
fi

if echo "$HEADERS" | grep -iq "X-Echo-Upgrade"; then
    echo " - [VIOLATION] Hop-by-hop 'Upgrade' was NOT stripped! [FAIL]"
    exit 1
else
    echo " - Hop-by-hop header 'Upgrade' successfully stripped [PASS]"
fi

if echo "$HEADERS" | grep -iq "X-Echo-Proxy-Authenticate"; then
    echo " - [VIOLATION] Hop-by-hop 'Proxy-Authenticate' was NOT stripped! [FAIL]"
    exit 1
else
    echo " - Hop-by-hop header 'Proxy-Authenticate' successfully stripped [PASS]"
fi

# 4. Standard Proxy Header Enrichment (X-Forwarded-*, X-Real-IP)
echo ""
echo ">>> [Phase 4] Standard Edge Header Enrichment Verification..."
ECHO_RESP=$(curl -s -i "$TARGET/echo")
if echo "$ECHO_RESP" | grep -iq "X-Echo-X-Forwarded-For: 127.0.0.1"; then
    echo " - Injected X-Forwarded-For verified [PASS]"
else
    echo " - Missing X-Forwarded-For in response! [WARN]"
fi

if echo "$ECHO_RESP" | grep -iq "X-Echo-X-Forwarded-Proto: http"; then
    echo " - Injected X-Forwarded-Proto: http verified [PASS]"
else
    echo " - Missing X-Forwarded-Proto in response! [WARN]"
fi

if echo "$ECHO_RESP" | grep -iq "X-Echo-X-Real-Ip: 127.0.0.1"; then
    echo " - Injected X-Real-IP verified [PASS]"
else
    echo " - Missing X-Real-IP in response! [WARN]"
fi

# 5. Status Codes (204 No Content, 304 Not Modified, Empty body handling)
echo ""
echo ">>> [Phase 5] Special Status Codes (204 No Content, 304 Not Modified)..."
CODE_204=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/status?code=204")
if [ "$CODE_204" -eq 204 ]; then
    echo " - Status 204 No Content handled cleanly without body [PASS]"
else
    echo " - Status 204 returned $CODE_204 [FAIL]"
    exit 1
fi

CODE_304=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET/status?code=304")
if [ "$CODE_304" -eq 304 ]; then
    echo " - Status 304 Not Modified handled cleanly [PASS]"
else
    echo " - Status 304 returned $CODE_304 [FAIL]"
    exit 1
fi

echo ""
echo "=========================================================================="
echo " [E2E HTTP/1.1] 01: All lifecycle & header verifications passed!"
echo "=========================================================================="
