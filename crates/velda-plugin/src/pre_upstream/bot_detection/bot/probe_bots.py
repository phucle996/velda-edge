#!/usr/bin/env python3
"""
Direct KVM HTTP/1.1 request spoofing & zero-false-positive probes for `bot_detection`.

Target defaults to `192.168.122.10:8443` (`velda-gw` KVM).
Uses distinct OpenSSL cipher specs so honest clients and spoofed bots receive distinct
`(client_ip, ja4)` keys even when fired from a single observer VM (`velda-obs`).
"""

import socket
import ssl
import sys
import time


CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def send_https_request(host: str, port: int, cipher_spec: str | None, headers: dict[str, str]):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_alpn_protocols(["http/1.1"])
    if cipher_spec:
        ctx.set_ciphers(cipher_spec)

    t0 = time.perf_counter()
    raw = socket.create_connection((host, port), timeout=5.0)
    with ctx.wrap_socket(raw, server_hostname="velda-gw") as tls:
        req_lines = ["GET /ping HTTP/1.1", f"Host: {host}:{port}", "Connection: close"]
        for k, v in headers.items():
            req_lines.append(f"{k}: {v}")
        tls.sendall(("\r\n".join(req_lines) + "\r\n\r\n").encode("ascii"))
        chunks = []
        while True:
            data = tls.recv(4096)
            if not data:
                break
            chunks.append(data)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    head = b"".join(chunks).decode("utf-8", errors="replace").splitlines()
    status_line = head[0] if head else ""
    parts = status_line.split()
    return (int(parts[1]) if len(parts) >= 2 else 0), round(elapsed_ms, 1)


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "192.168.122.10:8443"
    host, port_s = target.rsplit(":", 1)
    port = int(port_s)

    honest_ciphers = "DEFAULT:@SECLEVEL=1"
    bot_ciphers = "ECDHE-ECDSA-AES128-GCM-SHA256"

    print(f"=== [Python KVM Probe 1] Honest API + Firefox baselines -> {target} ===")
    for ua_name, extra in [
        ("velda-healthcheck/1.0", {"Accept": "*/*"}),
        (
            "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
            {
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document",
            },
        ),
    ]:
        code, rtt = send_https_request(host, port, honest_ciphers, {"User-Agent": ua_name, **extra})
        print(f"  ua={ua_name[:28]:28s} status={code} rtt={rtt}ms")
        assert code == 200 and rtt < 80.0, f"Unexpected block/tarpit on honest client: status={code} rtt={rtt}ms"

    print("\n=== [Python KVM Probe 2] Script claiming Chrome 131 UA -> expect progressive tarpit ===")
    rtts = []
    final_code = 0
    for i in range(5):
        code, rtt = send_https_request(
            host,
            port,
            bot_ciphers,
            {
                "User-Agent": CHROME_UA,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document",
            },
        )
        print(f"  strike#{i} status={code} rtt={rtt}ms")
        rtts.append(rtt)
        final_code = code

    assert rtts[0] >= 80.0, f"Expected initial tarpit >= 80ms, got {rtts}"
    assert rtts[2] > rtts[0] or final_code == 403, f"Expected escalation or 403, got {rtts} final={final_code}"

    print("\n=== [Python KVM Probe 3] Honest baseline still unaffected (distinct JA4) ===")
    code, rtt = send_https_request(host, port, honest_ciphers, {"User-Agent": "velda-healthcheck/1.0"})
    print(f"  honest status={code} rtt={rtt}ms")
    assert code == 200 and rtt < 80.0


if __name__ == "__main__":
    main()
