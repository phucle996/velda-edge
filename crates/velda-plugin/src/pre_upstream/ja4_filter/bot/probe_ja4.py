#!/usr/bin/env python3
"""
Direct KVM TLS/JA4 request-spoofing probe for `ja4_filter`.

Target defaults to `192.168.122.10:8443` (`velda-gw` KVM).
Checks two distinct OpenSSL ClientHello cipher shapes spoofing Chrome headers:
  1. Default Python OpenSSL cipher list (`--expect-denied-default` asserts 403 when listed in `ja4_filter.deny`).
  2. Shifted TLS 1.2 cipher list (`ECDHE-ECDSA-AES128-GCM-SHA256`) yielding a distinct JA4.
"""

import ssl
import socket
import sys


CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def send_tls_req(host: str, port: int, cipher_spec: str | None = None, tls12_only: bool = False) -> int:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_alpn_protocols(["http/1.1"])
    if cipher_spec:
        ctx.set_ciphers(cipher_spec)
    if tls12_only:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2

    raw = socket.create_connection((host, port), timeout=5.0)
    with ctx.wrap_socket(raw, server_hostname="velda-gw") as tls:
        lines = [
            "GET /ping HTTP/1.1",
            f"Host: {host}:{port}",
            "Connection: close",
            f"User-Agent: {CHROME_UA}",
            "Accept-Language: en-US,en;q=0.9",
            "Sec-Fetch-Site: none",
            "Sec-Fetch-Mode: navigate",
            "Sec-Fetch-Dest: document",
        ]
        tls.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
        head = tls.recv(2048).decode("utf-8", errors="replace").splitlines()
    status_line = head[0] if head else ""
    parts = status_line.split()
    return int(parts[1]) if len(parts) >= 2 else 0


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    target = args[0] if args else "192.168.122.10:8443"
    host, port_s = target.rsplit(":", 1)
    port = int(port_s)

    code1 = send_tls_req(host, port)
    print(f"[JA4 Probe 1] Default Python TLS + Chrome headers -> status={code1}")
    if "--expect-denied-default" in flags:
        assert code1 == 403, f"Expected 403 from ja4_filter, got {code1}"

    code2 = send_tls_req(
        host,
        port,
        cipher_spec="ECDHE-ECDSA-AES128-GCM-SHA256",
        tls12_only=True,
    )
    print(f"[JA4 Probe 2] Cipher-shifted TLS 1.2 + Chrome headers -> status={code2}")
    assert code2 == 200, f"Expected allowed JA4 variant to pass (200), got {code2}"


if __name__ == "__main__":
    main()
