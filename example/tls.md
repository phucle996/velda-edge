# `tls.json` — Edge TLS & Certificates Specification

The `tls.json` file configures TLS termination, certificate keypairs, mutual TLS (mTLS) client verification, and ALPN negotiation for secure ingress listeners.

---

## 1. Architectural Mindset & Zero-IO Invariant

### 1.1 In-Memory Pre-Compiled Hot Path
In Velda Edge, TLS certificate parsing, ASN.1 decoding, and SNI trie indexing are strictly performed during compilation (`velda-sync`) or bootstrap (`velda-tls`). The request serving hot path never performs disk I/O, certificate decoding, or dynamic key parsing during connection handshakes.

### 1.2 Automatic SNI Extraction Invariant
In `tls.json`, the `sni` field is **omitted from JSON declarations**. During compilation:
1. `velda-sync` parses each X.509 certificate (`cert_pem`).
2. Extracts Subject Alternative Names (`dNSName` SANs) and Common Name (`CN`).
3. Compiles the verified SNI list directly into `tls.bin`.
4. Edge runtime indexes certificates into a lock-free SNI lookup trie in RAM.

This eliminates configuration drift between declared domain names and actual X.509 certificate identities.

### 1.3 Strict No-SNI Rejection Policy
Handshakes without an SNI extension or targeting an unrecognized hostname are rejected immediately during the TLS ClientHello phase before consuming downstream connection resources.

---

## 2. Schema Definition & Top-Level Fields

```json
{
  "schema_version": 1,
  "tls": [ /* array of TlsConfig objects */ ]
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `schema_version` | `integer` | Yes | Schema version. Must be `1`. |
| `tls` | `array` | Yes | List of TLS server certificate configurations. |

---

## 3. `TlsConfig` Object Specification

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `cert_pem` | `string` | Yes | PEM-encoded server certificate chain (end-entity certificate followed by intermediate CAs). |
| `key_pem` | `string` | Yes | PEM-encoded private key (PKCS#8 or SEC1 EC / PKCS#1 RSA). |
| `client_ca_pem` | `string` | No | PEM-encoded CA certificate for Mutual TLS (mTLS) client authentication. Set to `null` if mTLS is not required. |
| `versions` | `array` | Yes | Array of allowed TLS protocol versions (e.g., `["tls1.2", "tls1.3"]`). |
| `alpn` | `array` | Yes | Array of advertised Application-Layer Protocol Negotiation identifiers (e.g., `["h2", "http/1.1"]`). |

---

## 4. Key Configuration Details

### 4.1 Supported Cryptographic Algorithms
Velda Edge supports modern cryptographic ciphers and curves via `rustls`:
- **ECDSA**: P-256 (`secp256r1`) and P-384 (`secp384r1`).
- **Ed25519**: EdDSA high-speed curve.
- **RSA**: 2048-bit and 4096-bit key sizes.

### 4.2 Application-Layer Protocol Negotiation (`alpn`)
The `alpn` array defines the protocols offered during the TLS handshake:
- `["h2", "http/1.1"]`: Allows clients to negotiate HTTP/2 multiplexing, falling back to HTTP/1.1 keep-alive.
- `["h2"]`: Strictly enforces HTTP/2 (required for dedicated gRPC ingress listeners).
- `["h3"]`: Negotiated over QUIC (HTTP/3) transport.

### 4.3 Mutual TLS (`client_ca_pem`)
When `client_ca_pem` is provided:
1. The edge engine demands a client certificate during the TLS handshake (`SSL_VERIFY_PEER`).
2. The client certificate is validated against the specified CA chain.
3. If validation fails or no certificate is presented, the connection is terminated immediately.
4. Downstream request context receives verified peer identity attributes (`peer_cert_subject`, `peer_cert_issuer`).
