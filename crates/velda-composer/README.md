# velda-composer

`velda-composer` is the **protocol composition and runtime coordination boundary** of Velda Edge.

It sits between the raw transport layer (`velda-transport`) and application protocol engines (`velda-http`, `velda-tls`):

```text
velda-transport
      │
      │ L7Handoff
      ▼
velda-composer
      │
      ├── TLS processing (velda-tls)
      │      ├── handshake
      │      ├── SNI
      │      └── ALPN
      │
      ▼
Application protocol (velda-http)
      │
      ├── parse
      ├── validate
      └── serialize
```

---

## 1. Core Rule

> **"Composer coordinates. Components implement."**

`velda-composer` does **NOT**:
- Parse HTTP requests or responses
- Implement TLS handshake or encryption mechanics
- Perform route matching or load balancing
- Implement WAF, authentication, or rate limiting
- Parse raw configuration JSON files

Instead, `velda-composer` answers a single question:
> *"Which protocol-processing components does this connection need, and in what order are they executed?"*

---

## 2. Structure

```text
crates/velda-composer/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs          # Re-exports core composer abstractions
│   ├── composer.rs     # Composer coordinator & ComposedStream resolution
│   ├── config.rs       # CompiledListenerComposition in RAM
│   ├── context.rs      # ComposerContext with connection, SNI, and ALPN metadata
│   └── error.rs        # Structured ComposerError classification
└── tests/
    └── handoff_test.rs # Integration test verifying L7Handoff from TrafficEngine
```

---

## 3. Verification

```bash
cargo fmt --check
cargo clippy -p velda-composer --all-targets --all-features -- -D warnings
cargo test -p velda-composer
```
