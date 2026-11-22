# HTTPS Redirect (`https_redirect`) — Deep Architecture

## 1. Cold-Path Split & Zero-Stack HTTPS Fast Path

```text
+-----------------------------------------------------------------------------------+
|  HttpsRedirectPlugin::on_request() [HOT FAST PATH]                                |
|    ├── Check x-forwarded-proto == "https" (const HeaderName, 0 GOT load)          |
|    ├── Check uri.scheme == "https"                                                |
|    └── Plaintext? ──► #[cold] #[inline(never)] build_redirect_response()          |
|                         ├── Extract Authority / Host                              |
|                         ├── strip_port() (byte scan, IPv6-bracket aware)          |
|                         ├── Reject control / CRLF bytes (<= 0x20, 0x7f)           |
|                         └── Format Location in 2048B stack buffer (heap fallback) |
+-----------------------------------------------------------------------------------+
```

---

## 2. Why `build_redirect_response()` Is Out-of-Line (`#[cold]`)

Inlining the `2048`-byte stack buffer (`[u8; 2048]`) and `HeaderMap` construction directly inside `on_request()` caused LLVM to emit a `2,312`-byte stack frame (`sub rsp, 2312`) on **every** request, including HTTPS traffic that immediately returns `Action::Continue`.

Isolating redirect construction into `#[cold] #[inline(never)] build_redirect_response()` combined with:
1. `const X_FORWARDED_PROTO: HeaderName` (eliminating `@GOTPCREL` loads), and
2. Pure byte scanning `bytes.iter().position(...)` in `strip_port()` (eliminating `core::str::pattern::CharSearcher` calls)

reduced `on_request()` from **614 ASM lines (`sub rsp, 2312`)** down to **142 ASM lines (`sub rsp, 32`)** — a **98.6% stack reduction** on the HTTPS fast path.

---

## 3. Security & Sanitization Invariants

- **Header Spoofing Immunity**: `velda-edge` overwrites `x-forwarded-proto` authoritatively from `meta.is_tls` before `pre_route` executes. Sending `X-Forwarded-Proto: https` on a plaintext port cannot bypass `https_redirect`.
- **CRLF / Header Injection Prevention**: `has_unsafe_header_bytes()` rejects any host or path containing ASCII control characters (`<= 0x20` or `0x7f`) with `RejectReason::BadRequest` (`400`).
- **IPv6 Bracket Preservation**: `strip_port("[2001:db8::1]:8080")` strips `:8080` while preserving `[2001:db8::1]`, and rejects malformed trailing characters like `[::1]abc`.
