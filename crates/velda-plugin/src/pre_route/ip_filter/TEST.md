# IP / CIDR Subnet Access Filter Plugin (`ip_filter`) — Test Specification

This document describes the test methodology, automated unit test suites, CVE-class adversarial security validation, and boundary verification for the **`ip_filter`** pre-route plugin.

---

## 1. Test Architecture & Scope

The `ip_filter` test suite validates four core functional and security areas:
1. **Bitwise Arithmetic, Canonicalization & Parsing Correctness (`network.rs`)**:
   - IPv4 and IPv6 CIDR subnet matching (`10.0.0.0/8`, `192.168.1.0/24`, `2001:db8::/32`).
   - Universal IPv6-to-IPv4 canonicalization across RFC 4291 Mapped (`::ffff:a.b.c.d`), RFC 2765 SIIT (`::ffff:0:a.b.c.d`), RFC 6052 Well-Known NAT64 (`64:ff9b::a.b.c.d`), and RFC 4291 Compatible (`::a.b.c.d`).
   - Fast-path byte-level decimal parsing with strict RFC 9110 OWS trimming, single-quote-pair stripping, and validated port stripping (`1..=65535`).
   - Subnet compaction, deduplication, and prefix-length sorting.
2. **Pre-Route Policy Enforcement (`plugin.rs`)**:
   - `AllowList` mode (whitelisting) with multi-hop deny exception overrides.
   - `DenyList` mode (blacklisting) with per-hop allow exception overrides.
   - Zero-allocation `403 Forbidden` response generation with `Content-Type` and `Cache-Control` headers.
3. **Sophisticated CVE & Adversarial Evasion Scenarios (`plugin.rs`)**:
   - Multi-hop `X-Forwarded-For` spoofing in `AllowList` mode (`10.1.2.3, 203.0.113.5`).
   - Allow-exception smuggling across hops in `DenyList` mode (`203.0.113.10, 203.0.113.5`).
   - Cross-header fallback poisoning (`X-Forwarded-For: malformed` + `X-Real-IP: 10.0.0.1`).
   - Duplicate `X-Real-IP` header smuggling and comma-list injection in `X-Real-IP`.
   - CVE-2021-28918 octal / leading-zero IPv4 parser differentials (`010.0.0.1`, `0177.0.0.1`).
   - Port userinfo smuggling (`10.0.0.1:80@203.0.113.5`) and out-of-range ports (`:0`, `:65536`, `:080`).
   - NAT64, SIIT, IPv4-mapped, and IPv4-compatible dotted & hex tunneling bypass attempts.
   - XFF hop-flood DoS (`> 16` hops) and empty/trailing comma token injection.
4. **Declarative Configuration, Subsumption & Shadowing Validation (`velda-sync`)**:
   - Schema validation, mode normalization, and source validation.
   - Automatic canonicalization (bare IPs $\to$ `/32`, host bit masking, NAT64/SIIT prefix normalization).
   - Contradictory rule detection and cross-list subnet shadowing detection (`allow_cidr` shadowed by broader `deny_cidr` in `allow` mode, or `deny_cidr` shadowed by broader `allow_cidr` in `deny` mode).
   - Intra-list subnet subsumption compaction (`10.0.0.0/8` + `10.1.2.0/24` $\to$ `10.0.0.0/8`).

---

## 2. Automated Test Catalog

| Suite / File | Test Function | Purpose / Assertion |
| :--- | :--- | :--- |
| `network.rs` | `test_ipv4_cidr_matching` | Verifies bitwise `(ip & mask) == net` for IPv4 subnets. |
| `network.rs` | `test_ipv6_cidr_matching` | Verifies bitwise matching for IPv6 subnets. |
| `network.rs` | `test_ipv4_mapped_ipv6_matching` | Verifies that `::ffff:192.168.1.50` matches `192.168.1.0/24` and `::ffff:192.168.1.0/120` compiles to IPv4 `/24`. |
| `network.rs` | `test_parse_ip_bytes` | Tests fast-path parsing of plain IPv4, IPv4:port, quoted IPv4, bracketed IPv6:port, and mapped IPv6. |
| `network.rs` | `test_compact_and_sort_v4` | Verifies that subsumed child subnets (`/16`, `/32` inside `/8`) are pruned and broadest subnets sort first. |
| `plugin.rs` | `test_allow_list_mode` | Verifies permitted IPs return `Action::Continue` and non-permitted IPs return `Action::Respond(403)`. |
| `plugin.rs` | `test_allow_list_mode_with_port_and_quotes` | Verifies port and quote stripping during allowlist evaluation. |
| `plugin.rs` | `test_deny_list_mode` | Verifies blocked IPs return `Action::Respond(403)` and clean IPs return `Action::Continue`. |
| `plugin.rs` | `test_dual_stack_mapped_ipv6_blocking` | Verifies dual-stack attacker connecting via `::ffff:203.0.113.5` is blocked by IPv4 `203.0.113.0/24` rule. |
| `plugin.rs` | `test_malformed_ip_rejection_anti_evasion` | Verifies malformed header strings are rejected with 403 Forbidden. |
| `plugin.rs` | `test_ip_filter_from_config` | Verifies deserialization and instantiation from JSON configuration. |
| `plugin.rs` | `test_cve_vector_1_multi_hop_xff_spoofing_in_allow_mode` | Blocks spoofed multi-hop chains (`10.1.2.3, 203.0.113.5` and `10.1.2.3, 10.99.1.5`) in `AllowList` mode. |
| `plugin.rs` | `test_cve_vector_2_allow_exception_smuggling_in_deny_mode` | Blocks allow-exception smuggling (`203.0.113.10, 203.0.113.5`) in `DenyList` mode. |
| `plugin.rs` | `test_cve_vector_3_cross_header_fallback_poisoning` | Verifies malformed `X-Forwarded-For` never falls back to spoofed `X-Real-IP`. |
| `plugin.rs` | `test_cve_vector_4_duplicate_header_and_x_real_ip_list_smuggling` | Rejects duplicate `X-Real-IP` headers and comma-separated lists inside `X-Real-IP`. |
| `plugin.rs` | `test_cve_vector_5_octal_ipv4_parser_differential_cve_2021_28918` | Rejects octal/leading-zero IPv4 notations (`010.0.0.1`, `0177.0.0.1`, `10.00.0.1`). |
| `plugin.rs` | `test_cve_vector_6_port_userinfo_and_out_of_range_port_smuggling` | Rejects `10.0.0.1:80@203.0.113.5`, `:0`, `:65536`, `:080`, and `[2001:db8::1]garbage`. |
| `plugin.rs` | `test_cve_vector_7_ipv6_transition_tunneling_evasion_nat64_siit_compat` | Blocks blacklisted IPv4 tunneled via RFC 4291 Mapped, RFC 2765 SIIT, RFC 6052 NAT64, and RFC 4291 Compatible (dotted & hex). |
| `plugin.rs` | `test_cve_vector_8_hop_flood_and_trailing_garbage_dos` | Rejects `> 16` hops in `X-Forwarded-For`, empty comma hops, and trailing garbage tokens. |
| `velda-sync` | `test_valid_allow_list` | Verifies normalization and bare IP expansion (`10.0.0.1` $\to$ `10.0.0.1/32`). |
| `velda-sync` | `test_valid_deny_list` | Verifies valid denylist configuration parsing. |
| `velda-sync` | `test_invalid_mode` | Rejects unsupported operational modes. |
| `velda-sync` | `test_invalid_cidr_format` | Rejects invalid CIDR prefix lengths (`/33`). |
| `velda-sync` | `test_empty_allowlist_rejected` | Rejects empty allowlist in allow mode to prevent outage. |
| `velda-sync` | `test_missing_required_fields_rejected` | Rejects configurations missing mandatory fields. |
| `velda-sync` | `test_contradictory_cidr_detection` | Detects and rejects identical rules in both allow and deny lists. |
| `velda-sync` | `test_cross_list_shadowing_detection` | Rejects rules completely shadowed by a broader opposite rule in `allow` or `deny` mode. |
| `velda-sync` | `test_intra_list_subsumption_and_ipv6_tunnel_canonicalization` | Verifies intra-list subnet compaction and NAT64/SIIT CIDR canonicalization. |

---

## 3. Running Test Suites

Execute all tests for the `ip_filter` plugin and sync validator:

```bash
# Run unit tests for velda-plugin ip_filter (19 tests)
cargo test -p velda-plugin --lib pre_route::ip_filter

# Run unit tests for velda-sync ip_filter validator (9 tests)
cargo test -p velda-sync --lib post_sync::plugin::pre_route::ip_filter

# Run full monorepo test suite (1063 tests)
cargo test
```

---

## 4. Live Chaotic Multi-Origin VPN & Adversarial Stress Suite

In addition to automated unit tests, a live multi-threaded stress suite simulates chaotic internet traffic from global commercial VPNs, Tor exit nodes, bulletproof datacenters, and adversarial evasion vectors across an Azure Virtual Network:

- **Volume & Concurrency**: 10,000 requests sampled across 11 categories dispatched via 16 concurrent HTTP keep-alive pipelines.
- **Coverage**:
  - `allowed_vpn_v4` & `allowed_vpn_v6` (Nord, Express, Mullvad, Proton, Surfshark, CyberGhost).
  - `allowed_vpn_ports_quotes` (Valid RFC 9110 ports, quotes, and OWS tabs).
  - `blocked_vpn_v4` & `blocked_vpn_v6` (Tor exit nodes, bulletproof hosting, banned VPN subnets).
  - `attack_octal` (CVE-2021-28918 leading-zero notation).
  - `attack_port_smuggling` (Userinfo `@`, ports `:0`, `:65536`, `:080`).
  - `attack_multihop_spoof` (Chained allow/deny hops, 18-hop floods, empty comma tokens).
  - `attack_ipv6_tunnel` (NAT64, SIIT, IPv4-mapped, IPv4-compatible dotted and hex).
  - `attack_corrupted_syntax` (Unclosed brackets, double quotes, non-OWS control characters `\x0c`, `\x0b`).
- **Empirical Precision on Azure**: **100.00% Pass** (10,000/10,000), **0 False Positives**, **0 False Negatives**, **2.44 ms P50 Latency**.

