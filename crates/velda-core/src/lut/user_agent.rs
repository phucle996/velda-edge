//! Finite User-Agent & OS classification and lookup tables (`velda-core::lut::user_agent`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                   UA_FAMILY_NAMES & UA_OS_NAMES (672 B L1D RESIDENT)                    │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ UaFamily              │ 0..=33        │ 34 variants    │ 544 bytes     │ Direct Index   │
//! │ UaOs                  │ 0..=7         │  8 variants    │ 128 bytes     │ Direct Index   │
//! ├───────────────────────┴───────────────┴────────────────┼───────────────┴────────────────┤
//! │ Total User-Agent Metadata Footprint:                   │ 672 bytes                      │
//! └────────────────────────────────────────────────────────┴────────────────────────────────┘
//!
//!   User-Agent Header Bytes ──► classify_user_agent()
//!                                       │
//!                                       ▼ (Pack into 4-byte scalar)
//!                 ┌─────────────────────┼─────────────────────┐
//!                 ▼                     ▼                     ▼
//!          UaFamily (u8)             UaOs (u8)       ua_major_version (u16)
//!                 │                     │
//!                 ▼                     ▼
//!        UA_FAMILY_NAMES[u8]       UA_OS_NAMES[u8]
//!        ("chrome", "curl", ...)   ("linux", "macos", ...)
//! ```

use serde::{Deserialize, Serialize};

/// Finite User-Agent client family discriminant (34 discrete variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum UaFamily {
    /// 0: No User-Agent header present.
    None = 0,
    /// 1: Google Chrome browser (desktop).
    Chrome = 1,
    /// 2: Mozilla Firefox browser.
    Firefox = 2,
    /// 3: Apple Safari browser (desktop).
    Safari = 3,
    /// 4: Microsoft Edge browser.
    Edge = 4,
    /// 5: Opera browser.
    Opera = 5,
    /// 6: Brave browser.
    Brave = 6,
    /// 7: Samsung Internet mobile browser.
    SamsungInternet = 7,
    /// 8: Mobile Safari on iOS / iPadOS.
    MobileSafari = 8,
    /// 9: Chrome Mobile on Android.
    ChromeMobile = 9,
    /// 10: Android WebView in-app browser.
    AndroidWebView = 10,
    /// 11: cURL command-line tool.
    Curl = 11,
    /// 12: GNU Wget tool.
    Wget = 12,
    /// 13: HTTPie command-line tool.
    Httpie = 13,
    /// 14: Postman API client.
    Postman = 14,
    /// 15: Insomnia REST client.
    Insomnia = 15,
    /// 16: Standard Go `net/http` client.
    GoHttpClient = 16,
    /// 17: Python `requests` library.
    PythonRequests = 17,
    /// 18: Python `httpx` library.
    PythonHttpx = 18,
    /// 19: Python `aiohttp` library.
    PythonAiohttp = 19,
    /// 20: Node.js `undici` / fetch client.
    NodeUndici = 20,
    /// 21: Node.js / browser `axios` library.
    Axios = 21,
    /// 22: Rust `reqwest` crate.
    RustReqwest = 22,
    /// 23: Rust `hyper` crate.
    RustHyper = 23,
    /// 24: Java standard `java.net.http.HttpClient`.
    JavaHttpClient = 24,
    /// 25: Square OkHttp client (Android / Java).
    OkHttp = 25,
    /// 26: Official gRPC Go client.
    GrpcGo = 26,
    /// 27: Official gRPC Java client.
    GrpcJava = 27,
    /// 28: Official gRPC C core / Python client.
    GrpcC = 28,
    /// 29: Rust `tonic` gRPC client.
    Tonic = 29,
    /// 30: Kubernetes kubelet / ingress health probe.
    KubeProbe = 30,
    /// 31: Prometheus metrics scraper.
    Prometheus = 31,
    /// 32: Googlebot search engine crawler.
    Googlebot = 32,
    /// 33: Custom or unclassified User-Agent client.
    Custom = 33,
}

/// Static lookup table mapping each `UaFamily` discriminant (0..33) directly to its
/// lowercase kebab-case ASCII name.
///
/// Resident in CPU L1 Data Cache ($34 \times 16\text{B} = 544\text{B}$).
pub static UA_FAMILY_NAMES: [&str; 34] = [
    "none",
    "chrome",
    "firefox",
    "safari",
    "edge",
    "opera",
    "brave",
    "samsung",
    "mobile-safari",
    "chrome-mobile",
    "android-webview",
    "curl",
    "wget",
    "httpie",
    "postman",
    "insomnia",
    "go-http-client",
    "python-requests",
    "python-httpx",
    "python-aiohttp",
    "node-undici",
    "axios",
    "rust-reqwest",
    "rust-hyper",
    "java-http",
    "okhttp",
    "grpc-go",
    "grpc-java",
    "grpc-c",
    "tonic",
    "kube-probe",
    "prometheus",
    "googlebot",
    "custom",
];

impl UaFamily {
    /// Total number of distinct User-Agent family variants.
    pub const COUNT: usize = 34;

    /// Static table containing all `UaFamily` variants in contiguous discriminant order.
    pub const ALL: [Self; 34] = [
        Self::None,
        Self::Chrome,
        Self::Firefox,
        Self::Safari,
        Self::Edge,
        Self::Opera,
        Self::Brave,
        Self::SamsungInternet,
        Self::MobileSafari,
        Self::ChromeMobile,
        Self::AndroidWebView,
        Self::Curl,
        Self::Wget,
        Self::Httpie,
        Self::Postman,
        Self::Insomnia,
        Self::GoHttpClient,
        Self::PythonRequests,
        Self::PythonHttpx,
        Self::PythonAiohttp,
        Self::NodeUndici,
        Self::Axios,
        Self::RustReqwest,
        Self::RustHyper,
        Self::JavaHttpClient,
        Self::OkHttp,
        Self::GrpcGo,
        Self::GrpcJava,
        Self::GrpcC,
        Self::Tonic,
        Self::KubeProbe,
        Self::Prometheus,
        Self::Googlebot,
        Self::Custom,
    ];

    /// Converts a raw `u8` discriminant into a typed [`UaFamily`].
    ///
    /// Returns the matched variant if valid (0..=33), or [`UaFamily::None`] if out of bounds.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::None
        }
    }

    /// Converts a raw `u8` discriminant into `Some(UaFamily)` if valid (0..=33), or `None` if out of bounds.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Resolves the canonical kebab-case string via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        UA_FAMILY_NAMES[self as usize]
    }

    /// Resolves the canonical kebab-case ASCII byte slice via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        UA_FAMILY_NAMES[self as usize].as_bytes()
    }

    /// Returns `true` if this family represents an interactive human web browser.
    #[inline(always)]
    pub const fn is_browser(self) -> bool {
        matches!(
            self,
            Self::Chrome
                | Self::Firefox
                | Self::Safari
                | Self::Edge
                | Self::Opera
                | Self::Brave
                | Self::SamsungInternet
                | Self::MobileSafari
                | Self::ChromeMobile
                | Self::AndroidWebView
        )
    }

    /// Returns `true` if this family represents an automated CLI, bot, SDK, or health probe.
    #[inline(always)]
    pub const fn is_bot_or_cli(self) -> bool {
        !self.is_browser() && !matches!(self, Self::None | Self::Custom)
    }
}

const _: () = assert!(UaFamily::COUNT == UA_FAMILY_NAMES.len());
const _: () = assert!(UaFamily::COUNT == UaFamily::ALL.len());
const _: () = assert!(std::mem::size_of::<UaFamily>() == 1);

impl TryFrom<u8> for UaFamily {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(v).ok_or(v)
    }
}

impl Default for UaFamily {
    #[inline(always)]
    fn default() -> Self {
        Self::None
    }
}

/// Finite Operating System / Environment discriminant extracted from `User-Agent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum UaOs {
    /// 0: Unknown / Unspecified client OS.
    Unknown = 0,
    /// 1: Microsoft Windows.
    Windows = 1,
    /// 2: Apple macOS.
    MacOs = 2,
    /// 3: GNU/Linux.
    Linux = 3,
    /// 4: Google Android.
    Android = 4,
    /// 5: Apple iOS / iPadOS.
    Ios = 5,
    /// 6: Google ChromeOS.
    ChromeOs = 6,
    /// 7: Non-desktop automated environment (CLI, curl, gRPC, Bot, Probe).
    BotOrCli = 7,
}

/// Static lookup table mapping each `UaOs` discriminant (0..7) directly to its
/// lowercase kebab-case ASCII name.
///
/// Resident in CPU L1 Data Cache ($8 \times 16\text{B} = 128\text{B}$).
pub static UA_OS_NAMES: [&str; 8] = [
    "unknown", "windows", "macos", "linux", "android", "ios", "chromeos", "bot-cli",
];

impl UaOs {
    /// Total number of distinct OS variants.
    pub const COUNT: usize = 8;

    /// Static table containing all `UaOs` variants in contiguous discriminant order.
    pub const ALL: [Self; 8] = [
        Self::Unknown,
        Self::Windows,
        Self::MacOs,
        Self::Linux,
        Self::Android,
        Self::Ios,
        Self::ChromeOs,
        Self::BotOrCli,
    ];

    /// Converts a raw `u8` discriminant into a typed [`UaOs`].
    ///
    /// Returns the matched variant if valid (0..=7), or [`UaOs::Unknown`] if out of bounds.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::Unknown
        }
    }

    /// Converts a raw `u8` discriminant into `Some(UaOs)` if valid (0..=7), or `None` if out of bounds.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Resolves the canonical kebab-case string via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        UA_OS_NAMES[self as usize]
    }

    /// Resolves the canonical kebab-case ASCII byte slice via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        UA_OS_NAMES[self as usize].as_bytes()
    }

    /// Returns `true` if this environment represents a mobile platform (Android or iOS).
    #[inline(always)]
    pub const fn is_mobile(self) -> bool {
        matches!(self, Self::Android | Self::Ios)
    }

    /// Returns `true` if this environment represents a desktop workstation (Windows, Mac, Linux, ChromeOS).
    #[inline(always)]
    pub const fn is_desktop(self) -> bool {
        matches!(
            self,
            Self::Windows | Self::MacOs | Self::Linux | Self::ChromeOs
        )
    }
}

const _: () = assert!(UaOs::COUNT == UA_OS_NAMES.len());
const _: () = assert!(UaOs::COUNT == UaOs::ALL.len());
const _: () = assert!(std::mem::size_of::<UaOs>() == 1);

impl TryFrom<u8> for UaOs {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(v).ok_or(v)
    }
}

impl Default for UaOs {
    #[inline(always)]
    fn default() -> Self {
        Self::Unknown
    }
}

/// Single-pass $\mathcal{O}(N)$ branchless-capable classifier converting a raw `User-Agent` byte slice
/// into compact scalar tokens `(UaFamily, UaOs, ua_major_version)`.
///
/// Executes with zero dynamic heap allocations.
#[inline]
pub fn classify_user_agent(ua: &[u8]) -> (UaFamily, UaOs, u16) {
    if ua.is_empty() {
        return (UaFamily::None, UaOs::Unknown, 0);
    }

    #[inline(always)]
    fn parse_major_after_slash(bytes: &[u8]) -> u16 {
        let mut idx = 0;
        let len = bytes.len();
        while idx < len && bytes[idx] != b'/' {
            idx += 1;
        }
        idx += 1;
        let mut acc: u32 = 0;
        let mut digits = 0;
        while idx < len {
            let d = bytes[idx];
            if d.is_ascii_digit() {
                acc = acc.saturating_mul(10).saturating_add((d - b'0') as u32);
                digits += 1;
                if digits >= 5 {
                    break;
                }
                idx += 1;
            } else {
                break;
            }
        }
        if acc > u16::MAX as u32 {
            u16::MAX
        } else {
            acc as u16
        }
    }

    // Fast path for non-Mozilla CLI/SDK/gRPC tokens based on first byte
    match ua[0] {
        b'c' | b'C' if ua.len() >= 5 && ua[..5].eq_ignore_ascii_case(b"curl/") => {
            return (UaFamily::Curl, UaOs::BotOrCli, parse_major_after_slash(ua));
        }
        b'g' | b'G' => {
            if ua.len() >= 8 && ua[..8].eq_ignore_ascii_case(b"grpc-go/") {
                return (
                    UaFamily::GrpcGo,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
            if ua.len() >= 10 && ua[..10].eq_ignore_ascii_case(b"grpc-java/") {
                return (
                    UaFamily::GrpcJava,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
            if ua.len() >= 7 && ua[..7].eq_ignore_ascii_case(b"grpc-c/") {
                return (UaFamily::GrpcC, UaOs::BotOrCli, parse_major_after_slash(ua));
            }
            if ua.len() >= 15 && ua[..15].eq_ignore_ascii_case(b"Go-http-client/") {
                return (
                    UaFamily::GoHttpClient,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
        }
        b't' | b'T' if ua.len() >= 6 && ua[..6].eq_ignore_ascii_case(b"tonic/") => {
            return (UaFamily::Tonic, UaOs::BotOrCli, parse_major_after_slash(ua));
        }
        b'k' | b'K' if ua.len() >= 11 && ua[..11].eq_ignore_ascii_case(b"kube-probe/") => {
            return (
                UaFamily::KubeProbe,
                UaOs::BotOrCli,
                parse_major_after_slash(ua),
            );
        }
        b'P' | b'p' => {
            if ua.len() >= 16 && ua[..16].eq_ignore_ascii_case(b"python-requests/") {
                return (
                    UaFamily::PythonRequests,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
            if ua.len() >= 13 && ua[..13].eq_ignore_ascii_case(b"python-httpx/") {
                return (
                    UaFamily::PythonHttpx,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
            if ua.len() >= 15 && ua[..15].eq_ignore_ascii_case(b"PostmanRuntime/") {
                return (
                    UaFamily::Postman,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
            if ua.len() >= 11 && ua[..11].eq_ignore_ascii_case(b"Prometheus/") {
                return (
                    UaFamily::Prometheus,
                    UaOs::BotOrCli,
                    parse_major_after_slash(ua),
                );
            }
        }
        b'r' | b'R' if ua.len() >= 8 && ua[..8].eq_ignore_ascii_case(b"reqwest/") => {
            return (
                UaFamily::RustReqwest,
                UaOs::BotOrCli,
                parse_major_after_slash(ua),
            );
        }
        b'h' | b'H' if ua.len() >= 6 && ua[..6].eq_ignore_ascii_case(b"hyper/") => {
            return (
                UaFamily::RustHyper,
                UaOs::BotOrCli,
                parse_major_after_slash(ua),
            );
        }
        b'o' | b'O' if ua.len() >= 7 && ua[..7].eq_ignore_ascii_case(b"okhttp/") => {
            return (
                UaFamily::OkHttp,
                UaOs::BotOrCli,
                parse_major_after_slash(ua),
            );
        }
        b'u' | b'U' if ua.len() >= 6 && ua[..6].eq_ignore_ascii_case(b"undici") => {
            return (
                UaFamily::NodeUndici,
                UaOs::BotOrCli,
                parse_major_after_slash(ua),
            );
        }
        b'a' | b'A' if ua.len() >= 6 && ua[..6].eq_ignore_ascii_case(b"axios/") => {
            return (UaFamily::Axios, UaOs::BotOrCli, parse_major_after_slash(ua));
        }
        b'W' | b'w' if ua.len() >= 5 && ua[..5].eq_ignore_ascii_case(b"Wget/") => {
            return (UaFamily::Wget, UaOs::BotOrCli, parse_major_after_slash(ua));
        }
        _ => {}
    }

    // Single-pass scan for composite Mozilla/... browser User-Agents
    let mut os = UaOs::Unknown;
    let mut family = UaFamily::Custom;
    let mut version: u16 = 0;
    let len = ua.len();
    let mut i = 0;

    while i < len {
        let b = ua[i];
        if os == UaOs::Unknown {
            if b == b'W' && i + 7 <= len && &ua[i..i + 7] == b"Windows" {
                os = UaOs::Windows;
            } else if b == b'A' && i + 7 <= len && &ua[i..i + 7] == b"Android" {
                os = UaOs::Android;
            } else if b == b'i'
                && ((i + 6 <= len && &ua[i..i + 6] == b"iPhone")
                    || (i + 4 <= len && &ua[i..i + 4] == b"iPad"))
            {
                os = UaOs::Ios;
            } else if b == b'M' && i + 9 <= len && &ua[i..i + 9] == b"Macintosh" {
                os = UaOs::MacOs;
            } else if b == b'C' && i + 4 <= len && &ua[i..i + 4] == b"CrOS" {
                os = UaOs::ChromeOs;
            } else if b == b'L' && i + 5 <= len && &ua[i..i + 5] == b"Linux" {
                os = UaOs::Linux;
            }
        }

        // EdgiOS / EdgA / Edg/ override generic Chrome/Safari tokens
        if b == b'E' && i + 4 <= len && (&ua[i..i + 4] == b"Edg/" || &ua[i..i + 4] == b"EdgA") {
            family = UaFamily::Edge;
            version = parse_major_after_slash(&ua[i..]);
            break;
        }
        if b == b'O' && i + 4 <= len && &ua[i..i + 4] == b"OPR/" {
            family = UaFamily::Opera;
            version = parse_major_after_slash(&ua[i..]);
            break;
        }
        if b == b'F' && i + 8 <= len && &ua[i..i + 8] == b"Firefox/" {
            family = UaFamily::Firefox;
            version = parse_major_after_slash(&ua[i..]);
            break;
        }
        if b == b'G' && i + 10 <= len && &ua[i..i + 10] == b"Googlebot/" {
            family = UaFamily::Googlebot;
            os = UaOs::BotOrCli;
            version = parse_major_after_slash(&ua[i..]);
            break;
        }
        if b == b'C' && i + 7 <= len && &ua[i..i + 7] == b"Chrome/" {
            family = if os == UaOs::Android {
                UaFamily::ChromeMobile
            } else {
                UaFamily::Chrome
            };
            version = parse_major_after_slash(&ua[i..]);
        } else if b == b'V'
            && i + 8 <= len
            && &ua[i..i + 8] == b"Version/"
            && family == UaFamily::Custom
        {
            family = if os == UaOs::Ios {
                UaFamily::MobileSafari
            } else {
                UaFamily::Safari
            };
            version = parse_major_after_slash(&ua[i..]);
        }
        i += 1;
    }

    (family, os, version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ua_family_dense_roundtrip() {
        for i in 0..34u8 {
            let f = UaFamily::from_u8(i);
            assert_eq!(f as u8, i);
            assert_eq!(UaFamily::ALL[i as usize], f);
            assert_eq!(UA_FAMILY_NAMES[i as usize], f.as_str());
            assert_eq!(UaFamily::try_from_u8(i), Some(f));
        }
        assert_eq!(UaFamily::from_u8(34), UaFamily::None);
        assert_eq!(UaFamily::from_u8(255), UaFamily::None);
        assert!(UaFamily::try_from_u8(34).is_none());
        assert!(UaFamily::try_from_u8(255).is_none());
    }

    #[test]
    fn test_ua_os_dense_roundtrip() {
        for i in 0..8u8 {
            let o = UaOs::from_u8(i);
            assert_eq!(o as u8, i);
            assert_eq!(UaOs::ALL[i as usize], o);
            assert_eq!(UA_OS_NAMES[i as usize], o.as_str());
            assert_eq!(UaOs::try_from_u8(i), Some(o));
        }
        assert_eq!(UaOs::from_u8(8), UaOs::Unknown);
        assert_eq!(UaOs::from_u8(255), UaOs::Unknown);
        assert!(UaOs::try_from_u8(8).is_none());
        assert!(UaOs::try_from_u8(255).is_none());
    }

    #[test]
    fn test_classify() {
        let (fam, os, ver) = classify_user_agent(
            b"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
        );
        assert_eq!(fam, UaFamily::Chrome);
        assert_eq!(os, UaOs::Windows);
        assert_eq!(ver, 126);

        let (fam, os, ver) = classify_user_agent(b"curl/8.7.1");
        assert_eq!(fam, UaFamily::Curl);
        assert_eq!(os, UaOs::BotOrCli);
        assert_eq!(ver, 8);

        let (fam, os, ver) = classify_user_agent(b"grpc-go/1.64.0");
        assert_eq!(fam, UaFamily::GrpcGo);
        assert_eq!(os, UaOs::BotOrCli);
        assert_eq!(ver, 1);

        let (fam, os, ver) = classify_user_agent(b"MyProprietaryClient/4.2");
        assert_eq!(fam, UaFamily::Custom);
        assert_eq!(os, UaOs::Unknown);
        assert_eq!(ver, 0);
    }
}
