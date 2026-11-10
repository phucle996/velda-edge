//! Finite User-Agent & OS Classification (`velda_observability::logs::user_agent`).
//!
//! # Mechanism & Rationale
//! User-Agent strings are arbitrary-length client inputs that would otherwise require heap
//! allocations or string pointers inside access log records. By classifying the raw header slice
//! (`&[u8]`) in a single $\mathcal{O}(N)$ pass into finite integer discriminants
//! `(UaFamily, UaOs, ua_major_version)` at request completion, Frontline Workers can immediately
//! reset `HeaderWorkspace` after emitting the 64-byte record without Use-After-Free hazards.

/// Finite User-Agent client family discriminant (32 variants + `Custom` fallback).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum UaFamily {
    None = 0,
    Chrome = 1,
    Firefox = 2,
    Safari = 3,
    Edge = 4,
    Opera = 5,
    Brave = 6,
    SamsungInternet = 7,
    MobileSafari = 8,
    ChromeMobile = 9,
    AndroidWebView = 10,
    Curl = 11,
    Wget = 12,
    Httpie = 13,
    Postman = 14,
    Insomnia = 15,
    GoHttpClient = 16,
    PythonRequests = 17,
    PythonHttpx = 18,
    PythonAiohttp = 19,
    NodeUndici = 20,
    Axios = 21,
    RustReqwest = 22,
    RustHyper = 23,
    JavaHttpClient = 24,
    OkHttp = 25,
    GrpcGo = 26,
    GrpcJava = 27,
    GrpcC = 28,
    Tonic = 29,
    KubeProbe = 30,
    Prometheus = 31,
    Googlebot = 32,
    Custom = 255,
}

impl UaFamily {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Chrome,
            2 => Self::Firefox,
            3 => Self::Safari,
            4 => Self::Edge,
            5 => Self::Opera,
            6 => Self::Brave,
            7 => Self::SamsungInternet,
            8 => Self::MobileSafari,
            9 => Self::ChromeMobile,
            10 => Self::AndroidWebView,
            11 => Self::Curl,
            12 => Self::Wget,
            13 => Self::Httpie,
            14 => Self::Postman,
            15 => Self::Insomnia,
            16 => Self::GoHttpClient,
            17 => Self::PythonRequests,
            18 => Self::PythonHttpx,
            19 => Self::PythonAiohttp,
            20 => Self::NodeUndici,
            21 => Self::Axios,
            22 => Self::RustReqwest,
            23 => Self::RustHyper,
            24 => Self::JavaHttpClient,
            25 => Self::OkHttp,
            26 => Self::GrpcGo,
            27 => Self::GrpcJava,
            28 => Self::GrpcC,
            29 => Self::Tonic,
            30 => Self::KubeProbe,
            31 => Self::Prometheus,
            32 => Self::Googlebot,
            _ => Self::Custom,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Chrome => "chrome",
            Self::Firefox => "firefox",
            Self::Safari => "safari",
            Self::Edge => "edge",
            Self::Opera => "opera",
            Self::Brave => "brave",
            Self::SamsungInternet => "samsung",
            Self::MobileSafari => "mobile-safari",
            Self::ChromeMobile => "chrome-mobile",
            Self::AndroidWebView => "android-webview",
            Self::Curl => "curl",
            Self::Wget => "wget",
            Self::Httpie => "httpie",
            Self::Postman => "postman",
            Self::Insomnia => "insomnia",
            Self::GoHttpClient => "go-http-client",
            Self::PythonRequests => "python-requests",
            Self::PythonHttpx => "python-httpx",
            Self::PythonAiohttp => "python-aiohttp",
            Self::NodeUndici => "node-undici",
            Self::Axios => "axios",
            Self::RustReqwest => "rust-reqwest",
            Self::RustHyper => "rust-hyper",
            Self::JavaHttpClient => "java-http",
            Self::OkHttp => "okhttp",
            Self::GrpcGo => "grpc-go",
            Self::GrpcJava => "grpc-java",
            Self::GrpcC => "grpc-c",
            Self::Tonic => "tonic",
            Self::KubeProbe => "kube-probe",
            Self::Prometheus => "prometheus",
            Self::Googlebot => "googlebot",
            Self::Custom => "custom",
        }
    }
}

/// Finite Operating System / Environment discriminant extracted from `User-Agent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum UaOs {
    Unknown = 0,
    Windows = 1,
    MacOs = 2,
    Linux = 3,
    Android = 4,
    Ios = 5,
    ChromeOs = 6,
    BotOrCli = 7,
}

impl UaOs {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Windows,
            2 => Self::MacOs,
            3 => Self::Linux,
            4 => Self::Android,
            5 => Self::Ios,
            6 => Self::ChromeOs,
            7 => Self::BotOrCli,
            _ => Self::Unknown,
        }
    }

    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Windows => "windows",
            Self::MacOs => "macos",
            Self::Linux => "linux",
            Self::Android => "android",
            Self::Ios => "ios",
            Self::ChromeOs => "chromeos",
            Self::BotOrCli => "bot-cli",
        }
    }
}

/// Single-pass $\mathcal{O}(N)$ classifier converting a raw `User-Agent` byte slice into
/// `(UaFamily, UaOs, ua_major_version)`.
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
