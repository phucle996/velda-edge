use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, UdpSocket};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum L4Protocol {
    Tcp,
    Udp,
}

#[inline]
pub(crate) fn ip_addresses_overlap(a: IpAddr, b: IpAddr) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (IpAddr::V4(v4_a), IpAddr::V4(v4_b)) => {
            v4_a.is_unspecified() || v4_b.is_unspecified() || v4_a == v4_b
        }
        (IpAddr::V6(v6_a), IpAddr::V6(v6_b)) => {
            v6_a.is_unspecified() || v6_b.is_unspecified() || v6_a == v6_b
        }
        (IpAddr::V4(v4), IpAddr::V6(v6)) => {
            v6.is_unspecified() || (v4.is_unspecified() && v6.to_ipv4_mapped().is_some())
        }
        (IpAddr::V6(v6), IpAddr::V4(v4)) => {
            v6.is_unspecified() || (v4.is_unspecified() && v6.to_ipv4_mapped().is_some())
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HostListeningSocket {
    pub proto: L4Protocol,
    pub ip: IpAddr,
    pub port: u16,
}

pub(crate) fn parse_proc_net_content(
    content: &str,
    proto: L4Protocol,
    is_ipv6: bool,
    out: &mut Vec<HostListeningSocket>,
) {
    for line in content.lines().skip(1) {
        let mut tokens = line.split_whitespace();
        // Token 0: sl
        let _ = tokens.next();
        // Token 1: local_address (HEX_IP:HEX_PORT)
        let Some(local_addr) = tokens.next() else {
            continue;
        };
        // Token 2: rem_address
        let _ = tokens.next();
        // Token 3: st
        let Some(state) = tokens.next() else { continue };

        let is_listening = match proto {
            L4Protocol::Tcp => state.eq_ignore_ascii_case("0A"),
            L4Protocol::Udp => state.eq_ignore_ascii_case("07"),
        };

        if !is_listening {
            continue;
        }

        let Some((hex_ip, hex_port)) = local_addr.split_once(':') else {
            continue;
        };
        let Ok(port) = u16::from_str_radix(hex_port, 16) else {
            continue;
        };

        let ip = if is_ipv6 {
            if hex_ip.len() != 32 {
                continue;
            }
            let mut octets = [0u8; 16];
            let mut valid = true;
            for i in 0..4 {
                let chunk = &hex_ip[i * 8..(i + 1) * 8];
                match u32::from_str_radix(chunk, 16) {
                    Ok(val) => {
                        let bytes = val.to_ne_bytes();
                        octets[i * 4..(i + 1) * 4].copy_from_slice(&bytes);
                    }
                    Err(_) => {
                        valid = false;
                        break;
                    }
                }
            }
            if !valid {
                continue;
            }
            IpAddr::V6(Ipv6Addr::from(octets))
        } else {
            if hex_ip.len() != 8 {
                continue;
            }
            match u32::from_str_radix(hex_ip, 16) {
                Ok(val) => IpAddr::V4(Ipv4Addr::from(val.to_ne_bytes())),
                Err(_) => continue,
            }
        };

        out.push(HostListeningSocket { proto, ip, port });
    }
}

pub(crate) fn collect_host_listening_sockets() -> Vec<HostListeningSocket> {
    let mut sockets = Vec::new();
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = fs::read_to_string("/proc/net/tcp") {
            parse_proc_net_content(&content, L4Protocol::Tcp, false, &mut sockets);
        }
        if let Ok(content) = fs::read_to_string("/proc/net/tcp6") {
            parse_proc_net_content(&content, L4Protocol::Tcp, true, &mut sockets);
        }
        if let Ok(content) = fs::read_to_string("/proc/net/udp") {
            parse_proc_net_content(&content, L4Protocol::Udp, false, &mut sockets);
        }
        if let Ok(content) = fs::read_to_string("/proc/net/udp6") {
            parse_proc_net_content(&content, L4Protocol::Udp, true, &mut sockets);
        }
    }
    sockets
}

pub(crate) fn probe_socket_conflict(addr: SocketAddr, proto: L4Protocol) -> bool {
    match proto {
        L4Protocol::Tcp => match TcpListener::bind(addr) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => true,
            Err(_) => false,
        },
        L4Protocol::Udp => match UdpSocket::bind(addr) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => true,
            Err(_) => false,
        },
    }
}
