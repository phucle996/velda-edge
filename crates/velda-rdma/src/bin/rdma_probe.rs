//! # Hardware RDMA Verification & Real-Payload Benchmark Suite
//!
//! High-performance CLI benchmark and probe tool for Reliable Connected (RC) Queue Pairs
//! over real InfiniBand or RoCEv2 Host Channel Adapters (HCAs) across cluster nodes.
//!
//! Evaluates and compares **RDMA (Zero-Copy Slab Arena / RoCEv2)** vs **Non-RDMA (Standard Kernel TCP)**
//! under identical, realistic payload sizes:
//! - **Phase 1**: Standard 2KB Payload (2,048 B) Request-Response Latency Benchmark (P50, P90, P99)
//! - **Phase 2**: Large 64KB Bulk Payload (65,536 B) Discrete Frame DMA Throughput Benchmark
//! - **Phase 3**: Pipelined Full-Duplex Sustained Stream Throughput Benchmark (52.4 MB bidirectional = 104.8 MB total)
//!
//! ### Usage:
//! ```bash
//! # RDMA Mode:
//! rdma_probe server <bind_addr> [device_name] [gid_index] [iters]
//! rdma_probe client <target_addr> [device_name] [gid_index] [iters]
//!
//! # Non-RDMA Mode (Standard Kernel TCP):
//! rdma_probe tcp-server <bind_addr> [iters]
//! rdma_probe tcp-client <target_addr> [iters]
//! ```

use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use velda_rdma::{RdmaConfig, RdmaConnector, RdmaListener};

const STANDARD_2K: usize = 2048;
const BULK_64K: usize = 65536;
const STREAM_CHUNK_SIZE: usize = 65536;
const STREAM_CHUNKS: usize = 800; // 800 * 64KB = 52,428,800 bytes (~52.4 MB)

#[inline(always)]
fn compute_checksum(data: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in data {
        hash = (hash ^ (b as u64)).wrapping_mul(0x100000001b3);
    }
    hash
}

fn print_latency_stats(
    name: &str,
    mut samples: Vec<u64>,
    bytes_per_msg: usize,
    total_bytes: usize,
    elapsed: Duration,
) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let min = samples[0];
    let p50 = samples[samples.len() * 50 / 100];
    let p90 = samples[samples.len() * 90 / 100];
    let p99 = samples[samples.len() * 99 / 100];
    let max = samples[samples.len() - 1];
    let avg = samples.iter().sum::<u64>() / samples.len() as u64;

    let sec = elapsed.as_secs_f64();
    let mb = (total_bytes as f64) / (1024.0 * 1024.0);
    let mb_sec = if sec > 0.0 { mb / sec } else { 0.0 };
    let gbps = (mb_sec * 8.0) / 1000.0;
    let ops = if sec > 0.0 {
        (samples.len() as f64) / sec
    } else {
        0.0
    };

    println!(
        ">>> [{name}] Completed {} iterations ({} bytes/msg, total {:.2} MB in {:.3}s)",
        samples.len(),
        bytes_per_msg,
        mb,
        sec
    );
    println!(
        ">>>   Throughput:  {:.2} MB/s ({:.3} Gbps) | {:.1} ops/sec",
        mb_sec, gbps, ops
    );
    println!(
        ">>>   Latency:     Min: {} µs | P50: {} µs | P90: {} µs | P99: {} µs | Max: {} µs | Avg: {} µs",
        min, p50, p90, p99, max, avg
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage:");
        eprintln!("  rdma_probe <server|client> <addr> [device_name] [gid_index] [iters]");
        eprintln!("  rdma_probe <tcp-server|tcp-client> <addr> [iters]");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  rdma_probe server 0.0.0.0:18515 rxe0 1 1000");
        eprintln!("  rdma_probe client 172.16.0.4:18515 rxe0 1 1000");
        eprintln!("  rdma_probe tcp-server 0.0.0.0:18516 1000");
        eprintln!("  rdma_probe tcp-client 172.16.0.4:18516 1000");
        std::process::exit(1);
    }

    let mode = &args[1];
    let addr: SocketAddr = args[2].parse()?;

    match mode.as_str() {
        "server" => {
            let device_name = args.get(3).filter(|s| !s.is_empty()).cloned();
            let gid_index: u8 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
            let iters_p1: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1000);
            let iters_p2: usize = (iters_p1 / 5).clamp(100, 1000);

            let config = RdmaConfig {
                device_name,
                gid_index,
                port: 1,
                max_send_wr: 64,
                max_recv_wr: 64,
                cq_size: 128,
                initial_credits: 32,
                timeout_ms: 5000,
                force_mock: false,
                ..Default::default()
            };

            println!(
                ">>> [RDMA Server] Initializing on {addr} (device={:?}, gid={gid_index})",
                config.device_name
            );

            let listener = RdmaListener::bind(addr, config.clone()).await?;
            println!(
                ">>> RDMA Listener bound on {}. Awaiting client connection...",
                listener.local_addr()?
            );
            let (channel, peer) = listener.accept().await?;
            println!(">>> [Channel 1] Established physical RDMA channel with peer: {peer}");

            let (sender, receiver) = channel.split();

            println!(
                ">>> [Server Phase 1] Serving {iters_p1} iterations of Real Standard 2KB Payloads..."
            );
            let mut resp_buf = vec![0u8; STANDARD_2K];
            for i in 0..iters_p1 {
                let rx = receiver.recv(Duration::from_secs(10)).await?;
                if rx.len() != STANDARD_2K {
                    panic!(
                        "Phase 1 size mismatch: expected {STANDARD_2K}, got {}",
                        rx.len()
                    );
                }
                let seq = u64::from_le_bytes(rx[0..8].try_into().unwrap());
                if seq != i as u64 {
                    panic!("Phase 1 sequence mismatch: expected {i}, got {seq}");
                }
                let expected_csum = u64::from_le_bytes(rx[8..16].try_into().unwrap());
                let actual_csum = compute_checksum(&rx[16..]);
                if expected_csum != actual_csum {
                    panic!(
                        "Phase 1 checksum error on request {i}: {expected_csum:#x} vs {actual_csum:#x}"
                    );
                }

                resp_buf[0..8].copy_from_slice(&(seq + 1).to_le_bytes());
                for (idx, b) in resp_buf[16..].iter_mut().enumerate() {
                    *b = (((i + 1) ^ idx) & 0xFF) as u8;
                }
                let resp_csum = compute_checksum(&resp_buf[16..]);
                resp_buf[8..16].copy_from_slice(&resp_csum.to_le_bytes());

                sender.send(&resp_buf).await?;
            }
            println!(
                ">>> [Server Phase 1] Completed {iters_p1} Standard 2KB requests with 100% checksum match."
            );

            println!(
                ">>> [Server Phase 2] Serving {iters_p2} frames of Real Bulk 64KB Payloads..."
            );
            let ack_payload = b"ACK_BULK_64K_FRAME";
            for i in 0..iters_p2 {
                let rx = receiver.recv(Duration::from_secs(10)).await?;
                if rx.len() != BULK_64K {
                    panic!(
                        "Phase 2 size mismatch: expected {BULK_64K}, got {}",
                        rx.len()
                    );
                }
                let seq = u64::from_le_bytes(rx[0..8].try_into().unwrap());
                if seq != i as u64 {
                    panic!("Phase 2 sequence mismatch: expected {i}, got {seq}");
                }
                let expected_csum = u64::from_le_bytes(rx[8..16].try_into().unwrap());
                let actual_csum = compute_checksum(&rx[16..]);
                if expected_csum != actual_csum {
                    panic!(
                        "Phase 2 checksum error on frame {i}: {expected_csum:#x} vs {actual_csum:#x}"
                    );
                }

                sender.send(ack_payload).await?;
            }
            println!(
                ">>> [Server Phase 2] Verified {iters_p2} Bulk 64KB frames with 100% data integrity."
            );

            drop(sender);
            drop(receiver);

            println!(
                ">>> [Server Phase 3] Awaiting Channel 2 for full-duplex sustained stream test..."
            );
            let (channel2, peer2) = listener.accept().await?;
            println!(">>> [Channel 2] Established physical RDMA channel with peer: {peer2}");

            let (mut stream_reader, mut stream_writer) = channel2.into_stream().into_split();

            let server_writer = tokio::spawn(async move {
                let mut chunk = vec![0u8; STREAM_CHUNK_SIZE];
                for i in 0..STREAM_CHUNKS {
                    chunk[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                    for (idx, b) in chunk[16..].iter_mut().enumerate() {
                        *b = ((i ^ idx ^ 0x55) & 0xFF) as u8;
                    }
                    let csum = compute_checksum(&chunk[16..]);
                    chunk[8..16].copy_from_slice(&csum.to_le_bytes());

                    stream_writer.write_all(&chunk).await?;
                }
                stream_writer.flush().await?;
                stream_writer.shutdown().await?;
                Ok::<(), std::io::Error>(())
            });

            let server_reader = tokio::spawn(async move {
                let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
                let mut total_bytes = 0usize;
                for i in 0..STREAM_CHUNKS {
                    stream_reader.read_exact(&mut buf).await?;
                    total_bytes += buf.len();
                    let seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    if seq != i as u64 {
                        panic!("Phase 3 stream sequence mismatch: expected {i}, got {seq}");
                    }
                    let expected_csum = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                    let actual_csum = compute_checksum(&buf[16..]);
                    if expected_csum != actual_csum {
                        panic!("Phase 3 stream checksum error on chunk {i}");
                    }
                }
                Ok::<usize, std::io::Error>(total_bytes)
            });

            let (res_w, res_r) = tokio::join!(server_writer, server_reader);
            res_w??;
            let read_bytes = res_r??;
            println!(
                ">>> [Server Phase 3] Full-duplex stream complete: verified {read_bytes} bytes."
            );
        }
        "client" => {
            let device_name = args.get(3).filter(|s| !s.is_empty()).cloned();
            let gid_index: u8 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
            let iters_p1: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1000);
            let iters_p2: usize = (iters_p1 / 5).clamp(100, 1000);

            let config = RdmaConfig {
                device_name,
                gid_index,
                port: 1,
                max_send_wr: 64,
                max_recv_wr: 64,
                cq_size: 128,
                initial_credits: 32,
                timeout_ms: 5000,
                force_mock: false,
                ..Default::default()
            };

            println!(
                ">>> [RDMA Client] Connecting to {addr} (device={:?}, gid={gid_index})",
                config.device_name
            );

            let start_conn = Instant::now();
            let channel = RdmaConnector::connect(addr, &config).await?;
            println!(
                ">>> [Channel 1] Connected in {:.2} ms",
                start_conn.elapsed().as_secs_f64() * 1000.0
            );

            let (sender, receiver) = channel.split();

            println!(
                ">>> [Client Phase 1] Benchmarking {iters_p1} iterations of Real Standard 2KB Payloads..."
            );
            let mut req_buf = vec![0u8; STANDARD_2K];
            let mut samples_p1 = Vec::with_capacity(iters_p1);
            let start_p1 = Instant::now();

            for i in 0..iters_p1 {
                req_buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                for (idx, b) in req_buf[16..].iter_mut().enumerate() {
                    *b = ((i ^ idx) & 0xFF) as u8;
                }
                let csum = compute_checksum(&req_buf[16..]);
                req_buf[8..16].copy_from_slice(&csum.to_le_bytes());

                let t0 = Instant::now();
                sender.send(&req_buf).await?;
                let rx = receiver.recv(Duration::from_secs(10)).await?;
                let lat_us = t0.elapsed().as_micros() as u64;
                samples_p1.push(lat_us);

                if rx.len() != STANDARD_2K {
                    panic!(
                        "Phase 1 response size mismatch: expected {STANDARD_2K}, got {}",
                        rx.len()
                    );
                }
                let resp_seq = u64::from_le_bytes(rx[0..8].try_into().unwrap());
                if resp_seq != (i as u64) + 1 {
                    panic!("Phase 1 response sequence mismatch");
                }
                let expected_csum = u64::from_le_bytes(rx[8..16].try_into().unwrap());
                let actual_csum = compute_checksum(&rx[16..]);
                if expected_csum != actual_csum {
                    panic!("Phase 1 response checksum error on iter {i}");
                }
            }

            let elapsed_p1 = start_p1.elapsed();
            print_latency_stats(
                "RDMA Phase 1: Standard 2KB Payload",
                samples_p1,
                STANDARD_2K,
                iters_p1 * STANDARD_2K * 2,
                elapsed_p1,
            );

            println!(
                ">>> [Client Phase 2] Benchmarking {iters_p2} frames of Real Bulk 64KB Payloads..."
            );
            let mut bulk_buf = vec![0u8; BULK_64K];
            let mut samples_p2 = Vec::with_capacity(iters_p2);
            let start_p2 = Instant::now();

            for i in 0..iters_p2 {
                bulk_buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                for (idx, b) in bulk_buf[16..].iter_mut().enumerate() {
                    *b = ((i ^ idx ^ 0xAA) & 0xFF) as u8;
                }
                let csum = compute_checksum(&bulk_buf[16..]);
                bulk_buf[8..16].copy_from_slice(&csum.to_le_bytes());

                let t0 = Instant::now();
                sender.send(&bulk_buf).await?;
                let ack = receiver.recv(Duration::from_secs(10)).await?;
                let lat_us = t0.elapsed().as_micros() as u64;
                samples_p2.push(lat_us);

                if &ack[..] != b"ACK_BULK_64K_FRAME" {
                    panic!("Phase 2 ACK mismatch on frame {i}: {:?}", ack);
                }
            }

            let elapsed_p2 = start_p2.elapsed();
            print_latency_stats(
                "RDMA Phase 2: Bulk 64KB Payload",
                samples_p2,
                BULK_64K,
                iters_p2 * BULK_64K,
                elapsed_p2,
            );

            drop(sender);
            drop(receiver);

            println!(
                ">>> [Client Phase 3] Connecting Channel 2 for full-duplex sustained stream test..."
            );
            let start_conn2 = Instant::now();
            let channel2 = RdmaConnector::connect(addr, &config).await?;
            println!(
                ">>> [Channel 2] Connected in {:.2} ms",
                start_conn2.elapsed().as_secs_f64() * 1000.0
            );

            let (mut stream_reader, mut stream_writer) = channel2.into_stream().into_split();
            let start_p3 = Instant::now();

            let client_writer = tokio::spawn(async move {
                let mut chunk = vec![0u8; STREAM_CHUNK_SIZE];
                for i in 0..STREAM_CHUNKS {
                    chunk[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                    for (idx, b) in chunk[16..].iter_mut().enumerate() {
                        *b = ((i ^ idx) & 0xFF) as u8;
                    }
                    let csum = compute_checksum(&chunk[16..]);
                    chunk[8..16].copy_from_slice(&csum.to_le_bytes());

                    stream_writer.write_all(&chunk).await?;
                }
                stream_writer.flush().await?;
                stream_writer.shutdown().await?;
                Ok::<(), std::io::Error>(())
            });

            let client_reader = tokio::spawn(async move {
                let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
                let mut total_bytes = 0usize;
                for i in 0..STREAM_CHUNKS {
                    stream_reader.read_exact(&mut buf).await?;
                    total_bytes += buf.len();
                    let seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    if seq != i as u64 {
                        panic!("Phase 3 stream sequence mismatch: expected {i}, got {seq}");
                    }
                    let expected_csum = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                    let actual_csum = compute_checksum(&buf[16..]);
                    if expected_csum != actual_csum {
                        panic!("Phase 3 stream checksum error on chunk {i}");
                    }
                }
                Ok::<usize, std::io::Error>(total_bytes)
            });

            let (res_w, res_r) = tokio::join!(client_writer, client_reader);
            res_w??;
            let read_bytes = res_r??;
            let elapsed_p3 = start_p3.elapsed();

            let total_stream_bytes = read_bytes * 2;
            let sec_p3 = elapsed_p3.as_secs_f64();
            let mb_p3 = (total_stream_bytes as f64) / (1024.0 * 1024.0);
            let mb_sec_p3 = if sec_p3 > 0.0 { mb_p3 / sec_p3 } else { 0.0 };
            let gbps_p3 = (mb_sec_p3 * 8.0) / 1000.0;

            println!(
                ">>> [RDMA Phase 3: Full-Duplex Sustained Stream] Dispatched & verified {:.2} MB bidirectional in {:.3}s",
                mb_p3, sec_p3
            );
            println!(
                ">>>   Aggregate Streaming Bandwidth: {:.2} MB/s ({:.3} Gbps)",
                mb_sec_p3, gbps_p3
            );
            println!(
                ">>>   Data Integrity: 100% Checksum match across all chunks (zero corruption)."
            );
        }
        "tcp-server" => {
            let iters_p1: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1000);
            let iters_p2: usize = (iters_p1 / 5).clamp(100, 1000);

            println!(">>> [Non-RDMA / Kernel TCP Server] Binding on {addr}...");
            let listener = TcpListener::bind(addr).await?;
            println!(
                ">>> TCP Listener bound on {}. Awaiting client connection...",
                listener.local_addr()?
            );

            let (mut stream1, peer1) = listener.accept().await?;
            stream1.set_nodelay(true)?;
            println!(">>> [TCP Channel 1] Connected with peer: {peer1}");

            println!(">>> [TCP Server Phase 1] Serving {iters_p1} iterations of 2KB Payloads...");
            let mut req_buf = vec![0u8; STANDARD_2K];
            let mut resp_buf = vec![0u8; STANDARD_2K];
            for i in 0..iters_p1 {
                stream1.read_exact(&mut req_buf).await?;
                let seq = u64::from_le_bytes(req_buf[0..8].try_into().unwrap());
                if seq != i as u64 {
                    panic!("TCP Phase 1 sequence mismatch: expected {i}, got {seq}");
                }
                let expected_csum = u64::from_le_bytes(req_buf[8..16].try_into().unwrap());
                let actual_csum = compute_checksum(&req_buf[16..]);
                if expected_csum != actual_csum {
                    panic!("TCP Phase 1 checksum error on request {i}");
                }

                resp_buf[0..8].copy_from_slice(&(seq + 1).to_le_bytes());
                for (idx, b) in resp_buf[16..].iter_mut().enumerate() {
                    *b = (((i + 1) ^ idx) & 0xFF) as u8;
                }
                let resp_csum = compute_checksum(&resp_buf[16..]);
                resp_buf[8..16].copy_from_slice(&resp_csum.to_le_bytes());

                stream1.write_all(&resp_buf).await?;
                stream1.flush().await?;
            }
            println!(">>> [TCP Server Phase 1] Completed {iters_p1} requests.");

            println!(">>> [TCP Server Phase 2] Serving {iters_p2} frames of Bulk 64KB Payloads...");
            let mut bulk_buf = vec![0u8; BULK_64K];
            let ack_payload = b"ACK_BULK_64K_FRAME";
            for i in 0..iters_p2 {
                stream1.read_exact(&mut bulk_buf).await?;
                let seq = u64::from_le_bytes(bulk_buf[0..8].try_into().unwrap());
                if seq != i as u64 {
                    panic!("TCP Phase 2 sequence mismatch: expected {i}, got {seq}");
                }
                let expected_csum = u64::from_le_bytes(bulk_buf[8..16].try_into().unwrap());
                let actual_csum = compute_checksum(&bulk_buf[16..]);
                if expected_csum != actual_csum {
                    panic!("TCP Phase 2 checksum error on frame {i}");
                }

                stream1.write_all(ack_payload).await?;
                stream1.flush().await?;
            }
            println!(">>> [TCP Server Phase 2] Verified {iters_p2} Bulk 64KB frames.");

            drop(stream1);

            println!(">>> [TCP Server Phase 3] Awaiting Channel 2 for sustained stream test...");
            let (stream2, peer2) = listener.accept().await?;
            stream2.set_nodelay(true)?;
            println!(">>> [TCP Channel 2] Connected with peer: {peer2}");

            let (mut stream_reader, mut stream_writer) = stream2.into_split();

            let server_writer = tokio::spawn(async move {
                let mut chunk = vec![0u8; STREAM_CHUNK_SIZE];
                for i in 0..STREAM_CHUNKS {
                    chunk[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                    for (idx, b) in chunk[16..].iter_mut().enumerate() {
                        *b = ((i ^ idx ^ 0x55) & 0xFF) as u8;
                    }
                    let csum = compute_checksum(&chunk[16..]);
                    chunk[8..16].copy_from_slice(&csum.to_le_bytes());

                    stream_writer.write_all(&chunk).await?;
                }
                stream_writer.flush().await?;
                stream_writer.shutdown().await?;
                Ok::<(), std::io::Error>(())
            });

            let server_reader = tokio::spawn(async move {
                let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
                let mut total_bytes = 0usize;
                for i in 0..STREAM_CHUNKS {
                    stream_reader.read_exact(&mut buf).await?;
                    total_bytes += buf.len();
                    let seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    if seq != i as u64 {
                        panic!("Phase 3 stream sequence mismatch");
                    }
                    let expected_csum = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                    let actual_csum = compute_checksum(&buf[16..]);
                    if expected_csum != actual_csum {
                        panic!("Phase 3 stream checksum error");
                    }
                }
                Ok::<usize, std::io::Error>(total_bytes)
            });

            let (res_w, res_r) = tokio::join!(server_writer, server_reader);
            res_w??;
            let read_bytes = res_r??;
            println!(
                ">>> [TCP Server Phase 3] Full-duplex stream complete: verified {read_bytes} bytes."
            );
        }
        "tcp-client" => {
            let iters_p1: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1000);
            let iters_p2: usize = (iters_p1 / 5).clamp(100, 1000);

            println!(">>> [Non-RDMA / Kernel TCP Client] Connecting to {addr}...");
            let start_conn = Instant::now();
            let mut stream1 = TcpStream::connect(addr).await?;
            stream1.set_nodelay(true)?;
            println!(
                ">>> [TCP Channel 1] Connected in {:.2} ms",
                start_conn.elapsed().as_secs_f64() * 1000.0
            );

            println!(
                ">>> [TCP Client Phase 1] Benchmarking {iters_p1} iterations of 2KB Payloads..."
            );
            let mut req_buf = vec![0u8; STANDARD_2K];
            let mut rx_buf = vec![0u8; STANDARD_2K];
            let mut samples_p1 = Vec::with_capacity(iters_p1);
            let start_p1 = Instant::now();

            for i in 0..iters_p1 {
                req_buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                for (idx, b) in req_buf[16..].iter_mut().enumerate() {
                    *b = ((i ^ idx) & 0xFF) as u8;
                }
                let csum = compute_checksum(&req_buf[16..]);
                req_buf[8..16].copy_from_slice(&csum.to_le_bytes());

                let t0 = Instant::now();
                stream1.write_all(&req_buf).await?;
                stream1.flush().await?;
                stream1.read_exact(&mut rx_buf).await?;
                let lat_us = t0.elapsed().as_micros() as u64;
                samples_p1.push(lat_us);

                let resp_seq = u64::from_le_bytes(rx_buf[0..8].try_into().unwrap());
                if resp_seq != (i as u64) + 1 {
                    panic!("TCP Phase 1 sequence mismatch");
                }
                let expected_csum = u64::from_le_bytes(rx_buf[8..16].try_into().unwrap());
                let actual_csum = compute_checksum(&rx_buf[16..]);
                if expected_csum != actual_csum {
                    panic!("TCP Phase 1 response checksum error on iter {i}");
                }
            }

            let elapsed_p1 = start_p1.elapsed();
            print_latency_stats(
                "Non-RDMA (TCP) Phase 1: Standard 2KB Payload",
                samples_p1,
                STANDARD_2K,
                iters_p1 * STANDARD_2K * 2,
                elapsed_p1,
            );

            println!(
                ">>> [TCP Client Phase 2] Benchmarking {iters_p2} frames of Bulk 64KB Payloads..."
            );
            let mut bulk_buf = vec![0u8; BULK_64K];
            let mut ack_buf = [0u8; 18]; // b"ACK_BULK_64K_FRAME".len() == 18
            let mut samples_p2 = Vec::with_capacity(iters_p2);
            let start_p2 = Instant::now();

            for i in 0..iters_p2 {
                bulk_buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                for (idx, b) in bulk_buf[16..].iter_mut().enumerate() {
                    *b = ((i ^ idx ^ 0xAA) & 0xFF) as u8;
                }
                let csum = compute_checksum(&bulk_buf[16..]);
                bulk_buf[8..16].copy_from_slice(&csum.to_le_bytes());

                let t0 = Instant::now();
                stream1.write_all(&bulk_buf).await?;
                stream1.flush().await?;
                stream1.read_exact(&mut ack_buf).await?;
                let lat_us = t0.elapsed().as_micros() as u64;
                samples_p2.push(lat_us);

                if &ack_buf[..] != b"ACK_BULK_64K_FRAME" {
                    panic!("TCP Phase 2 ACK mismatch on frame {i}: {:?}", ack_buf);
                }
            }

            let elapsed_p2 = start_p2.elapsed();
            print_latency_stats(
                "Non-RDMA (TCP) Phase 2: Bulk 64KB Payload",
                samples_p2,
                BULK_64K,
                iters_p2 * BULK_64K,
                elapsed_p2,
            );

            drop(stream1);

            println!(
                ">>> [TCP Client Phase 3] Connecting Channel 2 for full-duplex stream test..."
            );
            let start_conn2 = Instant::now();
            let stream2 = TcpStream::connect(addr).await?;
            stream2.set_nodelay(true)?;
            println!(
                ">>> [TCP Channel 2] Connected in {:.2} ms",
                start_conn2.elapsed().as_secs_f64() * 1000.0
            );

            let (mut stream_reader, mut stream_writer) = stream2.into_split();
            let start_p3 = Instant::now();

            let client_writer = tokio::spawn(async move {
                let mut chunk = vec![0u8; STREAM_CHUNK_SIZE];
                for i in 0..STREAM_CHUNKS {
                    chunk[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                    for (idx, b) in chunk[16..].iter_mut().enumerate() {
                        *b = ((i ^ idx) & 0xFF) as u8;
                    }
                    let csum = compute_checksum(&chunk[16..]);
                    chunk[8..16].copy_from_slice(&csum.to_le_bytes());

                    stream_writer.write_all(&chunk).await?;
                }
                stream_writer.flush().await?;
                stream_writer.shutdown().await?;
                Ok::<(), std::io::Error>(())
            });

            let client_reader = tokio::spawn(async move {
                let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
                let mut total_bytes = 0usize;
                for i in 0..STREAM_CHUNKS {
                    stream_reader.read_exact(&mut buf).await?;
                    total_bytes += buf.len();
                    let seq = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    if seq != i as u64 {
                        panic!("Phase 3 stream sequence mismatch");
                    }
                    let expected_csum = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                    let actual_csum = compute_checksum(&buf[16..]);
                    if expected_csum != actual_csum {
                        panic!("Phase 3 stream checksum error on chunk {i}");
                    }
                }
                Ok::<usize, std::io::Error>(total_bytes)
            });

            let (res_w, res_r) = tokio::join!(client_writer, client_reader);
            res_w??;
            let read_bytes = res_r??;
            let elapsed_p3 = start_p3.elapsed();

            let total_stream_bytes = read_bytes * 2;
            let sec_p3 = elapsed_p3.as_secs_f64();
            let mb_p3 = (total_stream_bytes as f64) / (1024.0 * 1024.0);
            let mb_sec_p3 = if sec_p3 > 0.0 { mb_p3 / sec_p3 } else { 0.0 };
            let gbps_p3 = (mb_sec_p3 * 8.0) / 1000.0;

            println!(
                ">>> [Non-RDMA (TCP) Phase 3: Sustained Stream] Dispatched & verified {:.2} MB bidirectional in {:.3}s",
                mb_p3, sec_p3
            );
            println!(
                ">>>   Aggregate Streaming Bandwidth: {:.2} MB/s ({:.3} Gbps)",
                mb_sec_p3, gbps_p3
            );
            println!(
                ">>>   Data Integrity: 100% Checksum match across all chunks (zero corruption)."
            );
        }
        other => {
            eprintln!(
                "Unknown mode: {other}. Use 'server', 'client', 'tcp-server', or 'tcp-client'."
            );
            std::process::exit(1);
        }
    }

    println!(">>> Exiting cleanly.");
    Ok(())
}
