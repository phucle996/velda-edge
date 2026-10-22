//! High-Performance HTTP/3 Benchmark Engine (RFC 9114 / RFC 9000).
//!
//! Generates concurrent HTTP/3 traffic using pooled QUIC client connections,
//! computes sub-millisecond percentiles (P50..P99.9), throughput (RPS), and goodput.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use http::header::HeaderValue;
use serde::Serialize;
use velda_core::{Body, L7Request};
use velda_http3::Http3Config;
use velda_http3::client::connect;

#[derive(Debug, Clone, Serialize)]
struct BenchmarkResult {
    rps: f64,
    bw_mbs: f64,
    mean_ms: f64,
    sd_ms: f64,
    min_ms: f64,
    max_ms: f64,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    ttfb_ms: f64,
    connect_ms: f64,
    total_req: usize,
    succeeded: usize,
    status_2xx: usize,
    status_4xx: usize,
    status_5xx: usize,
    errors: usize,
    success_rate: f64,
    duration_s: f64,
}

struct ParsedUrl {
    addr: SocketAddr,
    host: String,
    path: String,
}

fn parse_url(url_str: &str) -> Result<ParsedUrl, Box<dyn std::error::Error>> {
    let uri: http::Uri = url_str.parse()?;
    let host = uri.host().ok_or("URL missing host component")?.to_string();
    let port = uri.port_u16().unwrap_or(443);
    let path = uri
        .path_and_query()
        .map_or("/", |pq| pq.as_str())
        .to_string();

    let addr = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        SocketAddr::new(ip, port)
    } else {
        use std::net::ToSocketAddrs;
        format!("{}:{}", host, port)
            .to_socket_addrs()?
            .next()
            .ok_or("Failed to resolve host address")?
    };

    Ok(ParsedUrl { addr, host, path })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let mut urls: Vec<String> = Vec::new();
    let mut total_requests: usize = 50_000;
    let mut requests_explicit = false;
    let mut duration_secs: Option<u64> = None;
    let mut concurrency: usize = 100;
    let mut timeout_secs: u64 = 10;
    let mut json_output = false;
    let mut host_override: Option<String> = None;
    let mut custom_headers: Vec<(http::header::HeaderName, HeaderValue)> = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-u" | "--url" => {
                if i + 1 < args.len() {
                    urls.push(args[i + 1].clone());
                    i += 1;
                }
            }
            "-i" | "--input-file" => {
                if i + 1 < args.len() {
                    let file_content = std::fs::read_to_string(&args[i + 1])?;
                    for line in file_content.lines() {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() && !trimmed.starts_with('#') {
                            urls.push(trimmed.to_string());
                        }
                    }
                    i += 1;
                }
            }
            "-D" | "--duration" => {
                if i + 1 < args.len() {
                    let raw = args[i + 1].trim_end_matches('s').trim_end_matches('S');
                    duration_secs = Some(raw.parse()?);
                    i += 1;
                }
            }
            "-n" | "--requests" => {
                if i + 1 < args.len() {
                    total_requests = args[i + 1].parse()?;
                    requests_explicit = true;
                    i += 1;
                }
            }
            "-c" | "--concurrency" => {
                if i + 1 < args.len() {
                    concurrency = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "-t" | "--timeout" => {
                if i + 1 < args.len() {
                    timeout_secs = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--host" => {
                if i + 1 < args.len() {
                    host_override = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "-H" | "--header" => {
                if i + 1 < args.len() {
                    let header_str = &args[i + 1];
                    if let Some((name, val)) = header_str.split_once(':')
                        && let (Ok(hname), Ok(hval)) = (
                            name.trim().parse::<http::header::HeaderName>(),
                            HeaderValue::from_str(val.trim()),
                        )
                    {
                        custom_headers.push((hname, hval));
                    }
                    i += 1;
                }
            }
            "--json" => {
                json_output = true;
            }
            arg if arg.starts_with("http://") || arg.starts_with("https://") => {
                urls.push(arg.to_string());
            }
            _ => {}
        }
        i += 1;
    }

    if duration_secs.is_some() && !requests_explicit {
        total_requests = usize::MAX;
    }

    if urls.is_empty() {
        urls.push("https://192.168.122.14:8444/plain/ping".to_string());
    }

    let parsed_targets: Vec<ParsedUrl> = urls
        .iter()
        .map(|u| parse_url(u))
        .collect::<Result<Vec<_>, _>>()?;

    let primary_target = &parsed_targets[0];
    let effective_host = host_override
        .clone()
        .unwrap_or_else(|| primary_target.host.clone());

    if !json_output {
        eprintln!(
            "Starting h3bench: requests={}, concurrency={}, target={}:{}",
            total_requests, concurrency, primary_target.addr, primary_target.path
        );
    }

    let config = Http3Config::auto();
    let num_conns = concurrency;

    let conn_start = Instant::now();
    let mut clients = Vec::with_capacity(num_conns);
    for _ in 0..num_conns {
        let client = connect(primary_target.addr, &effective_host, &config).await?;
        clients.push(client);
    }
    let connect_time_ms = conn_start.elapsed().as_secs_f64() * 1000.0 / num_conns as f64;

    let clients = Arc::new(clients);
    let targets = Arc::new(parsed_targets);
    let headers_to_send = Arc::new(custom_headers);
    let remaining_counter = Arc::new(AtomicUsize::new(0));

    let bench_start = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let clients = clients.clone();
        let targets = targets.clone();
        let headers_to_send = headers_to_send.clone();
        let counter = remaining_counter.clone();
        let eff_host = effective_host.clone();

        let task = tokio::spawn(async move {
            let cap = if total_requests == usize::MAX {
                50_000
            } else {
                total_requests / concurrency + 64
            };
            let mut latencies: Vec<f64> = Vec::with_capacity(cap);
            let mut bytes_transferred: usize = 0;
            let mut status_2xx: usize = 0;
            let mut status_4xx: usize = 0;
            let mut status_5xx: usize = 0;
            let mut errors: usize = 0;

            let mut client_idx = worker_id % clients.len();
            let duration_limit = duration_secs.map(std::time::Duration::from_secs);

            loop {
                if let Some(dur) = duration_limit
                    && bench_start.elapsed() >= dur
                {
                    break;
                }
                let req_idx = counter.fetch_add(1, Ordering::Relaxed);
                if req_idx >= total_requests {
                    break;
                }

                let target = &targets[req_idx % targets.len()];

                let mut req = L7Request {
                    method: http::Method::GET,
                    uri: match target.path.parse() {
                        Ok(u) => u,
                        Err(_) => http::Uri::from_static("/"),
                    },
                    version: http::Version::HTTP_3,
                    headers: http::HeaderMap::new(),
                    body: Body::Empty,
                };

                if let Ok(hv) = HeaderValue::from_str(&eff_host) {
                    req.headers.insert("host", hv);
                }
                for (hname, hval) in headers_to_send.iter() {
                    req.headers.insert(hname.clone(), hval.clone());
                }

                let mut retries = 0;
                while retries < 3 {
                    let client = &clients[client_idx];
                    let t0 = Instant::now();
                    let send_fut = client.send_request(req.clone());
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_secs),
                        send_fut,
                    )
                    .await
                    {
                        Ok(Ok(resp)) => {
                            let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                            latencies.push(elapsed_ms);

                            if total_requests <= 10 {
                                eprintln!("Response Status: {}", resp.status);
                                for (k, v) in &resp.headers {
                                    eprintln!("Response Header: {}: {:?}", k, v);
                                }
                                if let Body::Bytes(ref b) = resp.body {
                                    eprintln!("Response Body: {}", String::from_utf8_lossy(b));
                                }
                            }
                            if resp.status.is_success() {
                                status_2xx += 1;
                            } else if resp.status.is_client_error() {
                                status_4xx += 1;
                            } else if resp.status.is_server_error() {
                                status_5xx += 1;
                            }

                            if let Body::Bytes(b) = resp.body {
                                bytes_transferred += b.len();
                            }
                            break;
                        }
                        Ok(Err(e)) => {
                            if errors < 10 {
                                eprintln!(
                                    "Worker {worker_id} (conn {client_idx}) failed on req #{req_idx} after {status_2xx} successes: {e:?}",
                                );
                            }
                            if client.is_closed() {
                                client_idx = (client_idx + 1) % clients.len();
                            }
                            retries += 1;
                            if retries == 3 {
                                errors += 1;
                            }
                        }
                        Err(_) => {
                            if errors < 10 {
                                eprintln!(
                                    "Worker {worker_id} (conn {client_idx}) request timed out after {timeout_secs}s on req #{req_idx}",
                                );
                            }
                            client_idx = (client_idx + 1) % clients.len();
                            retries += 1;
                            if retries == 3 {
                                errors += 1;
                            }
                        }
                    }
                }
            }

            (
                latencies,
                bytes_transferred,
                status_2xx,
                status_4xx,
                status_5xx,
                errors,
            )
        });

        tasks.push(task);
    }

    let cap_all = if total_requests == usize::MAX {
        250_000
    } else {
        total_requests
    };
    let mut all_latencies: Vec<f64> = Vec::with_capacity(cap_all);
    let mut total_bytes: usize = 0;
    let mut total_2xx: usize = 0;
    let mut total_4xx: usize = 0;
    let mut total_5xx: usize = 0;
    let mut total_errors: usize = 0;

    for task in tasks {
        let (lats, bytes, s2, s4, s5, errs) = task.await?;
        all_latencies.extend(lats);
        total_bytes += bytes;
        total_2xx += s2;
        total_4xx += s4;
        total_5xx += s5;
        total_errors += errs;
    }

    // Gracefully close all QUIC connections (RFC 9114 Section 8.1)
    for client in clients.iter() {
        let _ = client.close().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let elapsed = bench_start.elapsed();
    let succeeded = all_latencies.len();
    let total_executed = succeeded + total_errors;
    let rps = if elapsed.as_secs_f64() > 0.0 {
        succeeded as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let bw_mbs = if elapsed.as_secs_f64() > 0.0 {
        (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    } else {
        0.0
    };

    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let min_ms = all_latencies.first().copied().unwrap_or(0.0);
    let max_ms = all_latencies.last().copied().unwrap_or(0.0);
    let sum_ms: f64 = all_latencies.iter().sum();
    let mean_ms = if succeeded > 0 {
        sum_ms / succeeded as f64
    } else {
        0.0
    };

    let variance = if succeeded > 1 {
        all_latencies
            .iter()
            .map(|&l| {
                let diff = l - mean_ms;
                diff * diff
            })
            .sum::<f64>()
            / (succeeded - 1) as f64
    } else {
        0.0
    };
    let sd_ms = variance.sqrt();

    let p50_ms = if succeeded > 0 {
        all_latencies[(succeeded as f64 * 0.50) as usize]
    } else {
        0.0
    };
    let p90_ms = if succeeded > 0 {
        all_latencies[((succeeded as f64 * 0.90) as usize).min(succeeded - 1)]
    } else {
        0.0
    };
    let p99_ms = if succeeded > 0 {
        all_latencies[((succeeded as f64 * 0.99) as usize).min(succeeded - 1)]
    } else {
        0.0
    };
    let p999_ms = if succeeded > 0 {
        all_latencies[((succeeded as f64 * 0.999) as usize).min(succeeded - 1)]
    } else {
        0.0
    };

    let success_rate = if total_executed > 0 {
        round2(100.0 * succeeded as f64 / total_executed as f64)
    } else {
        100.0
    };

    let result = BenchmarkResult {
        rps: round2(rps),
        bw_mbs: round2(bw_mbs),
        mean_ms: round2(mean_ms),
        sd_ms: round2(sd_ms),
        min_ms: round3(min_ms),
        max_ms: round2(max_ms),
        p50_ms: round2(p50_ms),
        p90_ms: round2(p90_ms),
        p99_ms: round2(p99_ms),
        p999_ms: round2(p999_ms),
        ttfb_ms: round2(mean_ms),
        connect_ms: round2(connect_time_ms),
        total_req: total_executed,
        succeeded,
        status_2xx: total_2xx,
        status_4xx: total_4xx,
        status_5xx: total_5xx,
        errors: total_errors,
        success_rate,
        duration_s: round2(elapsed.as_secs_f64()),
    };

    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("============================================================");
        println!("  HTTP/3 Load Benchmark Results");
        println!("============================================================");
        println!("Requests:       {succeeded} / {total_requests} (Success: {success_rate}%)");
        println!("Throughput:     {:.2} req/s", result.rps);
        println!("Bandwidth:      {:.2} MB/s", result.bw_mbs);
        println!(
            "Latency (mean): {:.2} ms (SD: {:.2} ms)",
            result.mean_ms, result.sd_ms
        );
        println!("Latency (min):  {:.3} ms", result.min_ms);
        println!("Latency (p50):  {:.2} ms", result.p50_ms);
        println!("Latency (p90):  {:.2} ms", result.p90_ms);
        println!("Latency (p99):  {:.2} ms", result.p99_ms);
        println!("Latency (p999): {:.2} ms", result.p999_ms);
        println!("Latency (max):  {:.2} ms", result.max_ms);
        println!("Connect Time:   {:.2} ms", result.connect_ms);
        println!("HTTP 2xx:       {}", result.status_2xx);
        println!("============================================================");
    }

    Ok(())
}

fn round2(val: f64) -> f64 {
    (val * 100.0).round() / 100.0
}

fn round3(val: f64) -> f64 {
    (val * 1000.0).round() / 1000.0
}
