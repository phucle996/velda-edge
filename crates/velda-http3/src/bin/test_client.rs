use std::net::SocketAddr;
use std::time::Instant;
use velda_core::{Body, L7Request};
use velda_http3::client::connector::{connect, default_client_config};
use velda_http3::config::Http3Config;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target: SocketAddr = "192.168.122.14:8445".parse()?;
    println!("Connecting to HTTP/3 server at {target}...");

    let config = Http3Config::auto();
    let start = Instant::now();
    let (t_ip, t_port_be) = match target {
        SocketAddr::V4(v4) => (
            velda_core::lut::ip::v4_to_u128(v4.ip().to_bits()),
            v4.port().to_be(),
        ),
        SocketAddr::V6(v6) => (v6.ip().to_bits(), v6.port().to_be()),
    };
    let client = connect(
        t_ip,
        t_port_be,
        "backend.local",
        &config,
        default_client_config()?,
    )
    .await?;
    println!("QUIC handshake completed in {:.2?}", start.elapsed());

    let mut req = L7Request::new(
        http::Method::GET,
        http::Uri::from_static("/ping"),
        http::Version::HTTP_3,
        velda_core::HeaderWorkspace::default(),
        Body::Empty,
    );
    req.headers
        .insert("host", http::HeaderValue::from_static("backend.local"));

    let resp = client.send_request(req).await?;
    println!("Status: {}", resp.status);
    if let Body::Bytes(b) = resp.body {
        println!("Body: {:?}", String::from_utf8_lossy(&b));
    }

    Ok(())
}
