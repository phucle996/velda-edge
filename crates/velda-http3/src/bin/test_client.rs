use std::net::SocketAddr;
use std::time::Instant;
use velda_core::{Body, L7Request};
use velda_http3::Http3Config;
use velda_http3::client::connect;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target: SocketAddr = "192.168.122.14:8445".parse()?;
    println!("Connecting to HTTP/3 server at {target}...");

    let config = Http3Config::auto();
    let start = Instant::now();
    let client = connect(
        target,
        "backend.local",
        &config,
        velda_http3::client::default_client_config()?,
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
