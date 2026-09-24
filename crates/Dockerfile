FROM rust:1.98-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo build --release -p velda-edge

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/velda-edge /app/velda-edge

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/app/velda-edge"]
