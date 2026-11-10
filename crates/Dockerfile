FROM rust:1.98-slim-bookworm AS builder

ARG TARGET_CPU=x86-64-v3
ENV RUSTFLAGS="-C target-cpu=${TARGET_CPU}"

WORKDIR /app
COPY .cargo/ ./.cargo/
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo build --release -p velda-edge && \
    strip target/release/velda-edge

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/velda-edge /app/velda-edge

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/app/velda-edge"]
