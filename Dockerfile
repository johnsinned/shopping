FROM rust:1.85-slim-bookworm AS builder
ARG TARGET_CPU=cascadelake
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN RUSTFLAGS="-C target-cpu=${TARGET_CPU}" cargo build --release --locked --offline

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /build/target/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
CMD ["/app/shopping-sessions"]
