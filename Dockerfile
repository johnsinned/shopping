FROM rust:1.85-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
RUN RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /build/target/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
CMD ["/app/shopping-sessions"]
