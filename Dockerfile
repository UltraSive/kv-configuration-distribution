# -------- Stage 1: Build --------
FROM rust:1-bookworm AS builder
WORKDIR /app

# Cache dependencies separately from sources.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && echo "fn main() {}" > src/main.rs && cp src/main.rs src/bin/kvctl.rs && touch src/lib.rs \
    && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs src/lib.rs src/bin/kvctl.rs && cargo build --release

# -------- Stage 2: Runtime --------
FROM gcr.io/distroless/cc-debian12
WORKDIR /app
COPY --from=builder /app/target/release/kvdist /app/target/release/kvctl /app/

VOLUME ["/data"]
ENV KV_DATA_DIR=/data \
    KV_HTTP_ADDR=0.0.0.0:8080 \
    KV_PEER_ADDR=0.0.0.0:7070

EXPOSE 8080 7070
ENTRYPOINT ["/app/kvdist"]
