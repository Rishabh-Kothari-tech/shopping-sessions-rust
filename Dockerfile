FROM rust:slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app \
    && mkdir -p /output/user_daily /output/campaign_daily /scratch \
    && chown -R 1000:1000 /output /scratch

COPY --from=builder /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
COPY run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/shopping-sessions /usr/local/bin/run.sh

ENV DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    HOME=/scratch

USER 1000:1000
ENTRYPOINT ["/usr/local/bin/run.sh"]
