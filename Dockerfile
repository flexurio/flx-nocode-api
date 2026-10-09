# syntax=docker/dockerfile:1
# ---- build stage -----------------------------------------------------------
FROM rust:1.97-slim-bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev cmake build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
# Binary name comes from [package].name in Cargo.toml (flx-nocode-api).
RUN cargo build --release --locked \
 && test -x target/release/flx-nocode-api

# ---- runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --create-home --home-dir /app app

WORKDIR /app
COPY --from=builder /app/target/release/flx-nocode-api /app/flx-nocode-api
RUN mkdir -p /app/static /app/config /app/seed && chown -R app:app /app
USER app

ENV PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/healthz" || exit 1

CMD ["/app/flx-nocode-api"]
