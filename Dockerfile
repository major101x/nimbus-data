FROM rust:trixie AS builder

WORKDIR /app
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./producer ./producer
COPY ./consumer ./consumer

RUN apt-get update && apt-get install -y cmake build-essential pkg-config

RUN cargo build --release

FROM debian:trixie-slim AS runtime

FROM runtime AS producer
COPY --from=builder /app/target/release/producer /app/producer
CMD [ "/app/producer" ]

FROM runtime AS consumer
COPY --from=builder /app/target/release/consumer /app/consumer
CMD [ "/app/consumer" ]

