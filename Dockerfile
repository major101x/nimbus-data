FROM rust:trixie AS builder

WORKDIR /app
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./producer ./producer
# Consumer to be copied here also once built
# COPY ./consumer ./consumer

RUN apt-get update && apt-get install -y cmake build-essential pkg-config

RUN cargo build --release

FROM debian:trixie-slim
COPY --from=builder /app/target/release/producer /app/producer
# Copy consumer also
# COPY --from=builder /app/target/release/consumer /app/consumer

CMD [ "/app/producer" ]