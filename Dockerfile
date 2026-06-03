FROM rust:latest AS builder

# rust:latest is debian based
RUN apt-get update && apt-get install -y cmake && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/kafka_event_aggregator
COPY . .

RUN cargo install --path .


FROM debian:stable-slim
RUN apt-get update && apt-get install -y libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/kafka_event_aggregator /usr/local/bin/kafka_event_aggregator
ENTRYPOINT ["kafka_event_aggregator"]
