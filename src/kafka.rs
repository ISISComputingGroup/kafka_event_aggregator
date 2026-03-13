//! Kafka utilities.

use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};

pub fn make_consumer(
    bootstrap_servers: &str,
    input_topic_name: &str,
    auto_commit_interval_ms: u64,
) -> StreamConsumer<DefaultConsumerContext> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set(
            "group.id",
            format!("kafka-event-aggregator-{input_topic_name}"),
        )
        .set("enable.auto.commit", "true")
        .set(
            "auto.commit.interval.ms",
            format!("{}", auto_commit_interval_ms),
        )
        .create()
        .unwrap_or_else(|e| panic!("Kafka consumer creation failed due to {}", e));

    consumer.subscribe(&[input_topic_name]).unwrap_or_else(|e| {
        panic!(
            "Kafka consumer can't subscribe to specified topic ({}) due to: {}",
            input_topic_name, e
        )
    });

    consumer
}

pub fn make_producer(bootstrap_servers: &str) -> ThreadedProducer<DefaultProducerContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("enable.idempotence", "true")
        .create()
        .unwrap_or_else(|e| panic!("Kafka producer creation failed due to: {}", e))
}
