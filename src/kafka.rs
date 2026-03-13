//! Kafka utilities.

use crate::config::AggregatorConfig;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use anyhow::Result;

pub fn make_consumer(config: &AggregatorConfig) -> Result<StreamConsumer<DefaultConsumerContext>> {
    let mut client_config = ClientConfig::new();

    for (k, v) in &config.kafka_consumer {
        client_config.set(k, v);
    }

    let consumer: StreamConsumer<DefaultConsumerContext> = client_config
        .create()?;

    consumer
        .subscribe(&[&config.input_topic])
        .unwrap_or_else(|e| {
            panic!(
                "Kafka consumer can't subscribe to specified topic ({}) due to: {}",
                config.input_topic, e
            )
        });

    Ok(consumer)
}

pub fn make_producer(config: &AggregatorConfig) -> Result<ThreadedProducer<DefaultProducerContext>> {
    let mut client_config = ClientConfig::new();
    for (k, v) in &config.kafka_producer {
        client_config.set(k, v);
    }
    Ok(client_config.create()?)
}
