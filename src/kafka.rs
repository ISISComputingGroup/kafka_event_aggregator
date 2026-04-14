//! Kafka utilities.

use crate::config::AggregatorConfig;
use anyhow::{Context, Result, bail};
use isis_streaming_data_types::{DeserializedMessage, deserialize_message};
use log::{debug, warn};
use rdkafka::Offset::Offset;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use std::time::Duration;

pub fn make_consumer(config: &AggregatorConfig) -> Result<StreamConsumer<DefaultConsumerContext>> {
    let mut client_config = ClientConfig::new();

    for (k, v) in config.kafka_consumer_settings() {
        client_config.set(k, v);
    }

    let consumer: StreamConsumer<DefaultConsumerContext> = client_config.create()?;

    consumer
        .subscribe(&[config.input_topic()])
        .with_context(|| format!("Failed to subscribe to topic {}", config.input_topic()))?;

    Ok(consumer)
}

fn latest_message_on_one_partition(
    config: &AggregatorConfig,
    consumer: &BaseConsumer<DefaultConsumerContext>,
    partition: i32,
) -> Result<i64> {
    let (low, high) = consumer.fetch_watermarks(
        config.output_topic(),
        partition,
        Duration::from_millis(config.read_last_message_timeout_ms()),
    )?;

    if high <= low {
        bail!(
            "Partition {} on topic {} has no messages; skipping",
            partition,
            config.output_topic()
        );
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(config.output_topic(), partition, Offset(high - 1))?;
    consumer.assign(&tpl)?;

    let mut attempts = 0;

    // Note: must call poll repeatedly with short timeouts, rather than calling poll
    // once with a long timeout here, to avoid race condition in kafka with polling
    // immediately after assignment.
    while attempts < config.read_last_message_timeout_ms() {
        if let Some(msg) = consumer.poll(Duration::from_millis(1)) {
            return match msg {
                Ok(message) => {
                    if let Some(payload) = message.payload() {
                        match deserialize_message(payload) {
                            Ok(DeserializedMessage::EventDataEv44(msg)) => Ok(msg.message_id()),
                            Ok(DeserializedMessage::PulseMetadataPu00(msg)) => Ok(msg.message_id()),
                            _ => {
                                bail!(
                                    "Cannot deserialize latest message on partition {}",
                                    partition
                                );
                            }
                        }
                    } else {
                        bail!("Latest message has no payload on partition {}", partition);
                    }
                }
                Err(err) => {
                    bail!(
                        "Cannot read message from kafka: {} on partition {}",
                        err,
                        partition
                    );
                }
            };
        }
        attempts += 1;
    }

    bail!("Could not read any messages from partition {}", partition)
}

pub fn get_most_recent_message_id(config: &AggregatorConfig) -> Result<i64> {
    let mut client_config = ClientConfig::new();

    for (k, v) in config.kafka_consumer_settings() {
        client_config.set(k, v);
    }

    let consumer: BaseConsumer<DefaultConsumerContext> = client_config.create()?;

    let metadata = consumer.fetch_metadata(Some(config.output_topic()), Duration::from_secs(10))?;

    metadata
        .topics()
        .iter()
        .find(|t| t.name() == config.output_topic())
        .with_context(|| "Cannot get topic")?
        .partitions()
        .iter()
        .filter_map(
            |p| match latest_message_on_one_partition(config, &consumer, p.id()) {
                Err(err) => {
                    warn!("{}", err);
                    None
                }
                Ok(val) => {
                    debug!("Latest id for partition {} is {}", p.id(), val);
                    Some(val)
                }
            },
        )
        .max()
        .with_context(|| format!("No usable messages on topic {}", config.output_topic()))
}

pub fn make_producer(
    config: &AggregatorConfig,
) -> Result<ThreadedProducer<DefaultProducerContext>> {
    let mut client_config = ClientConfig::new();

    // Check that enable.idempotence has been set - if not, fail loudly as
    // this is not a valid configuration for `kafka_event_aggregator` and the stream format
    // will be incorrect.
    if config.kafka_producer_settings().get("enable.idempotence") != Some(&"true".to_owned()) {
        bail!("Kafka producer is not set to be idempotent.
It is not possible to produce a coherent stream with a non-idempotent producer as messages may be duplicated, missing, or out of order (within a single partition).
See documentation of this program for details of the _events stream format.
")
    }

    for (k, v) in config.kafka_producer_settings() {
        client_config.set(k, v);
    }

    Ok(client_config.create()?)
}
