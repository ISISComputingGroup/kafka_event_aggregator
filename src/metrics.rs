use crate::config::AggregatorConfig;
use metrics::{Unit, counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

pub const INCOMING_MESSAGES_PROCESSED: &str = "incoming_messages_processed";
pub const INCOMING_EVENT_MESSAGES_PROCESSED: &str = "incoming_event_messages_processed";
pub const INCOMING_NEUTRON_EVENTS: &str = "incoming_neutron_events";
pub const INCOMING_METADATA_MESSAGES_PROCESSED: &str = "incoming_metadata_messages_processed";
pub const INCOMING_INVALID_MESSAGES_DISCARDED: &str = "incoming_invalid_messages_discarded";
pub const INCOMING_MESSAGE_SIZE: &str = "incoming_message_size";

pub const OUTGOING_MESSAGE_SIZE: &str = "outgoing_message_size";
pub const OUTGOING_FRAMES: &str = "outgoing_frames";
pub const OUTGOING_METADATA_MESSAGES: &str = "outgoing_metadata_messages";
pub const OUTGOING_EVENT_MESSAGES: &str = "outgoing_event_messages";
pub const OUTGOING_NEUTRON_EVENTS: &str = "outgoing_neutron_events";
pub const OUTGOING_DROPPED_FRAMES_NO_METADATA: &str = "outgoing_dropped_frames_no_metadata";
pub const OUTGOING_DROPPED_NEUTRON_EVENTS_NO_METADATA: &str =
    "outgoing_dropped_neutron_events_no_metadata";
pub const OUTGOING_KAFKA_ERRORS: &str = "outgoing_kafka_errors";
pub const QUEUE_FRAMES: &str = "queue_frames";

pub fn initialize_metrics(config: &AggregatorConfig) -> anyhow::Result<()> {
    let builder = PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener((config.metrics_bind_addr).parse::<SocketAddr>()?);

    builder.install()?;

    describe_counter!(
        INCOMING_MESSAGES_PROCESSED,
        Unit::Count,
        "total incoming Kafka messages processed"
    );
    counter!(INCOMING_MESSAGES_PROCESSED).absolute(0);

    describe_counter!(
        INCOMING_EVENT_MESSAGES_PROCESSED,
        Unit::Count,
        "total incoming event messages processed"
    );
    counter!(INCOMING_EVENT_MESSAGES_PROCESSED).absolute(0);

    describe_counter!(
        INCOMING_NEUTRON_EVENTS,
        Unit::Count,
        "total incoming event messages processed"
    );
    counter!(INCOMING_NEUTRON_EVENTS).absolute(0);

    describe_counter!(
        INCOMING_METADATA_MESSAGES_PROCESSED,
        Unit::Count,
        "total incoming metadata messages processed"
    );
    counter!(INCOMING_METADATA_MESSAGES_PROCESSED).absolute(0);

    describe_counter!(
        INCOMING_MESSAGE_SIZE,
        Unit::Bytes,
        "Total incoming bytes processed"
    );
    counter!(INCOMING_MESSAGE_SIZE).absolute(0);

    describe_counter!(
        OUTGOING_MESSAGE_SIZE,
        Unit::Bytes,
        "Total outgoing message size"
    );
    counter!(OUTGOING_MESSAGE_SIZE).absolute(0);

    describe_counter!(
        OUTGOING_FRAMES,
        Unit::Count,
        "Total outgoing completed frames"
    );
    counter!(OUTGOING_FRAMES).absolute(0);

    describe_counter!(
        OUTGOING_METADATA_MESSAGES,
        Unit::Count,
        "Total metadata messages sent"
    );
    counter!(OUTGOING_METADATA_MESSAGES).absolute(0);

    describe_counter!(
        OUTGOING_EVENT_MESSAGES,
        Unit::Count,
        "Total event messages sent"
    );
    counter!(OUTGOING_EVENT_MESSAGES).absolute(0);

    describe_counter!(
        OUTGOING_NEUTRON_EVENTS,
        Unit::Count,
        "Total number of neutron events sent"
    );
    counter!(OUTGOING_NEUTRON_EVENTS).absolute(0);

    describe_counter!(
        OUTGOING_DROPPED_FRAMES_NO_METADATA,
        Unit::Count,
        "Number of frames dropped due to having insufficient metadata"
    );
    counter!(OUTGOING_DROPPED_FRAMES_NO_METADATA).absolute(0);

    describe_counter!(
        OUTGOING_DROPPED_NEUTRON_EVENTS_NO_METADATA,
        Unit::Count,
        "Number of neutron events dropped due to having insufficient metadata"
    );
    counter!(OUTGOING_DROPPED_NEUTRON_EVENTS_NO_METADATA).absolute(0);

    describe_gauge!(
        QUEUE_FRAMES,
        Unit::Count,
        "Number of partially-complete frames pending in queue"
    );
    gauge!(QUEUE_FRAMES).set(0);

    describe_counter!(
        OUTGOING_KAFKA_ERRORS,
        Unit::Count,
        "Number of errors producing messages to Kafka"
    );
    counter!(OUTGOING_KAFKA_ERRORS).absolute(0);

    Ok(())
}
