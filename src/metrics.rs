use crate::config::AggregatorConfig;
use metrics::{Unit, counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

pub const INCOMING_MESSAGES_DROPPED: &str = "aggregator_incoming_messages_dropped";
pub const INCOMING_MESSAGES_PROCESSED: &str = "aggregator_incoming_messages_processed";
pub const INCOMING_NEUTRON_EVENTS: &str = "aggregator_incoming_neutron_events";
pub const INCOMING_MESSAGE_SIZE: &str = "aggregator_incoming_message_size";


pub const OUTGOING_MESSAGE_SIZE: &str = "aggregator_outgoing_message_size";
pub const OUTGOING_FRAMES: &str = "aggregator_outgoing_frames";
pub const OUTGOING_MESSAGES: &str = "aggregator_outgoing_messages";
pub const OUTGOING_NEUTRON_EVENTS: &str = "aggregator_outgoing_neutron_events";

pub const OUTGOING_DROPPED_FRAMES: &str = "aggregator_outgoing_dropped_frames";
pub const OUTGOING_DROPPED_NEUTRON_EVENTS: &str = "aggregator_outgoing_dropped_neutron_events";

pub const OUTGOING_KAFKA_ERRORS: &str = "aggregator_outgoing_kafka_errors";
pub const QUEUE_FRAMES: &str = "aggregator_queue_frames";

pub struct IncomingMessageDropReason {}
impl IncomingMessageDropReason {
    pub const NO_PAYLOAD: &str = "no_payload";
    pub const FAILED_DESERIALIZE: &str = "failed_deserialize";
    pub const UNKNOWN_SCHEMA: &str = "unknown_schema";
}

pub struct OutgoingFrameDropReason {}

impl OutgoingFrameDropReason {
    pub const NO_METADATA: &str = "no_metadata";
}

pub fn initialize_metrics(config: &AggregatorConfig) -> anyhow::Result<()> {
    let builder = PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener((config.metrics_bind_addr()).parse::<SocketAddr>()?);

    builder.install()?;

    describe_counter!(
        INCOMING_MESSAGES_PROCESSED,
        Unit::Count,
        "total incoming Kafka messages processed"
    );
    counter!(INCOMING_MESSAGES_PROCESSED).absolute(0);
    counter!(INCOMING_MESSAGES_PROCESSED, "schema" => "ev44").absolute(0);
    counter!(INCOMING_MESSAGES_PROCESSED, "schema" => "pu00").absolute(0);

    counter!(INCOMING_MESSAGES_DROPPED).absolute(0);
    counter!(INCOMING_MESSAGES_DROPPED, "reason" => IncomingMessageDropReason::FAILED_DESERIALIZE).absolute(0);
    counter!(INCOMING_MESSAGES_DROPPED, "reason" => IncomingMessageDropReason::NO_PAYLOAD).absolute(0);
    counter!(INCOMING_MESSAGES_DROPPED, "reason" => IncomingMessageDropReason::UNKNOWN_SCHEMA).absolute(0);

    describe_counter!(
        INCOMING_NEUTRON_EVENTS,
        Unit::Count,
        "total incoming event messages processed"
    );
    counter!(INCOMING_NEUTRON_EVENTS).absolute(0);

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
        OUTGOING_MESSAGES,
        Unit::Count,
        "Total metadata messages sent"
    );
    counter!(OUTGOING_MESSAGES).absolute(0);
    counter!(OUTGOING_MESSAGES, "schema" => "ev44").absolute(0);
    counter!(OUTGOING_MESSAGES, "schema" => "pu00").absolute(0);

    describe_counter!(
        OUTGOING_NEUTRON_EVENTS,
        Unit::Count,
        "Total number of neutron events sent"
    );
    counter!(OUTGOING_NEUTRON_EVENTS).absolute(0);

    describe_counter!(
        OUTGOING_DROPPED_FRAMES,
        Unit::Count,
        "Number of frames dropped due to having insufficient metadata"
    );
    counter!(OUTGOING_DROPPED_FRAMES).absolute(0);
    counter!(OUTGOING_DROPPED_FRAMES, "reason" => OutgoingFrameDropReason::NO_METADATA).absolute(0);

    describe_counter!(
        OUTGOING_DROPPED_NEUTRON_EVENTS,
        Unit::Count,
        "Number of neutron events dropped due to having insufficient metadata"
    );
    counter!(OUTGOING_DROPPED_NEUTRON_EVENTS).absolute(0);
    counter!(OUTGOING_DROPPED_FRAMES, "reason" => OutgoingFrameDropReason::NO_METADATA).absolute(0);

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
