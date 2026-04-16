//! Config-handling utilities

use serde::Deserialize;
use std::collections::HashMap;

/// Global configuration flags for `kafka_event_aggregator`
#[derive(Deserialize, Debug, Default)]
pub struct AggregatorConfig {
    /// Input Kafka topic to read from (usually ends with `_rawEvents`)
    pub input_topic: String,

    /// Output Kafka topic to write to (usually ends with `_events`)
    pub output_topic: String,

    /// How often to look for expired frames to send them to Kafka.
    pub frame_queue_poll_interval_ms: Option<u64>,

    /// Reference-time comparison tolerance.
    ///
    /// If two frames have equal reference times to within this tolerance,
    /// they are considered the same frame. The reference time of the frame
    /// is determined by the first message which arrives.
    pub reference_time_tolerance_ns: Option<u64>,

    /// Maximum number of neutron events to emit in each output Kafka message.
    /// Larger messages are somewhat more efficient, however the maximum resulting
    /// message size must fit within `max.message.bytes` on the Kafka broker.
    pub max_events_per_message: Option<usize>,

    /// When a new frame arrives, it's expiry time will be set to the current time
    /// plus this offset. All events for this frame are assumed to be consumed, in
    /// `kafka_event_aggregator`'s kafka producer, within this time.
    ///
    /// Note that this may be affected by Kafka consumer batching and must account
    /// for the fact that messages from the same frame may be delivered in different
    /// Kafka batches.
    pub expiry_offset_ms: Option<u64>,

    /// A maximum number of queued frames allowed to accumulate in this program. If there
    /// are more than this many frames, the oldest frames are emitted, even if they have
    /// not yet reached their expiry time.
    pub max_queued_frames: Option<usize>,

    /// Whether to sort neutron events by time-of-flight before emitting them.
    ///
    /// The input events are generally ToF ordered, however this process concatenates
    /// multiple events from multiple detectors, so the per-detector order is lost.
    /// This flag restores the ordering.
    pub sort_events_by_tof: Option<bool>,

    /// Timeout on reading the last message(s) emitted to the `_events` stream, to get the
    /// most recent message ID at startup. This configuration parameter is only used at startup.
    pub read_last_message_timeout_ms: Option<u64>,

    /// String to insert into the `source_name` field of output `pu00` and `ev44` messages.
    pub source_name: Option<String>,

    /// IP and port on which to bind the metrics server.
    /// Example: `127.0.0.1:8484`
    pub metrics_bind_addr: String,

    /// Map of Kafka producer configuration properties. Values should be provided as strings.
    /// All properties are passed through to `librdkafka`.
    pub kafka_producer: HashMap<String, String>,

    /// Map of Kafka consumer configuration properties. Values should be provided as strings.
    /// All properties are passed through to `librdkafka`.
    pub kafka_consumer: HashMap<String, String>,
}

impl AggregatorConfig {
    pub fn input_topic(&self) -> &str {
        &self.input_topic
    }

    pub fn output_topic(&self) -> &str {
        &self.output_topic
    }

    pub fn frame_queue_poll_interval_ms(&self) -> u64 {
        self.frame_queue_poll_interval_ms.unwrap_or(200)
    }

    pub fn reference_time_tolerance_ns(&self) -> u64 {
        self.reference_time_tolerance_ns.unwrap_or(10)
    }

    pub fn max_events_per_message(&self) -> usize {
        self.max_events_per_message.unwrap_or(100_000)
    }

    pub fn expiry_offset_ms(&self) -> u64 {
        self.expiry_offset_ms.unwrap_or(500)
    }

    pub fn max_queued_frames(&self) -> usize {
        self.max_queued_frames.unwrap_or(200)
    }

    pub fn sort_events_by_tof(&self) -> bool {
        self.sort_events_by_tof.unwrap_or(true)
    }

    pub fn read_last_message_timeout_ms(&self) -> u64 {
        self.read_last_message_timeout_ms.unwrap_or(5000)
    }

    pub fn source_name(&self) -> &str {
        if let Some(ref name) = self.source_name {
            name
        } else {
            "kafka_event_aggregator"
        }
    }

    pub fn metrics_bind_addr(&self) -> &str {
        &self.metrics_bind_addr
    }

    pub fn kafka_producer_settings(&self) -> &HashMap<String, String> {
        &self.kafka_producer
    }

    pub fn kafka_consumer_settings(&self) -> &HashMap<String, String> {
        &self.kafka_consumer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toml_deserialize() {
        let config: AggregatorConfig = toml::from_str(
            &"
input_topic = 'machine_rawEvents'
output_topic = 'machine_events'

frame_queue_poll_interval_ms = 100
max_events_per_message = 100000
reference_time_tolerance_ns = 100
expiry_offset_ms = 100
max_queued_frames = 50
source_name = 'kafka_event_aggregator'
metrics_bind_addr = '127.0.0.1:1234'
sort_events_by_tof = true

[kafka_producer]
foo = 'bar'

[kafka_consumer]
foo2 = 'bar2'
",
        )
        .unwrap();

        assert_eq!(config.input_topic, "machine_rawEvents");
        assert_eq!(config.output_topic, "machine_events");
        assert_eq!(config.kafka_producer.get("foo").unwrap(), "bar");
        assert_eq!(config.kafka_consumer.get("foo2").unwrap(), "bar2");
    }
}
