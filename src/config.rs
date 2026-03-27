use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug, Default)]
pub struct AggregatorConfig {
    pub input_topic: String,
    pub output_topic: String,
    pub frame_queue_poll_interval_ms: Option<u64>,
    pub reference_time_tolerance_ns: Option<u64>,
    pub max_events_per_message: Option<usize>,
    pub expiry_offset_ms: Option<u64>,
    pub max_queued_frames: Option<usize>,
    pub sort_events_by_tof: Option<bool>,
    pub read_last_message_timeout_ms: Option<u64>,
    pub source_name: Option<String>,
    pub metrics_bind_addr: String,
    pub kafka_producer: HashMap<String, String>,
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

pub fn config_from_str(data: &str) -> Result<AggregatorConfig, toml::de::Error> {
    toml::from_str(data)
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
