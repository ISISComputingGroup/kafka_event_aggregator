use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug, Default)]
pub struct AggregatorConfig {
    pub input_topic: String,
    pub output_topic: String,
    pub frame_queue_poll_interval_ms: u64,
    pub reference_time_tolerance_ns: u64,
    pub max_events_per_message: usize,
    pub expiry_offset_ms: u64,
    pub kafka_producer: HashMap<String, String>,
    pub kafka_consumer: HashMap<String, String>,
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
