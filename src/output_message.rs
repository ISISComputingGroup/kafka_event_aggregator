/// An output message to be sent to Kafka.
pub struct OutputMessage<'a> {
    pub kafka_timestamp: i64,
    pub key: u64,
    pub payload: &'a [u8],
}
