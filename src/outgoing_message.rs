pub struct OutgoingMessage {
    timestamp: i64,
    bytes: Vec<u8>,
}

impl OutgoingMessage {

    pub fn new(timestamp: i64, bytes: Vec<u8>) -> OutgoingMessage {
        OutgoingMessage { timestamp, bytes }
    }

    pub fn content(&self) -> &[u8] {
        &self.bytes
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }
}
