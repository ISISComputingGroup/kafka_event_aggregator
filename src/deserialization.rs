//! Flatbuffer deserialization utilities.

use crate::ev44_events_generated::{Event44Message, root_as_event_44_message};
use crate::pu00_pulse_metadata_generated::{Pu00Message, root_as_pu_00_message};

/// A message received from Kafka on the _rawEvents topic, which may be:
/// - A `pu00` message from the streaming control board
/// - A `pu00` message from an individual detector module
/// - An `ev44` message with events from a detector module
pub enum ReceivedMessage<'a> {
    Ev44(Event44Message<'a>),
    Pu00(Pu00Message<'a>),
}

/// Deserialize an arbitrary message from the `_rawEvents` Kafka topic.
pub fn deserialize(value: &[u8]) -> Result<ReceivedMessage<'_>, String> {
    let identifier = value
        .get(4..8)
        .ok_or_else(|| "Cannot extract schema ID; invalid message".to_owned())?;

    match identifier {
        b"ev44" => root_as_event_44_message(value)
            .map(ReceivedMessage::Ev44)
            .map_err(|e| e.to_string()),

        b"pu00" => root_as_pu_00_message(value)
            .map(ReceivedMessage::Pu00)
            .map_err(|e| e.to_string()),

        _ => Err(format!("Invalid schema identifier: {identifier:?}")),
    }
}
