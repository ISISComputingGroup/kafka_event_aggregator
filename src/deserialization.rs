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

impl ReceivedMessage<'_> {
    pub fn message_id(&self) -> i64 {
        match self {
            ReceivedMessage::Ev44(ev44_message) => ev44_message.message_id(),
            ReceivedMessage::Pu00(pu00_message) => pu00_message.message_id(),
        }
    }
}

/// Deserialize a message from the `_rawEvents` Kafka topic.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialization::ReceivedMessage::{Ev44, Pu00};
    use crate::ev44_events_generated::{Event44MessageArgs, finish_event_44_message_buffer};
    use crate::pu00_pulse_metadata_generated::{Pu00MessageArgs, finish_pu_00_message_buffer};
    use flatbuffers::FlatBufferBuilder;

    #[test]
    fn test_deserialize_pu00() {
        let mut fbb = FlatBufferBuilder::new();
        let pu00_args = Pu00MessageArgs {
            source_name: Some(fbb.create_string("")),
            ..Default::default()
        };

        let pu00 = Pu00Message::create(&mut fbb, &pu00_args);
        finish_pu_00_message_buffer(&mut fbb, pu00);

        let deserialized = deserialize(fbb.finished_data());

        match deserialized {
            Ok(Pu00(_)) => {}
            _ => panic!("Deserializing pu00 did not give a pu00 message"),
        }
    }

    #[test]
    fn test_deserialize_ev44() {
        let mut fbb = FlatBufferBuilder::new();
        let ev44_args = Event44MessageArgs {
            source_name: Some(fbb.create_string("")),
            reference_time: Some(fbb.create_vector(&[0_i64])),
            reference_time_index: Some(fbb.create_vector(&[0_i32])),
            ..Default::default()
        };

        let ev44 = Event44Message::create(&mut fbb, &ev44_args);
        finish_event_44_message_buffer(&mut fbb, ev44);

        let deserialized = deserialize(fbb.finished_data());

        match deserialized {
            Ok(Ev44(_)) => {}
            _ => panic!("Deserializing ev44 did not give a ev44 message"),
        }
    }
}
