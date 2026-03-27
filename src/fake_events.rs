use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};
use rand::RngExt;
use rand::rngs::ChaCha8Rng;
use std::iter;

/// Make fake flatbuffers-encoded events
/// NOTE: this is only used in tests and benchmarks.
pub fn make_fake_flatbuffers_encoded_events(
    rng: &mut ChaCha8Rng,
    reference_time: i64,
    num_messages: usize,
    events_per_message: usize,
) -> Vec<Vec<u8>> {
    (0..num_messages)
        .map(|_| {
            let mut pixel_ids = Vec::with_capacity(events_per_message);
            let mut tofs = Vec::with_capacity(events_per_message);

            for _ in 0..events_per_message {
                pixel_ids.push(rng.random_range(0..65536));
                tofs.push(rng.random_range(0..100_000_000));
            }

            let mut fbb = FlatBufferBuilder::new();
            let args = Event44MessageArgs {
                source_name: Some(fbb.create_string("source_name")),
                message_id: 0,
                reference_time: Some(fbb.create_vector(&[reference_time])),
                reference_time_index: Some(fbb.create_vector(&[0])),
                time_of_flight: Some(fbb.create_vector(&tofs)),
                pixel_id: Some(fbb.create_vector(&pixel_ids)),
            };

            let ev44 = Event44Message::create(&mut fbb, &args);
            finish_event_44_message_buffer(&mut fbb, ev44);
            fbb.finished_data().to_owned()
        })
        .chain(iter::once_with(|| {
            let mut fbb = FlatBufferBuilder::new();
            let args = Pu00MessageArgs {
                source_name: Some(fbb.create_string("source_name")),
                message_id: 0,
                reference_time,
                proton_charge: Some(1.2345),
                vetos: Some(0b11010011),
                period_number: Some(1),
            };
            let pu00 = Pu00Message::create(&mut fbb, &args);
            finish_pu_00_message_buffer(&mut fbb, pu00);
            fbb.finished_data().to_owned()
        }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use isis_streaming_data_types::{DeserializedMessage, deserialize_message};
    use rand::SeedableRng;
    use rand::rngs::ChaCha8Rng;

    #[test]
    fn test_fake_events_deserialize() {
        let mut rng = ChaCha8Rng::seed_from_u64(0);
        let events = make_fake_flatbuffers_encoded_events(&mut rng, 20_000_000, 1, 5);

        assert_eq!(events.len(), 2); // ev44 followed by pu00

        match deserialize_message(events.get(0).unwrap()) {
            Ok(DeserializedMessage::EventDataEv44(data)) => {
                assert_eq!(data.reference_time().get(0), 20_000_000);
                assert_eq!(data.reference_time_index().get(0), 0);
                assert_eq!(data.time_of_flight().unwrap().len(), 5);
                assert_eq!(data.pixel_id().unwrap().len(), 5);
            }
            _ => panic!("did not deserialize as expected"),
        }

        match deserialize_message(events.get(1).unwrap()) {
            Ok(DeserializedMessage::PulseMetadataPu00(data)) => {
                assert_eq!(data.reference_time(), 20_000_000);
            }
            _ => panic!("did not deserialize as expected"),
        }
    }
}
