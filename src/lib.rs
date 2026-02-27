#[allow(non_snake_case)]
#[path = "flatbuffers_generated/ev44_events_generated.rs"]
#[allow(clippy::all)]
pub mod ev44_events_generated;

use crate::ev44_events_generated::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use flatbuffers::FlatBufferBuilder;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) struct Event {
    time_of_flight: i32,
    pixel_id: i32,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct FrameMetadata {
    vetos: u64,             // TODO: nowhere to actually put this yet
    protons_per_pulse: f64, // TODO: nowhere to actually put this yet
}

#[derive(Debug)]
pub(crate) struct Frame {
    reference_time: i64,
    events: Vec<Event>,
}

impl Frame {
    fn sort_by_tof(&mut self) {
        // TODO: is a stable sort faster for multiple concatenated sorted inputs? Benchmark it.
        self.events.sort_unstable_by_key(|e| e.time_of_flight);
    }

    fn to_ev44_message<'a, 'b, F>(
        &self,
        fbb: &'b mut FlatBufferBuilder<'a>,
        events: &[Event],
        mut sink: F,
    ) where
        F: FnMut(&[u8]),
    {
        let tofs = fbb.create_vector_from_iter(events.iter().map(|e| e.time_of_flight));
        let pixel_ids = fbb.create_vector_from_iter(events.iter().map(|e| e.pixel_id));

        let args = Event44MessageArgs {
            source_name: Some(fbb.create_string("foo")),
            message_id: 0,
            reference_time: Some(fbb.create_vector(&[self.reference_time])),
            reference_time_index: Some(fbb.create_vector(&[0])),
            time_of_flight: Some(tofs),
            pixel_id: Some(pixel_ids),
        };

        let ev44 = Event44Message::create(fbb, &args);
        finish_event_44_message_buffer(fbb, ev44);
        sink(fbb.finished_data());
        fbb.reset();
    }

    fn to_ev44_messages<'a, F>(
        &self,
        fbb: &mut FlatBufferBuilder<'a>,
        max_events_per_message: usize,
        mut sink: F,
    ) where
        F: FnMut(&[u8]),
    {
        self.events
            .chunks(max_events_per_message)
            .for_each(|chunk| self.to_ev44_message(fbb, chunk, &mut sink))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ev44_events_generated::root_as_event_44_message;

    #[test]
    fn test_to_ev44_messages() {
        let mut captured_messages = vec![];

        let frame = Frame {
            events: vec![
                Event {
                    pixel_id: 0,
                    time_of_flight: 1,
                },
                Event {
                    pixel_id: 2,
                    time_of_flight: 3,
                },
                Event {
                    pixel_id: 5,
                    time_of_flight: 6,
                },
            ],
            reference_time: 123456,
        };

        let mut fbb = FlatBufferBuilder::new();
        frame.to_ev44_messages(&mut fbb, 2, |e| captured_messages.push(e.to_vec()));

        assert_eq!(captured_messages.len(), 2);

        let event1 = root_as_event_44_message(&captured_messages[0]).unwrap();
        assert_eq!(
            event1.pixel_id().unwrap().iter().collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            event1.time_of_flight().unwrap().iter().collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(event1.reference_time().iter().next(), Some(123456));

        let event2 = root_as_event_44_message(&captured_messages[1]).unwrap();
        assert_eq!(
            event2.pixel_id().unwrap().iter().collect::<Vec<_>>(),
            vec![5]
        );
        assert_eq!(
            event2.time_of_flight().unwrap().iter().collect::<Vec<_>>(),
            vec![6]
        );
        assert_eq!(event2.reference_time().iter().next(), Some(123456));
    }
}
