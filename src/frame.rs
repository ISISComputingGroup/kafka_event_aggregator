//! Internal representation of a frame of data.

use crate::ev44_events_generated::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use crate::pu00_pulse_metadata_generated::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer
};
use flatbuffers::FlatBufferBuilder;
use rayon::prelude::*;
use std::time::{Duration, Instant};

/// Source name for messages aggregated by this program.
const SOURCE_NAME: &str = "kafka_event_aggregator";

/// Representation of a single neutron event
#[derive(Debug, Copy, Clone)]
pub struct Event {
    /// The time of flight of this event, in nanoseconds since reference time
    time_of_flight: i32,
    /// Pixel ID that detected this neutron event
    pixel_id: i32,
}

impl Event {
    pub fn new(time_of_flight: i32, pixel_id: i32) -> Self {
        Event {
            time_of_flight,
            pixel_id,
        }
    }
}

/// Data corresponding to a single neutron frame
#[derive(Debug)]
pub struct Frame {
    /// Reference time (nanoseconds since epoch)
    reference_time: i64,
    /// Veto flags for this frame
    vetos: u32,
    /// Protons-per-pulse (uAh per frame) for this frame
    protons_per_pulse: Option<f32>,
    /// Which period this frame belongs to
    period: Option<u32>,
    /// List of recorded neutron events
    events: Vec<Event>,
    /// Frame expiry deadline
    ttl_deadline: Instant,
}

impl Frame {
    /// Create a new frame with the specified reference time and a TTL deadline
    /// `expiry_offset_ms` from now.
    pub fn new_with_reference_time(reference_time: i64, expiry_offset_ms: u64) -> Self {
        Frame {
            reference_time,
            ttl_deadline: Instant::now() + Duration::from_millis(expiry_offset_ms),
            period: None,
            vetos: 0,
            events: Vec::with_capacity(100_000), // Limit reallocations; reserve space for 100k events per frame up-front
            protons_per_pulse: None,
        }
    }

    /// Get the timestamp of this Frame in Kafka format (ms since epoch),
    /// from a reference time which is stored as ns since epoch.
    pub fn kafka_timestamp(&self) -> i64 {
        self.reference_time / 1_000_000
    }

    /// Reference time of this frame in nanoseconds since epoch
    pub fn reference_time(&self) -> i64 {
        self.reference_time
    }

    /// Period into which this frame was collected
    pub fn period(&self) -> Option<u32> {
        self.period
    }

    /// Veto flags for this frame
    pub fn vetos(&self) -> u32 {
        self.vetos
    }

    /// Proton charge for this frame (in uAh)
    pub fn proton_charge(&self) -> Option<f32> {
        self.protons_per_pulse
    }

    /// Whether this frame's time-to-live has expired.
    pub fn is_ttl_expired(&self) -> bool {
        Instant::now() >= self.ttl_deadline
    }

    /// Assign new metadata to a frame.
    pub fn set_metadata(&mut self, vetos: Option<u32>, protons_per_pulse: Option<f32>, period: Option<u32>) {
        if let Some(vetos) = vetos {
            self.vetos |= vetos;
        }
        if let Some(protons_per_pulse) = protons_per_pulse {
            self.protons_per_pulse = Some(protons_per_pulse);
        }
        if let Some(period) = period {
            self.period = Some(period);
        }
    }

    /// Append new events to this frame from an iterator.
    pub fn append_events(&mut self, events: impl Iterator<Item = Event>) {
        self.events.extend(events)
    }

    /// Sort the events in this frame by time-of-flight
    pub fn sort_by_tof(&mut self) {
        self.events.par_sort_unstable_by_key(|e| e.time_of_flight);
    }

    /// Emit a pu00 (frame metadata) message for this frame to the provided sink.
    fn emit_pu00_message<F>(
        &self,
        fbb: &'_ mut FlatBufferBuilder<'_>,
        mut sink: F,
    ) where
        F: FnMut(i64, &[u8]),
    {
        let args = Pu00MessageArgs {
            source_name: Some(fbb.create_string(SOURCE_NAME)),
            message_id: 0, // TODO
            proton_charge: self.protons_per_pulse,
            vetos: Some(self.vetos),
            period_number: self.period,
            reference_time: self.reference_time,
        };

        let pu00 = Pu00Message::create(fbb, &args);
        finish_pu_00_message_buffer(fbb, pu00);
        sink(self.kafka_timestamp(), fbb.finished_data());
        fbb.reset();
    }

    /// Emit an ev44 message for the provided events to the specified sink.
    fn emit_ev44_message<F>(
        &self,
        fbb: &'_ mut FlatBufferBuilder<'_>,
        events: &[Event],
        mut sink: F,
    ) where
        F: FnMut(i64, &[u8]),
    {
        let tofs = fbb.create_vector_from_iter(events.iter().map(|e| e.time_of_flight));
        let pixel_ids = fbb.create_vector_from_iter(events.iter().map(|e| e.pixel_id));

        let args = Event44MessageArgs {
            source_name: Some(fbb.create_string(SOURCE_NAME)),
            message_id: 0, // TODO
            reference_time: Some(fbb.create_vector(&[self.reference_time])),
            reference_time_index: Some(fbb.create_vector(&[0])),
            time_of_flight: Some(tofs),
            pixel_id: Some(pixel_ids),
        };

        let ev44 = Event44Message::create(fbb, &args);
        finish_event_44_message_buffer(fbb, ev44);
        sink(self.kafka_timestamp(), fbb.finished_data());
        fbb.reset();
    }

    /// Emit pu00 and ev44 messages representing this frame to the specified sink.
    pub fn emit_messages<F>(
        &self,
        fbb: &mut FlatBufferBuilder<'_>,
        max_events_per_message: usize,
        mut sink: F,
    ) where
        F: FnMut(i64, &[u8]),
    {
        self.emit_pu00_message(fbb, &mut sink);

        self.events
            .chunks(max_events_per_message)
            .for_each(|chunk| self.emit_ev44_message(fbb, chunk, &mut sink))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ev44_events_generated::root_as_event_44_message;
    use crate::pu00_pulse_metadata_generated::root_as_pu_00_message;

    #[test]
    fn test_emit_messages() {
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
            vetos: 0b11010011,
            period: Some(5),
            protons_per_pulse: Some(123.456),
            reference_time: 123456,
            ttl_deadline: Instant::now(),
        };

        let mut fbb = FlatBufferBuilder::new();
        frame.emit_messages(&mut fbb, 2, |_, e| captured_messages.push(e.to_vec()));

        assert_eq!(captured_messages.len(), 3);

        let pu00 = root_as_pu_00_message(&captured_messages[0]).unwrap();
        assert_eq!(
            pu00.vetos(),
            Some(0b11010011)
        );
        assert!(
            (pu00.proton_charge().unwrap() - 123.456).abs() < 0.001
        );
        assert_eq!(
            pu00.period_number(),
            Some(5)
        );

        // First ev44 containing two events
        let event1 = root_as_event_44_message(&captured_messages[1]).unwrap();
        assert_eq!(
            event1.pixel_id().unwrap().iter().collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            event1.time_of_flight().unwrap().iter().collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(event1.reference_time().iter().next(), Some(123456));

        // Second ev44 containing one event
        let event2 = root_as_event_44_message(&captured_messages[2]).unwrap();
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

    #[test]
    fn test_to_kafka_timestamp() {
        let frame = Frame::new_with_reference_time(123_456_789_000_000, 0);
        assert_eq!(frame.kafka_timestamp(), 123_456_789);
    }

    #[test]
    fn test_is_ttl_expired() {
        let frame1 = Frame::new_with_reference_time(123_456_789_000_000, 0);
        assert!(frame1.is_ttl_expired());

        let frame2 = Frame::new_with_reference_time(123_456_789_000_000, 50_000);
        assert!(!frame2.is_ttl_expired());
    }
}
