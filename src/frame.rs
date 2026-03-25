//! Internal representation of a frame of data.

use crate::config::AggregatorConfig;
use crate::metrics::{
    OUTGOING_DROPPED_FRAMES_NO_METADATA, OUTGOING_DROPPED_NEUTRON_EVENTS_NO_METADATA,
    OUTGOING_EVENT_MESSAGES, OUTGOING_FRAMES, OUTGOING_METADATA_MESSAGES, OUTGOING_NEUTRON_EVENTS,
};
use crate::outgoing_message::OutgoingMessage;
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};
use log::warn;
use metrics::counter;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use thread_local::ThreadLocal;

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

static TLS: ThreadLocal<RefCell<FlatBufferBuilder>> = ThreadLocal::new();

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

thread_local! {
    static FBB: RefCell<FlatBufferBuilder<'static>> = RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

impl Frame {
    /// Create a new frame with the specified reference time and a TTL deadline
    /// `expiry_offset_ms` from now.
    pub fn new(reference_time: i64, config: &AggregatorConfig) -> Self {
        Frame {
            reference_time,
            ttl_deadline: Instant::now() + Duration::from_millis(config.expiry_offset_ms),
            period: None,
            vetos: 0,
            events: Vec::with_capacity(config.max_events_per_message),
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

    /// Number of events currently accumulated into this frame
    pub fn num_events(&self) -> usize {
        self.events.len()
    }

    /// Whether this frame's time-to-live has expired.
    pub fn is_ttl_expired(&self) -> bool {
        Instant::now() >= self.ttl_deadline
    }

    pub fn add_vetos(&mut self, vetos: u32) {
        self.vetos |= vetos;
    }

    pub fn set_protons_per_pulse(&mut self, protons_per_pulse: f32) {
        self.protons_per_pulse = Some(protons_per_pulse);
    }

    pub fn set_period(&mut self, period: u32) {
        self.period = Some(period);
    }

    /// Append new events to this frame from an iterator.
    pub fn append_events(&mut self, events: impl Iterator<Item = Event>) {
        self.events.extend(events)
    }

    /// Sort the events in this frame by time-of-flight
    fn sort_by_tof(&mut self) {
        self.events.sort_unstable_by_key(|e| e.time_of_flight);
    }

    fn to_pu00_message(
        &self,
        fbb: &mut FlatBufferBuilder,
        message_id: i64,
        config: &AggregatorConfig,
    ) -> OutgoingMessage {
        fbb.reset();
        let args = Pu00MessageArgs {
            source_name: Some(fbb.create_string(&config.source_name)),
            message_id,
            proton_charge: self.protons_per_pulse,
            vetos: Some(self.vetos),
            period_number: self.period,
            reference_time: self.reference_time,
        };

        let pu00 = Pu00Message::create(fbb, &args);
        finish_pu_00_message_buffer(fbb, pu00);

        counter!(OUTGOING_METADATA_MESSAGES).increment(1);

        OutgoingMessage::new(self.kafka_timestamp(), fbb.finished_data().to_vec())
    }

    /// Emit an ev44 message for the provided events to the specified sink.
    fn to_ev44_message(
        &self,
        fbb: &mut FlatBufferBuilder<'_>,
        message_id: i64,
        events: &[Event],
        config: &AggregatorConfig,
    ) -> OutgoingMessage {
        fbb.reset();

        fbb.start_vector::<i32>(events.len());
        events.iter().map(|e| e.time_of_flight).for_each(|t| {
            fbb.push(t);
        });
        let tofs = fbb.end_vector(events.len());

        fbb.start_vector::<i32>(events.len());
        events.iter().map(|e| e.pixel_id).for_each(|p| {
            fbb.push(p);
        });
        let pixel_ids = fbb.end_vector(events.len());

        let args = Event44MessageArgs {
            source_name: Some(fbb.create_string(&config.source_name)),
            message_id,
            reference_time: Some(fbb.create_vector(&[self.reference_time])),
            reference_time_index: Some(fbb.create_vector(&[0])),
            time_of_flight: Some(tofs),
            pixel_id: Some(pixel_ids),
        };

        let ev44 = Event44Message::create(fbb, &args);
        finish_event_44_message_buffer(fbb, ev44);

        counter!(OUTGOING_EVENT_MESSAGES).increment(1);

        OutgoingMessage::new(self.kafka_timestamp(), fbb.finished_data().to_vec())
    }

    /// Emit pu00 and ev44 messages representing this frame to the specified sink.
    pub fn messages(
        &mut self,
        message_id: Arc<AtomicI64>,
        config: &AggregatorConfig,
    ) -> Vec<OutgoingMessage> {
        let mut fbb = TLS
            .get_or(|| RefCell::new(FlatBufferBuilder::new()))
            .borrow_mut();

        if self.protons_per_pulse.is_none() || self.period.is_none() {
            warn!(
                "Failed to emit partial frame; required metadata for this frame was not present. \
            This can occur if an event message and it's corresponding metadata are not \
            received within expiry_offset_ms of each other."
            );
            counter!(OUTGOING_DROPPED_FRAMES_NO_METADATA).increment(1);
            counter!(OUTGOING_DROPPED_NEUTRON_EVENTS_NO_METADATA)
                .increment(self.events.len() as u64);
            return vec![];
        }

        if config.sort_events_by_tof {
            self.sort_by_tof();
        }

        let num_messages = 1 + self.events.len().div_ceil(config.max_events_per_message);

        let base_id = message_id.fetch_add(num_messages as i64, Ordering::Relaxed);

        let mut msgs = Vec::with_capacity(num_messages);

        msgs.push(self.to_pu00_message(&mut fbb, base_id, config));

        msgs.extend(
            self.events
                .chunks(config.max_events_per_message)
                .zip(base_id + 1..)
                .map(|(chunk, id)| self.to_ev44_message(&mut fbb, id, chunk, config)),
        );

        counter!(OUTGOING_FRAMES).increment(1);
        counter!(OUTGOING_NEUTRON_EVENTS).increment(self.events.len() as u64);

        msgs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use isis_streaming_data_types::flatbuffers_generated::events_ev44::root_as_event_44_message;
    use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::root_as_pu_00_message;

    #[test]
    fn test_emit_messages() {
        let mut frame = Frame {
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

        let tls = ThreadLocal::new();
        tls.get_or(|| RefCell::new(FlatBufferBuilder::new()));

        let msg_id = Arc::new(AtomicI64::new(0));
        let captured_messages = frame.messages(
            msg_id,
            &AggregatorConfig {
                max_events_per_message: 2,
                ..Default::default()
            },
        );

        assert_eq!(captured_messages.len(), 3);

        let pu00 = root_as_pu_00_message(&captured_messages[0].content()).unwrap();
        assert_eq!(pu00.vetos(), Some(0b11010011));
        assert!((pu00.proton_charge().unwrap() - 123.456).abs() < 0.001);
        assert_eq!(pu00.period_number(), Some(5));

        // First ev44 containing two events
        let event1 = root_as_event_44_message(&captured_messages[1].content()).unwrap();
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
        let event2 = root_as_event_44_message(&captured_messages[2].content()).unwrap();
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
        let frame = Frame::new(
            123_456_789_000_000,
            &AggregatorConfig {
                ..Default::default()
            },
        );
        assert_eq!(frame.kafka_timestamp(), 123_456_789);
    }

    #[test]
    fn test_is_ttl_expired() {
        let frame1 = Frame::new(
            123_456_789_000_000,
            &AggregatorConfig {
                max_events_per_message: 10_000,
                expiry_offset_ms: 0,
                ..Default::default()
            },
        );
        assert!(frame1.is_ttl_expired());

        let frame2 = Frame::new(
            123_456_789_000_000,
            &AggregatorConfig {
                max_events_per_message: 10_000,
                expiry_offset_ms: 10_000,
                ..Default::default()
            },
        );
        assert!(!frame2.is_ttl_expired());
    }
}
