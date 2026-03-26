//! Frame queue of pending frames, which will be accumulated into and sent to
//! Kafka once their time-to-live expires.

use crate::config::AggregatorConfig;
use crate::frame::{Event, Frame};
use crate::metrics::{
    INCOMING_EVENT_MESSAGES_PROCESSED, INCOMING_INVALID_MESSAGES_DISCARDED, INCOMING_MESSAGE_SIZE,
    INCOMING_MESSAGES_PROCESSED, INCOMING_METADATA_MESSAGES_PROCESSED, INCOMING_NEUTRON_EVENTS,
};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::Event44Message;
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::Pu00Message;
use isis_streaming_data_types::{DeserializedMessage, deserialize_message, get_schema_id};
use log::{debug, warn};
use metrics::counter;
use std::collections::VecDeque;
use std::time::Instant;

/// A queue of frames, ordered by the arrival time of the first ev44 in each frame
/// in the rawEvents Kafka consumer (which is also ordered by time-to-live).
#[derive(Debug)]
pub struct FrameQueue<'a> {
    /// New frames pushed to back of queue, old frames popped from front of queue
    frames: VecDeque<Frame>,
    /// Aggregator configuration
    config: &'a AggregatorConfig,
    /// Message ID
    message_id: i64,
}

impl<'a> FrameQueue<'a> {
    pub fn new(config: &'a AggregatorConfig, next_message_id: i64) -> Self {
        FrameQueue {
            frames: VecDeque::new(),
            config,
            message_id: next_message_id,
        }
    }

    /// Current size of queue (number of frames)
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Whether the queue is empty
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn send_expired_frames<F>(&mut self, fbb: &mut FlatBufferBuilder<'_>, mut sink: F)
    where
        F: FnMut(i64, &[u8]),
    {
        while self.frames.len() > self.config.max_queued_frames {
            let mut completed_frame = self
                .frames
                .pop_front()
                .expect("unreachable; frames is empty after checking a frame exists");

            debug!(
                "Sending frame as queue is too long (reference_time={}, neutron_events={})",
                completed_frame.reference_time(),
                completed_frame.num_events(),
            );
            completed_frame.emit_messages(fbb, &mut self.message_id, self.config, &mut sink);
        }

        let now = Instant::now();

        while self
            .frames
            .front()
            .map(|f| f.is_ttl_expired(&now))
            .unwrap_or(false)
        {
            let mut completed_frame = self
                .frames
                .pop_front()
                .expect("unreachable; frames is empty after checking first frame exists");

            debug!(
                "Sending expired frame (reference_time={}, neutron_events={})",
                completed_frame.reference_time(),
                completed_frame.num_events(),
            );
            completed_frame.emit_messages(fbb, &mut self.message_id, self.config, &mut sink);
        }
    }

    /// Applies a mutation to a frame with the given reference time.
    ///
    /// If a frame with the specified reference time, within a tolerance, is already
    /// present, the mutation is applied to that frame.
    ///
    /// If no such frame is present, then a new frame is inserted and the mutation
    /// is immediately applied to the new frame. The new frame's expiry time will be
    /// an offset from the arrival time of the first message which causes the frame to
    /// be created.
    fn apply_to_frame<F>(&mut self, reference_time: i64, f: F)
    where
        F: FnOnce(&mut Frame),
    {
        let frame = match self
            .frames
            .iter_mut()
            .rev() // Start search from the most recent frame; this is where it is most likely to be.
            .find(|frame| {
                frame.reference_time().abs_diff(reference_time)
                    <= self.config.reference_time_tolerance_ns
            }) {
            Some(frame) => frame,
            None => {
                self.frames
                    .push_back(Frame::new(reference_time, self.config));
                self.frames
                    .back_mut()
                    .expect("unreachable; frames is empty after pushing new frame")
            }
        };

        f(frame)
    }

    pub fn process_raw_pu00_metadata_message(&mut self, msg: &Pu00Message) {
        let reference_time = msg.reference_time();
        self.apply_to_frame(reference_time, |frame| {
            if let Some(vetos) = msg.vetos() {
                frame.add_vetos(vetos);
            }
            if let Some(protons_per_pulse) = msg.proton_charge() {
                frame.set_protons_per_pulse(protons_per_pulse);
            }
            if let Some(period_number) = msg.period_number() {
                frame.set_period(period_number);
            }
        })
    }

    pub fn process_raw_ev44_message(&mut self, msg: &Event44Message) {
        if msg.reference_time().len() == 1 {
            self.apply_to_frame(msg.reference_time().get(0), |frame| {
                if let Some(tofs) = msg.time_of_flight()
                    && let Some(pixel_ids) = msg.pixel_id()
                {
                    counter!(INCOMING_NEUTRON_EVENTS).increment(tofs.len() as u64);
                    frame.append_events(
                        tofs.into_iter()
                            .zip(pixel_ids)
                            .map(|(tof, pixel)| Event::new(tof, pixel)),
                    )
                } else {
                    warn!("Ignoring event without time_of_flight or pixel_id datasets present")
                }
            })
        } else {
            warn!(
                "Ignoring event with unexpected number of reference times: {} (expected a single reference time)",
                msg.reference_time().len()
            );
        }
    }

    pub fn process_raw_message(&mut self, msg: &[u8]) {
        match deserialize_message(msg) {
            Ok(DeserializedMessage::EventDataEv44(data)) => {
                self.process_raw_ev44_message(&data);
                counter!(INCOMING_EVENT_MESSAGES_PROCESSED).increment(1);
            }
            Ok(DeserializedMessage::PulseMetadataPu00(data)) => {
                self.process_raw_pu00_metadata_message(&data);
                counter!(INCOMING_METADATA_MESSAGES_PROCESSED).increment(1);
            }
            Ok(_) => {
                warn!("Unhandled message type: {:?}", get_schema_id(msg));
            }
            Err(e) => {
                warn!("Cannot deserialize message: {e:?}");
                counter!(INCOMING_INVALID_MESSAGES_DISCARDED).increment(1);
            }
        }

        counter!(INCOMING_MESSAGES_PROCESSED).increment(1);
        counter!(INCOMING_MESSAGE_SIZE).increment(msg.len() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_metadata_to_frame() {
        let config = AggregatorConfig {
            max_events_per_message: 2,
            reference_time_tolerance_ns: 10,
            ..Default::default()
        };
        let mut frame_queue = FrameQueue::new(&config, 10);

        frame_queue.apply_to_frame(1, |frame| {
            frame.add_vetos(0b11010011);
            frame.set_period(3);
        });
        assert_eq!(frame_queue.len(), 1);

        // Reference time within 10ns of above frame; should add to that same frame
        // ORing together vetos and overwriting period if provided
        frame_queue.apply_to_frame(5, |frame| {
            frame.add_vetos(0b11110000);
            frame.set_period(5);
        });
        assert_eq!(frame_queue.len(), 1);

        // Reference time outside 10ns of first frame; should add a new frame
        frame_queue.apply_to_frame(15, |frame| {
            frame.add_vetos(0);
            frame.set_period(6);
        });
        assert_eq!(frame_queue.len(), 2);

        assert_eq!(frame_queue.frames[0].period(), Some(5));
        assert_eq!(frame_queue.frames[0].vetos(), 0b11110011);

        assert_eq!(frame_queue.frames[1].period(), Some(6));
        assert_eq!(frame_queue.frames[1].vetos(), 0);
    }
}
