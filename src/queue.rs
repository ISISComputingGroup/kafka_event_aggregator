//! Frame queue of pending frames, which will be accumulated into and sent to
//! Kafka once their time-to-live expires.

use crate::deserialization::{ReceivedMessage, deserialize};
use crate::ev44_events_generated::Event44Message;
use crate::frame::{Event, Frame};
use crate::pu00_pulse_metadata_generated::Pu00Message;
use flatbuffers::FlatBufferBuilder;
use log::{error, warn};
use std::collections::VecDeque;

/// A queue of frames, ordered by the arrival time of the first ev44 in each frame
/// in the rawEvents Kafka consumer (which is also ordered by time-to-live).
#[derive(Debug, Default)]
pub struct FrameQueue {
    /// New frames pushed to back of queue, old frames popped from front of queue
    frames: VecDeque<Frame>,
    /// How long, in ms, to keep frames in the queue before emitting them
    expiry_offset_ms: u64,
    /// If two frames have the same reference time to within this many nanoseconds,
    /// then they are considered to be the same frame
    reference_time_tolerance_ns: u64,
}

impl FrameQueue {
    pub fn new(expiry_offset_ms: u64, reference_time_tolerance_ns: u64) -> Self {
        FrameQueue {
            frames: VecDeque::new(),
            expiry_offset_ms,
            reference_time_tolerance_ns,
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

    pub fn send_expired_frames<F>(
        &mut self,
        fbb: &mut FlatBufferBuilder<'_>,
        max_events_per_message: usize,
        mut sink: F,
    ) where
        F: FnMut(i64, &[u8]),
    {
        while self
            .frames
            .front()
            .map(|f| f.is_ttl_expired())
            .unwrap_or(false)
        {
            let mut completed_frame = self
                .frames
                .pop_front()
                .expect("unreachable; frames is empty after checking first frame exists");

            completed_frame.sort_by_tof();

            completed_frame.emit_messages(fbb, max_events_per_message, &mut sink)
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
    fn apply_to_frame<F>(&'_ mut self, reference_time: i64, mut f: F)
    where
        F: FnMut(&mut Frame),
    {
        let frame = match self
            .frames
            .iter_mut()
            .rev() // Start search from the most recent frame; this is where it is most likely to be.
            .find(|frame| {
                frame.reference_time().abs_diff(reference_time) <= self.reference_time_tolerance_ns
            }) {
            Some(frame) => frame,
            None => {
                self.frames.push_back(Frame::new_with_reference_time(
                    reference_time,
                    self.expiry_offset_ms,
                ));
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
            frame.set_metadata(msg.vetos(), msg.proton_charge(), msg.period_number());
        })
    }

    pub fn process_raw_ev44_message(&mut self, msg: &Event44Message) {
        if msg.reference_time().len() == 1 {
            self.apply_to_frame(msg.reference_time().get(0), |frame| {
                if let Some(tofs) = msg.time_of_flight()
                    && let Some(pixel_ids) = msg.pixel_id()
                {
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
        match deserialize(msg) {
            Ok(ReceivedMessage::Ev44(data)) => self.process_raw_ev44_message(&data),
            Ok(ReceivedMessage::Pu00(data)) => self.process_raw_pu00_metadata_message(&data),
            Err(e) => {
                error!("Cannot deserialize message: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_metadata_to_frame() {
        let mut frame_queue = FrameQueue::new(0, 10);

        frame_queue.apply_to_frame(1, |frame| {
            frame.set_metadata(Some(0b11010011), None, Some(3))
        });
        assert_eq!(frame_queue.len(), 1);

        // Reference time within 10ns of above frame; should add to that same frame
        // ORing together vetos and overwriting period if provided
        frame_queue.apply_to_frame(5, |frame| {
            frame.set_metadata(Some(0b11110000), None, Some(5))
        });
        assert_eq!(frame_queue.len(), 1);

        // Reference time outside 10ns of first frame; should add a new frame
        frame_queue.apply_to_frame(15, |frame| frame.set_metadata(Some(0), None, Some(6)));
        assert_eq!(frame_queue.len(), 2);

        assert_eq!(frame_queue.frames[0].period(), Some(5));
        assert_eq!(frame_queue.frames[0].vetos(), 0b11110011);

        assert_eq!(frame_queue.frames[1].period(), Some(6));
        assert_eq!(frame_queue.frames[1].vetos(), 0);
    }
}
