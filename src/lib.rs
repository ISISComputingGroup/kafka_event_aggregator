//! # `kafka_event_aggregator`
//!
//! This program _aggregates_ events from a `_rawEvents` Kafka topic, and emits
//! events onto an `_events` topic.
//!
//! ## Operation
//!
//! When data arrives on the `_rawEvents` topic, `kafka_event_aggregator` will:
//! - Check whether this data forms part of an existing frame. If not, a new Frame will be created,
//!   with a time-to-live of `expiry_offset_ms`.
//! - Append neutron events from the new data, or combine veto statuses from the new data, with the
//!   existing or just-created frame
//!
//! When a frame of data's time-to-live expires, and the next `frame_queue_poll_interval` timer
//! interval expires, `kafka_event_aggregator` will:
//! - Sort the events in the frame by increasing time of flight
//! - Emit a `pu00` frame metadata message to the `_events` stream
//! - Emit zero or more `ev44` messages, each containing no more than `max_events_per_message`
//!   neutron events per `ev44` message.
//!
//! ## `events` stream format
//!
//! Consumers of the `_events` stream may make the following assumptions:
//! - When the stream is sorted by `message_id`, the most recently seen `pu00` message contains the
//!   metadata relevant to the neutron events until the next `pu00` message. The messages are
//!   emitted in order by `message_id`, but if the `_events` stream is distributed across multiple
//!   Kafka partitions then it is not guaranteed to be received in order.
//!   If the `_events` stream is on a single partition, ordering by `message_id`
//!   is equivalent to the message order in Kafka (as long as `enable.idempotence = true` in the
//!   producer)
//! - Events will be emitted ordered in time-of-flight, both within a single `ev44` message and
//!   across multiple `ev44` messages within one frame, if `sort_events_by_tof = true`.
//! - Each `ev44` on the `_events` stream will contain at most `max_events_per_message` events
//! - The `reference_time` of all `pu00` and `ev44` messages which make up a frame will be
//!   identical.
//!
//! For the avoidance of doubt, it is **not** correct to assume that:
//! - Each frame will only contain one `ev44`. This may be true when count rate is small, but may
//!   not be true for instruments with a high count rate.
//! - A frame will contain an `ev44`. If all events are vetoed, no `ev44` messages will be emitted.
//!   (A `pu00` frame metadata message will still be emitted, containing the veto flags).
//! - Frames will be emitted in increasing order of reference time. Due to the underlying UDP
//!   connection from the streaming hardware, this cannot be guaranteed (but is expected to be
//!   true *most* of the time).
//! - A frame must be "complete" to be emitted. Due to the underlying UDP connection from the
//!   streaming hardware, and the fact that each streaming detector may emit any number of event
//!   messages, it is not possible to tell whether all data from a frame has been
//!   received. It is **assumed** that all data corresponding to a frame will appear in the `events`
//!   stream within `expiry_offset_ms` of the *first* message for that frame arriving. Events that
//!   come in too late are dropped.

#[allow(clippy::all)]
#[rustfmt::skip]
#[allow(dead_code, unused, non_snake_case)]
#[path = "flatbuffers_generated/ev44_events_generated.rs"]
pub mod ev44_events_generated;

#[allow(clippy::all)]
#[rustfmt::skip]
#[allow(dead_code, unused, non_snake_case)]
#[path = "flatbuffers_generated/pu00_pulse_metadata_generated.rs"]
pub mod pu00_pulse_metadata_generated;

pub mod config;
pub mod deserialization;
pub mod frame;
pub mod kafka;
pub mod metrics;
pub mod queue;
