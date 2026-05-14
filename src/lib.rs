//! # `kafka_event_aggregator`
//!
//! This program _aggregates_ events from a `_rawEvents` Kafka topic, and emits
//! events onto an `_events` topic. It is responsible for zipping together events
//! and metadata messages emitted by different sources into "frames".
//!
//! ## Operation
//!
//! When data arrives on `config.input_topic` (usually `_rawEvents`), `kafka_event_aggregator` will:
//! - Check whether this data forms part of an existing frame. If not, a new Frame will be created,
//!   with a time-to-live `config.expiry_offset_ms` in the future.
//! - Append neutron events from the new data, or combine veto statuses from the new data, with the
//!   existing or newly-created frame
//!
//! When a frame of data's time-to-live expires, and the next `config.frame_queue_poll_interval`
//! timer interval expires, `kafka_event_aggregator` will:
//! - Sort the events in the frame by increasing time of flight
//! - Emit a `pu00` frame metadata message to the `config.output_topic` (usually `_events`) stream
//! - Emit zero or more `ev44` messages, each containing at most `config.max_events_per_message`
//!   neutron events per `ev44` message.
//!
//! ## `events` stream format
//!
//! Consumers of the `_events` stream may make the following assumptions:
//! - Each frame is made of one `pu00` message and any number of `ev44` messages.
//! - All messages in each frame will go to the same Kafka partition (the messages that make up
//!   a frame all have the same Kafka message key).
//! - Messages are produced to Kafka using an idempotent producer (enable.idempotence = true),
//!   ensuring that retries do not create duplicate messages. Kafka guarantees in-order delivery
//!   within each partition.
//! - Events will be emitted ordered in time-of-flight, both within a single `ev44` message and
//!   across multiple `ev44` messages within one frame, if `config.sort_events_by_tof = true`.
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
//!   stream within `config.expiry_offset_ms` of the *first* message for that frame arriving.
//!   Events that come in too late are dropped.
//!
//! ## Consuming from the `_events` stream
//!
//! The recommended approach to consume from the events stream, while accounting for frame
//! metadata (vetoes, period number, proton charge) is to keep a per-partition
//! **array** of the most recently seen frame metadata, keyed by Kafka partition of the message.
//! This metadata applies to all `ev44` event messages on that partition until the next `pu00`
//! arrives.
//!
//! In a live consumer:
//! - When a `pu00` (metadata) `msg` arrives: Update `array[msg.partition]` with the new metadata
//! - When an `ev44` (event data) `msg` arrives: Use `array[msg.partition]` as the metadata for
//!   those neutron events
//!
//! This ensures that each neutron event is associated with the correct metadata, even if the
//! metadata for frame `N+1` arrives between the metadata and the events for frame `N` (which it
//! may, iff those frames are on different partitions).
//!
//! This approach also works across multiple consumer processes: a consumer will receive all
//! messages from a single frame, but different frames may be processed by different consumers.
//!
//! ## Metrics
//!
//! Prometheus-compatible metrics are provided by a scrape endpoint, configured by
//! `config.metrics_bind_addr`. This endpoint can also be accessed by curl or a standard web
//! browser.
//!
//! ## Logging
//!
//! `kafka_event_aggregator` accepts command-line `-v` and `-q` flags to increase and decrease
//! logging verbosity respectively. The flags may be supplied multiple times.
//!
//! ## Benchmarks
//!
//! A benchmark of the full aggregation process is available in the `benchmarks/` folder, and
//! is run using `cargo bench`.
//!
//! ## Documentation
//!
//! `cargo doc --no-deps --open`.
//!
//! ## Configuration
//!
//! An example config file is provided in `config_example.toml`. This may be copied to `config.toml`
//! and adjusted to taste.
//!
//! The aggregator is then run with:
//! `kafka_event_aggregator --config path/to/config.toml`

pub mod config;
pub mod fake_events;
pub mod frame;
pub mod kafka;
pub mod metrics;
pub mod output_message;
pub mod queue;
