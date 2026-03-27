use clap::Parser;
use flatbuffers::FlatBufferBuilder;
use futures::stream::StreamExt;
use kafka_event_aggregator::config::config_from_str;
use kafka_event_aggregator::kafka::{get_most_recent_message_id, make_consumer, make_producer};
use kafka_event_aggregator::metrics::{OUTGOING_KAFKA_ERRORS, OUTGOING_MESSAGE_SIZE, QUEUE_FRAMES, initialize_metrics, INCOMING_MESSAGE_SIZE, INCOMING_MESSAGES_DROPPED, IncomingMessageDropReason};
use kafka_event_aggregator::queue::FrameQueue;
use log::{debug, error, info, warn};
use metrics::{counter, gauge};
use rdkafka::Message;
use rdkafka::producer::BaseRecord;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::signal::ctrl_c;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: String,

    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    env_logger::Builder::new()
        .filter_level(args.verbosity.into())
        .format_timestamp_micros()
        .init();

    let config = config_from_str(&std::fs::read_to_string(args.config)?)?;

    initialize_metrics(&config)?;

    let consumer = make_consumer(&config)?;
    let mut stream = consumer.stream();
    let producer = make_producer(&config)?;

    let mut frame_queue_poll_interval =
        tokio::time::interval(Duration::from_millis(config.frame_queue_poll_interval_ms()));

    let next_message_id = get_most_recent_message_id(&config)
        .map(|n| n + 1)
        .unwrap_or_else(|e| {
            warn!(
                "Cannot get last message ID from Kafka due to {}; setting next message ID to 0.",
                e
            );
            0
        });

    info!("Starting at message ID {}", next_message_id);

    let mut frame_queue = FrameQueue::new(&config, next_message_id);

    let mut fbb = FlatBufferBuilder::new();

    loop {
        select! {
            Some(msg) = stream.next() => {
                if let Ok(msg) = msg {
                    if let Some(payload) = msg.payload() {
                        let start = Instant::now();
                        counter!(INCOMING_MESSAGE_SIZE).increment(payload.len() as u64);
                        frame_queue.process_raw_message(payload);
                        debug!("Processing raw msg ({} bytes) took {} us", payload.len(), start.elapsed().as_micros());
                    } else {
                        warn!("Received event without payload; ignoring.");
                        counter!(INCOMING_MESSAGES_DROPPED, "reason" => IncomingMessageDropReason::NO_PAYLOAD).increment(1);
                    }
                } else {
                    error!("Error reading from stream {:?}", msg);
                }
            },
            _ = frame_queue_poll_interval.tick() => {
                let start = Instant::now();
                let len_start = frame_queue.len();
                frame_queue.send_expired_frames(&mut fbb, |timestamp, msg| {
                    let result = producer.send(
                        BaseRecord::<[u8], [u8]>::to(&config.output_topic)
                            .payload(msg)
                            .timestamp(timestamp)
                    );

                    if let Err((e, _)) = result {
                        warn!("Error sending message to kafka: {:?}", e);
                        counter!(OUTGOING_KAFKA_ERRORS).increment(1);
                    } else {
                        counter!(OUTGOING_MESSAGE_SIZE).increment(msg.len() as u64);
                    }
                });
                let len_end = frame_queue.len();
                if len_start != len_end {
                    debug!("Sending messages {} -> {} took {} us", len_start, len_end, start.elapsed().as_micros());
                }
                gauge!(QUEUE_FRAMES).set(len_end as f64);
            },
            _ = ctrl_c() => {
                info!("Shutting down after ctrl-c");
                break;
            }
        }
    }
    Ok(())
}
