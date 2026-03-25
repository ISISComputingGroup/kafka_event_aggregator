use clap::Parser;
use kafka_event_aggregator::config::config_from_str;
use kafka_event_aggregator::kafka::{get_most_recent_message_id, make_consumer, make_producer};
use kafka_event_aggregator::metrics::{
    OUTGOING_KAFKA_ERRORS, OUTGOING_MESSAGE_SIZE, QUEUE_FRAMES, initialize_metrics,
};
use kafka_event_aggregator::queue::FrameQueue;
use log::{error, info, warn};
use metrics::{counter, gauge};
use rdkafka::Message;
use rdkafka::producer::BaseRecord;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: String,

    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    env_logger::Builder::new()
        .filter_level(args.verbosity.into())
        .format_timestamp_micros()
        .init();

    let config = config_from_str(&std::fs::read_to_string(args.config)?)?;

    initialize_metrics(&config)?;

    let consumer = make_consumer(&config)?;
    let producer = make_producer(&config)?;

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

    let mut last_emit = Instant::now();

    loop {
        while let Some(msg) = consumer.poll(Duration::from_millis(1)) {
            if let Ok(msg) = msg {
                if let Some(payload) = msg.payload() {
                    // frame_queue.process_raw_message(payload);

                    let result = producer.send(
                        BaseRecord::<[u8], [u8]>::to(&config.output_topic)
                            .payload(payload),
                    );

                    if let Err((e, _)) = result {
                        warn!("Error sending message to kafka: {:?}", e);
                    }
                } else {
                    warn!("Received event without payload; ignoring.");
                }
            } else {
                error!("Error reading from stream {:?}", msg);
            }

            if last_emit.elapsed() > Duration::from_millis(100) {
                break;
            }
        }

        if last_emit.elapsed() > Duration::from_millis(100) {
            // frame_queue
            //     .messages_for_expired_frames()
            //     .into_iter()
            //     .for_each(|msg| {
            //         let result = producer.send(
            //             BaseRecord::<[u8], [u8]>::to(&config.output_topic)
            //                 .payload(msg.content())
            //                 .timestamp(msg.timestamp()),
            //         );
            //         counter!(OUTGOING_MESSAGE_SIZE).increment(msg.len() as u64);
            //
            //         if let Err((e, _)) = result {
            //             warn!("Error sending message to kafka: {:?}", e);
            //             counter!(OUTGOING_KAFKA_ERRORS).increment(1);
            //         }
            //     });
            last_emit = Instant::now();
        }
    }
}
