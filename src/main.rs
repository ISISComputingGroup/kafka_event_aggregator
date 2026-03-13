use anyhow::bail;
use clap::Parser;
use flatbuffers::FlatBufferBuilder;
use futures::stream::StreamExt;
use kafka_event_aggregator::config::config_from_str;
use kafka_event_aggregator::kafka::{make_consumer, make_producer};
use kafka_event_aggregator::queue::FrameQueue;
use log::{info, warn};
use rdkafka::Message;
use rdkafka::producer::BaseRecord;
use std::time::Duration;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    env_logger::Builder::new()
        .filter_level(args.verbosity.into())
        .format_timestamp_micros()
        .init();

    let config = config_from_str(&std::fs::read_to_string(args.config)?)?;

    let consumer = make_consumer(&config)?;
    let mut stream = consumer.stream();
    let producer = make_producer(&config)?;

    let mut frame_queue_poll_interval =
        tokio::time::interval(Duration::from_millis(config.frame_queue_poll_interval_ms));

    let next_message_id = 0;

    let mut frame_queue = FrameQueue::new(&config, next_message_id);

    let mut fbb = FlatBufferBuilder::new();

    loop {
        select! {
            Some(msg) = stream.next() => {
                if let Ok(msg) = msg {
                    if let Some(payload) = msg.payload() {
                        frame_queue.process_raw_message(payload);
                    } else {
                        warn!("Received event without payload; ignoring.");
                    }
                } else {
                    bail!("Error reading from stream {:?}", msg);
                }
            },
            _ = frame_queue_poll_interval.tick() => {
                frame_queue.send_expired_frames(&mut fbb, |timestamp, msg| {
                    let result = producer.send(
                        BaseRecord::<[u8], [u8]>::to(&config.output_topic)
                            .payload(msg)
                            .timestamp(timestamp)
                    );

                    if let Err((e, _)) = result {
                        warn!("Error sending message to kafka: {:?}", e);
                    }
                });
            },
            _ = ctrl_c() => {
                info!("Shutting down after ctrl-c");
                break;
            }
        }
    }
    Ok(())
}
