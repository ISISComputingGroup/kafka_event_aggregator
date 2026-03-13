use flatbuffers::FlatBufferBuilder;
use futures::stream::StreamExt;
use kafka_event_aggregator::config::config_from_str;
use kafka_event_aggregator::kafka::{make_consumer, make_producer};
use kafka_event_aggregator::queue::FrameQueue;
use log::{error, info};
use rdkafka::Message;
use rdkafka::producer::BaseRecord;
use std::time::Duration;
use tokio::select;
use tokio::signal::ctrl_c;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::try_parse()?;

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
                        error!("Received event without payload; ignoring.");
                    }
                } else {
                    error!("Error reading from stream {:?}", msg);
                    break;
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
                        error!("Error sending message to kafka: {:?}", e);
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
