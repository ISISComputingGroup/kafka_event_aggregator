use flatbuffers::FlatBufferBuilder;
use futures::stream::StreamExt;
use kafka_event_aggregator::kafka::{make_consumer, make_producer};
use kafka_event_aggregator::queue::FrameQueue;
use log::{error, info};
use rdkafka::Message;
use rdkafka::producer::BaseRecord;
use std::time::Duration;
use tokio::select;
use tokio::signal::ctrl_c;

#[tokio::main]
async fn main() {
    env_logger::init();

    let bootstrap_servers = "itachi.isis.cclrc.ac.uk:9092";

    let input_topic_name = "NDW2922_rawEvents";
    let output_topic_name = "NDW2922_events";
    let auto_commit_interval_ms = 5000;

    let consumer = make_consumer(bootstrap_servers, input_topic_name, auto_commit_interval_ms);
    let mut stream = consumer.stream();

    let producer = make_producer(bootstrap_servers);

    let frame_queue_poll_interval_ms = 100;
    let mut frame_queue_poll_interval =
        tokio::time::interval(Duration::from_millis(frame_queue_poll_interval_ms));

    let mut fbb = FlatBufferBuilder::new();

    let expiry_offset_ms = 1000;
    let reference_time_tolerance_ns = 500;
    let next_message_id = 0;

    let mut frame_queue = FrameQueue::new(
        expiry_offset_ms,
        reference_time_tolerance_ns,
        next_message_id,
    );

    let max_events_per_message = 100_000;

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
                frame_queue.send_expired_frames(&mut fbb, max_events_per_message, |timestamp, msg| {
                    let result = producer.send(
                        BaseRecord::<[u8], [u8]>::to(output_topic_name)
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
}
