use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flatbuffers::FlatBufferBuilder;

use kafka_event_aggregator::config::AggregatorConfig;
use kafka_event_aggregator::fake_events::make_fake_flatbuffers_encoded_events;
use kafka_event_aggregator::queue::FrameQueue;
use rand::prelude::*;
use rand::rngs::ChaCha8Rng;
use std::hint::black_box;
use std::time::Duration;

fn make_config() -> AggregatorConfig {
    AggregatorConfig {
        max_events_per_message: Some(100_000),
        sort_events_by_tof: Some(true),
        expiry_offset_ms: Some(0),
        reference_time_tolerance_ns: Some(0),
        max_queued_frames: Some(50),
        ..Default::default()
    }
}

const BYTES_PER_EVENT: usize = 8;

fn benchmark_full_aggregation(c: &mut Criterion) {
    const INPUT_MESSAGES_PER_FRAME: usize = 100;
    const NUM_FRAMES: usize = 10;

    let config = make_config();

    let mut group = c.benchmark_group("full_aggregation");

    let mut rng = ChaCha8Rng::seed_from_u64(0);

    let mut fbb = FlatBufferBuilder::new();

    for events_per_input_msg in [10, 100, 1000, 10_000].into_iter() {
        group.throughput(Throughput::ElementsAndBytes {
            elements: (NUM_FRAMES * INPUT_MESSAGES_PER_FRAME * events_per_input_msg) as u64,
            bytes: (NUM_FRAMES * INPUT_MESSAGES_PER_FRAME * events_per_input_msg * BYTES_PER_EVENT)
                as u64,
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "{} events per input message",
                events_per_input_msg
            )),
            &events_per_input_msg,
            |b, &events_per_input_msg| {
                b.iter_batched_ref(
                    || {
                        let messages = (0..NUM_FRAMES)
                            .map(|t| {
                                make_fake_flatbuffers_encoded_events(
                                    &mut rng,
                                    t as i64 * 1_000_000_000,
                                    INPUT_MESSAGES_PER_FRAME,
                                    events_per_input_msg,
                                )
                            })
                            .flatten()
                            .collect::<Vec<_>>();

                        let frame_queue = FrameQueue::new(&config, 0);

                        (messages, frame_queue)
                    },
                    |(messages, frame_queue)| {
                        for msg in messages {
                            frame_queue.process_raw_message(msg);
                        }
                        frame_queue.send_expired_frames(&mut fbb, |timestamp, msg| {
                            // Use to_vec() to simulate the copy that librdkafka does
                            // (rdkafka uses RD_KAFKA_MSG_F_COPY)
                            black_box((timestamp, msg.to_vec()));
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(10));
    targets = benchmark_full_aggregation
}
criterion_main!(benches);
