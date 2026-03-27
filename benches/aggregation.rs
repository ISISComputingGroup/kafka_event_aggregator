use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};
use kafka_event_aggregator::config::AggregatorConfig;
use kafka_event_aggregator::queue::FrameQueue;
use rand::prelude::*;
use rand::rngs::ChaCha8Rng;
use std::hint::black_box;
use std::iter;
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

fn make_fake_flatbuffers_encoded_events(
    rng: &mut ChaCha8Rng,
    reference_time: i64,
    num_messages: usize,
    events_per_message: usize,
) -> Vec<Vec<u8>> {
    (0..num_messages)
        .map(|_| {
            let mut pixel_ids = Vec::with_capacity(events_per_message);
            let mut tofs = Vec::with_capacity(events_per_message);

            for _ in 0..events_per_message {
                pixel_ids.push(rng.random_range(0..65536));
                tofs.push(rng.random_range(0..100_000_000));
            }

            let mut fbb = FlatBufferBuilder::new();
            let args = Event44MessageArgs {
                source_name: Some(fbb.create_string("source_name")),
                message_id: 0,
                reference_time: Some(fbb.create_vector(&[reference_time])),
                reference_time_index: Some(fbb.create_vector(&[0])),
                time_of_flight: Some(fbb.create_vector(&tofs)),
                pixel_id: Some(fbb.create_vector(&pixel_ids)),
            };

            let ev44 = Event44Message::create(&mut fbb, &args);
            finish_event_44_message_buffer(&mut fbb, ev44);
            fbb.finished_data().to_owned()
        })
        .chain(iter::once_with(|| {
            let mut fbb = FlatBufferBuilder::new();
            let args = Pu00MessageArgs {
                source_name: Some(fbb.create_string("source_name")),
                message_id: 0,
                reference_time,
                proton_charge: Some(1.2345),
                vetos: Some(0b11010011),
                period_number: Some(1),
            };
            let pu00 = Pu00Message::create(&mut fbb, &args);
            finish_pu_00_message_buffer(&mut fbb, pu00);
            fbb.finished_data().to_owned()
        }))
        .collect()
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
