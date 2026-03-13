use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flatbuffers::FlatBufferBuilder;
use kafka_event_aggregator::ev44_events_generated::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use kafka_event_aggregator::frame::{Event, Frame};
use kafka_event_aggregator::queue::FrameQueue;
use rand::prelude::*;
use rand::rngs::ChaCha8Rng;
use std::hint::black_box;

fn make_fake_events(num: usize) -> Vec<Event> {
    let mut rng = ChaCha8Rng::seed_from_u64(0);
    let mut events = Vec::with_capacity(num);
    for _ in 0..num {
        let tof = rng.random();
        let pixel = rng.random();
        events.push(Event::new(tof, pixel))
    }
    events
}

const BYTES_PER_EVENT: usize = 8;

fn benchmark_emit_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("emit_events");
    for events_per_frame in [
        1000,       // Approx 3 mbps at 50Hz
        100_000,    // Approx 300 mbps at 50Hz
        10_000_000, // Approx 30 gbps at 50Hz
    ]
    .into_iter()
    {
        group.throughput(Throughput::ElementsAndBytes {
            elements: events_per_frame as u64,
            bytes: (events_per_frame * BYTES_PER_EVENT) as u64,
        });
        group.bench_with_input(
            BenchmarkId::from_parameter(events_per_frame),
            &events_per_frame,
            |b, events_per_frame| {
                let mut fbb = FlatBufferBuilder::new();

                b.iter_batched_ref(
                    || {
                        let mut frame = Frame::new_with_reference_time(0, 0);
                        frame.append_events(make_fake_events(*events_per_frame).into_iter());
                        frame
                    },
                    |frame| {
                        frame.emit_messages(&mut fbb, &mut 0, 100_000, |timestamp, msg| {
                            black_box((timestamp, msg));
                        });
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

fn benchmark_process_raw_messages(c: &mut Criterion) {
    const NUM_FRAMES: i64 = 100;
    const MESSAGES_PER_FRAME: usize = 100;
    const EVENTS_PER_MESSAGE: usize = 500;

    let messages = (0..NUM_FRAMES) // 1000 different reference times
        .flat_map(|n| [n; MESSAGES_PER_FRAME]) // each reference time appears 100 times
        .map(|t| {
            let mut fbb = FlatBufferBuilder::new();
            let args = Event44MessageArgs {
                source_name: Some(fbb.create_string("source_name")),
                message_id: 0,
                reference_time: Some(fbb.create_vector(&[t])),
                reference_time_index: Some(fbb.create_vector(&[0])),
                time_of_flight: Some(fbb.create_vector(&[0; EVENTS_PER_MESSAGE])),
                pixel_id: Some(fbb.create_vector(&[0; EVENTS_PER_MESSAGE])),
            };

            let ev44 = Event44Message::create(&mut fbb, &args);
            finish_event_44_message_buffer(&mut fbb, ev44);
            fbb.finished_data().to_owned()
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("process_raw_message");
    group.throughput(Throughput::Bytes(
        messages.iter().map(|m| m.len() as u64).sum(),
    ));

    group.bench_function("process_raw_message", |b| {
        b.iter(|| {
            let mut queue = FrameQueue::new(0, 0, 0);

            messages
                .iter()
                .for_each(|msg| queue.process_raw_message(msg));

            assert_eq!(queue.len(), NUM_FRAMES as usize);
        })
    });
}

criterion_group!(
    benches,
    benchmark_emit_events,
    benchmark_process_raw_messages
);
criterion_main!(benches);
