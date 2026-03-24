use clap::Parser;
use kafka_event_aggregator::config::config_from_str;
use kafka_event_aggregator::kafka::make_producer;
use kafka_event_aggregator::metrics::{initialize_metrics};
use log::warn;
use rdkafka::producer::{BaseRecord, Producer};
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

    const NUM_PRODUCERS: usize = 1;
    let producers = (0..NUM_PRODUCERS).map(|_| make_producer(&config)).collect::<Result<Vec<_>, _>>()?;

    let mut bytes_sent = 0;
    let start_time = Instant::now();
    const MSG_LEN: usize = 100_000;

    for i in 0..100_000 {
        let result = producers[i%NUM_PRODUCERS].send(BaseRecord::<[u8], [u8]>::to(&config.output_topic).payload(&[0_u8; MSG_LEN]));

        bytes_sent += MSG_LEN;

        if let Err((e, _)) = result {
            warn!("Error sending message to kafka: {:?}", e);
        }
    }

    producers.iter().for_each(|p| {
        let _ = p.flush(Duration::from_secs(3600));
    });

    println!(
        "Wrote {} GB",
        bytes_sent as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "{} Mbit/s",
        8. * bytes_sent as f64 / (1024. * 1024. * start_time.elapsed().as_secs_f64())
    );

    Ok(())
}
