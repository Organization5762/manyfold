use std::env;
use std::hint::black_box;
use std::time::Instant;

use manyfold::architecture::InMemoryPubSubCore;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Workload {
    Publish,
    Poll,
    WildcardFanout,
    Replay,
    Latest,
    Retention,
    All,
}

struct Config {
    workload: Workload,
    iterations: usize,
    retained_messages: usize,
    subscribers: usize,
    batch_size: usize,
    payload_bytes: usize,
    topic_count: usize,
    max_average_operation_us: Option<f64>,
}

struct BenchmarkResult {
    workload: &'static str,
    iterations: usize,
    retained_messages: usize,
    subscribers: usize,
    batch_size: usize,
    payload_bytes: usize,
    topic_count: usize,
    operation_count: usize,
    elapsed_seconds: f64,
    average_operation_us: f64,
    final_message_count: usize,
    final_subscriber_count: usize,
    checksum: u64,
}

fn main() {
    let config = Config::parse(env::args().skip(1).collect());
    let results = match config.workload {
        Workload::Publish => vec![run_publish(&config)],
        Workload::Poll => vec![run_poll(&config)],
        Workload::WildcardFanout => vec![run_wildcard_fanout(&config)],
        Workload::Replay => vec![run_replay(&config)],
        Workload::Latest => vec![run_latest(&config)],
        Workload::Retention => vec![run_retention(&config)],
        Workload::All => vec![
            run_publish(&config),
            run_poll(&config),
            run_wildcard_fanout(&config),
            run_replay(&config),
            run_latest(&config),
            run_retention(&config),
        ],
    };
    if let Some(max_average_operation_us) = config.max_average_operation_us {
        for result in &results {
            if result.average_operation_us > max_average_operation_us {
                panic!(
                    "{} average operation latency {:.6} us exceeds {:.6} us",
                    result.workload, result.average_operation_us, max_average_operation_us
                );
            }
        }
    }
    print_results(&results);
}

fn run_publish(config: &Config) -> BenchmarkResult {
    let mut pubsub = InMemoryPubSubCore::new(config.retained_messages).expect("valid pubsub");
    let payload = payload(config.payload_bytes);
    let start = Instant::now();
    let mut checksum = 0_u64;
    for index in 0..config.iterations {
        let topic = topic(index, config.topic_count);
        let delivery = pubsub.publish(topic, payload.clone()).expect("publish");
        checksum ^= delivery.offset;
    }
    result(
        "publish",
        config,
        &pubsub,
        config.iterations,
        start,
        checksum,
    )
}

fn run_poll(config: &Config) -> BenchmarkResult {
    let mut pubsub = InMemoryPubSubCore::new(config.retained_messages).expect("valid pubsub");
    pubsub
        .subscribe("topic-0".to_string(), Some("poller".to_string()), true)
        .expect("subscribe");
    let payload = payload(config.payload_bytes);
    let start = Instant::now();
    let mut checksum = 0_u64;
    for index in 0..config.iterations {
        pubsub
            .publish("topic-0".to_string(), payload.clone())
            .expect("publish");
        if (index + 1) % config.batch_size == 0 {
            let messages = pubsub
                .poll("poller", Some(config.batch_size))
                .expect("poll");
            checksum ^= messages.len() as u64;
            checksum ^= messages.last().map_or(0, |message| message.offset);
        }
    }
    result("poll", config, &pubsub, config.iterations, start, checksum)
}

fn run_wildcard_fanout(config: &Config) -> BenchmarkResult {
    let mut pubsub = InMemoryPubSubCore::new(config.retained_messages).expect("valid pubsub");
    for index in 0..config.subscribers {
        pubsub
            .subscribe("*".to_string(), Some(format!("wildcard-{index}")), false)
            .expect("subscribe");
    }
    let payload = payload(config.payload_bytes);
    let start = Instant::now();
    let mut checksum = 0_u64;
    for index in 0..config.iterations {
        let delivery = pubsub
            .publish(topic(index, config.topic_count), payload.clone())
            .expect("publish");
        checksum ^= delivery.delivered_to.len() as u64;
        checksum ^= delivery.offset;
    }
    result(
        "wildcard_fanout",
        config,
        &pubsub,
        config.iterations,
        start,
        checksum,
    )
}

fn run_replay(config: &Config) -> BenchmarkResult {
    let mut pubsub = InMemoryPubSubCore::new(config.retained_messages).expect("valid pubsub");
    let payload = payload(config.payload_bytes);
    for index in 0..config.iterations {
        pubsub
            .publish(topic(index, config.topic_count), payload.clone())
            .expect("publish");
    }
    let start = Instant::now();
    let mut checksum = 0_u64;
    for index in 0..config.subscribers {
        let subscription = format!("replay-{index}");
        pubsub
            .subscribe("*".to_string(), Some(subscription.clone()), true)
            .expect("subscribe");
        let messages = pubsub.poll(&subscription, None).expect("poll");
        checksum ^= messages.len() as u64;
        checksum ^= messages.last().map_or(0, |message| message.offset);
    }
    result(
        "replay",
        config,
        &pubsub,
        config.subscribers,
        start,
        checksum,
    )
}

fn run_latest(config: &Config) -> BenchmarkResult {
    let mut pubsub = InMemoryPubSubCore::new(config.retained_messages).expect("valid pubsub");
    let payload = payload(config.payload_bytes);
    for index in 0..config.retained_messages {
        pubsub
            .publish(topic(index, config.topic_count), payload.clone())
            .expect("publish");
    }
    let start = Instant::now();
    let mut checksum = 0_u64;
    for index in 0..config.iterations {
        let latest = pubsub
            .latest(Some(&topic(index, config.topic_count)))
            .expect("latest")
            .expect("topic has latest");
        checksum ^= latest.offset;
    }
    result(
        "latest",
        config,
        &pubsub,
        config.iterations,
        start,
        checksum,
    )
}

fn run_retention(config: &Config) -> BenchmarkResult {
    let mut pubsub = InMemoryPubSubCore::new(config.retained_messages).expect("valid pubsub");
    let payload = payload(config.payload_bytes);
    let start = Instant::now();
    let mut checksum = 0_u64;
    for index in 0..config.iterations {
        let delivery = pubsub
            .publish(topic(index, config.topic_count), payload.clone())
            .expect("publish");
        checksum ^= delivery.offset;
        checksum ^= pubsub.message_count() as u64;
    }
    result(
        "retention",
        config,
        &pubsub,
        config.iterations,
        start,
        checksum,
    )
}

fn result(
    workload: &'static str,
    config: &Config,
    pubsub: &InMemoryPubSubCore,
    operation_count: usize,
    start: Instant,
    checksum: u64,
) -> BenchmarkResult {
    let elapsed_seconds = start.elapsed().as_secs_f64();
    black_box(checksum);
    BenchmarkResult {
        workload,
        iterations: config.iterations,
        retained_messages: config.retained_messages,
        subscribers: config.subscribers,
        batch_size: config.batch_size,
        payload_bytes: config.payload_bytes,
        topic_count: config.topic_count,
        operation_count,
        elapsed_seconds,
        average_operation_us: elapsed_seconds * 1_000_000.0 / operation_count as f64,
        final_message_count: pubsub.message_count(),
        final_subscriber_count: pubsub.subscriber_count(),
        checksum,
    }
}

fn topic(index: usize, topic_count: usize) -> String {
    format!("topic-{}", index % topic_count)
}

fn payload(payload_bytes: usize) -> Vec<u8> {
    vec![b'x'; payload_bytes]
}

fn print_results(results: &[BenchmarkResult]) {
    println!("[");
    for (index, result) in results.iter().enumerate() {
        let comma = if index + 1 == results.len() { "" } else { "," };
        println!(
            "  {{\n    \"workload\": \"{}\",\n    \"iterations\": {},\n    \
             \"retained_messages\": {},\n    \"subscribers\": {},\n    \
             \"batch_size\": {},\n    \"payload_bytes\": {},\n    \
             \"topic_count\": {},\n    \"operation_count\": {},\n    \
             \"elapsed_seconds\": {:.9},\n    \"average_operation_us\": {:.6},\n    \
             \"final_message_count\": {},\n    \"final_subscriber_count\": {},\n    \
             \"checksum\": {}\n  }}{}",
            result.workload,
            result.iterations,
            result.retained_messages,
            result.subscribers,
            result.batch_size,
            result.payload_bytes,
            result.topic_count,
            result.operation_count,
            result.elapsed_seconds,
            result.average_operation_us,
            result.final_message_count,
            result.final_subscriber_count,
            result.checksum,
            comma,
        );
    }
    println!("]");
}

impl Config {
    fn parse(args: Vec<String>) -> Self {
        let mut config = Self {
            workload: Workload::All,
            iterations: 100_000,
            retained_messages: 1024,
            subscribers: 16,
            batch_size: 32,
            payload_bytes: 64,
            topic_count: 8,
            max_average_operation_us: None,
        };
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--workload" => {
                    index += 1;
                    config.workload = parse_workload(args.get(index));
                }
                "--iterations" => {
                    index += 1;
                    config.iterations = parse_positive_usize(args.get(index), "--iterations");
                }
                "--retained-messages" => {
                    index += 1;
                    config.retained_messages =
                        parse_positive_usize(args.get(index), "--retained-messages");
                }
                "--subscribers" => {
                    index += 1;
                    config.subscribers = parse_positive_usize(args.get(index), "--subscribers");
                }
                "--batch-size" => {
                    index += 1;
                    config.batch_size = parse_positive_usize(args.get(index), "--batch-size");
                }
                "--payload-bytes" => {
                    index += 1;
                    config.payload_bytes = parse_positive_usize(args.get(index), "--payload-bytes");
                }
                "--topic-count" => {
                    index += 1;
                    config.topic_count = parse_positive_usize(args.get(index), "--topic-count");
                }
                "--max-average-operation-us" => {
                    index += 1;
                    config.max_average_operation_us = Some(parse_positive_f64(
                        args.get(index),
                        "--max-average-operation-us",
                    ));
                }
                "--help" | "-h" => print_usage_and_exit(),
                other => panic!("unknown argument: {other}"),
            }
            index += 1;
        }
        config
    }
}

fn parse_workload(value: Option<&String>) -> Workload {
    match value.map(String::as_str) {
        Some("publish") => Workload::Publish,
        Some("poll") => Workload::Poll,
        Some("wildcard_fanout") => Workload::WildcardFanout,
        Some("replay") => Workload::Replay,
        Some("latest") => Workload::Latest,
        Some("retention") => Workload::Retention,
        Some("all") => Workload::All,
        Some(other) => panic!("unknown --workload: {other}"),
        None => panic!("--workload requires a value"),
    }
}

fn parse_positive_usize(value: Option<&String>, name: &str) -> usize {
    let parsed = value
        .unwrap_or_else(|| panic!("{name} requires a value"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{name} must be an integer"));
    if parsed == 0 {
        panic!("{name} must be positive");
    }
    parsed
}

fn parse_positive_f64(value: Option<&String>, name: &str) -> f64 {
    let parsed = value
        .unwrap_or_else(|| panic!("{name} requires a value"))
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("{name} must be a number"));
    if parsed <= 0.0 {
        panic!("{name} must be positive");
    }
    parsed
}

fn print_usage_and_exit() -> ! {
    println!(
        "architecture_pubsub_benchmark [--workload publish|poll|wildcard_fanout|replay|latest|retention|all] \
         [--iterations N] [--retained-messages N] [--subscribers N] \
         [--batch-size N] [--payload-bytes N] [--topic-count N] \
         [--max-average-operation-us N]"
    );
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_benchmark_reports_requested_shape() {
        let result = run_publish(&Config {
            workload: Workload::Publish,
            iterations: 4,
            retained_messages: 8,
            subscribers: 2,
            batch_size: 2,
            payload_bytes: 4,
            topic_count: 2,
            max_average_operation_us: None,
        });

        assert_eq!(result.workload, "publish");
        assert_eq!(result.operation_count, 4);
        assert_eq!(result.final_message_count, 4);
        assert!(result.average_operation_us >= 0.0);
    }

    #[test]
    fn parser_rejects_zero_iterations() {
        let result = std::panic::catch_unwind(|| {
            Config::parse(vec!["--iterations".to_string(), "0".to_string()]);
        });

        assert!(result.is_err());
    }
}
