use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use manyfold::core::{
    GraphCore, Layer, NamespaceRefCore, Plane, ProducerKind, ProducerRefCore, RetentionPolicyCore,
    RouteRefCore, SchemaRefCore, Variant,
};

#[derive(Clone, Copy)]
enum LineageRetentionMode {
    None,
    Retained,
}

impl LineageRetentionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Retained => "retained",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LineageStoreMode {
    Retained,
    Noop,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CorrelationStoreMode {
    Retained,
    Noop,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MetadataMode {
    None,
    Static,
    Unique,
}

#[derive(Clone, Copy)]
struct Config {
    iterations: u64,
    history_limit: usize,
    sample_every: u64,
    rss_plateau_kib: Option<u64>,
    rss_tail_plateau_kib: Option<u64>,
    rss_tail_min_samples: usize,
    live_plateau_bytes: Option<usize>,
    peak_plateau_bytes: Option<usize>,
    projected_live_growth_bytes: Option<usize>,
    projected_live_segment_growth_bytes: Option<usize>,
    projected_peak_growth_bytes: Option<usize>,
    projected_peak_segment_growth_bytes: Option<usize>,
    projected_rss_growth_kib: Option<u64>,
    projected_rss_segment_growth_kib: Option<u64>,
    max_elapsed_seconds: Option<f64>,
    max_cpu_seconds: Option<f64>,
    max_average_event_us: Option<f64>,
    max_interval_event_us: Option<f64>,
    max_disk_input_blocks: Option<u64>,
    max_disk_output_blocks: Option<u64>,
    project_events: u64,
    warmup_samples: usize,
    check_invariants_every: u64,
    seed_seq_source: Option<u64>,
    lineage_retention: LineageRetentionMode,
    lineage_store: LineageStoreMode,
    correlation_store: CorrelationStoreMode,
    metadata_mode: MetadataMode,
    payload_bytes: usize,
    materialize_state: bool,
}

struct Sample {
    step: u64,
    history: usize,
    payloads: usize,
    lineage: usize,
    lineage_values: usize,
    trace_index: usize,
    correlation_index: usize,
    live_allocated_bytes: usize,
    peak_allocated_bytes: usize,
    current_rss_kib: u64,
    elapsed_seconds: f64,
    cpu_seconds: f64,
    average_event_us: f64,
    interval_event_us: f64,
    input_blocks: u64,
    output_blocks: u64,
}

#[derive(Clone, Copy)]
struct UsageSnapshot {
    cpu_seconds: f64,
    input_blocks: u64,
    output_blocks: u64,
}

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

static LIVE_ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add_live_allocated_bytes(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_ALLOCATED_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            let old_size = layout.size();
            if new_size >= old_size {
                add_live_allocated_bytes(new_size - old_size);
            } else {
                LIVE_ALLOCATED_BYTES.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        }
        pointer
    }
}

fn add_live_allocated_bytes(size: usize) {
    let live = LIVE_ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    let mut peak = PEAK_ALLOCATED_BYTES.load(Ordering::Relaxed);
    while live > peak {
        match PEAK_ALLOCATED_BYTES.compare_exchange_weak(
            peak,
            live,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(current) => peak = current,
        }
    }
}

fn main() {
    let config = parse_args();
    let samples = run_stress(config);
    if let Some(plateau_bytes) = config.live_plateau_bytes {
        check_live_bytes_plateau(&samples, config.warmup_samples, plateau_bytes);
    }
    if let Some(plateau_bytes) = config.peak_plateau_bytes {
        check_peak_bytes_plateau(&samples, config.warmup_samples, plateau_bytes);
    }
    if let Some(projected_bytes) = config.projected_live_growth_bytes {
        check_projected_live_bytes_growth(
            &samples,
            config.warmup_samples,
            config.project_events,
            projected_bytes,
        );
    }
    if let Some(projected_bytes) = config.projected_peak_growth_bytes {
        check_projected_peak_bytes_growth(
            &samples,
            config.warmup_samples,
            config.project_events,
            projected_bytes,
        );
    }
    if let Some(projected_bytes) = config.projected_peak_segment_growth_bytes {
        check_projected_peak_bytes_segment_growth(
            &samples,
            config.warmup_samples,
            config.project_events,
            projected_bytes,
        );
    }
    if let Some(projected_bytes) = config.projected_live_segment_growth_bytes {
        check_projected_live_bytes_segment_growth(
            &samples,
            config.warmup_samples,
            config.project_events,
            projected_bytes,
        );
    }
    if let Some(plateau_kib) = config.rss_plateau_kib {
        check_rss_plateau(&samples, config.warmup_samples, plateau_kib);
    }
    if let Some(plateau_kib) = config.rss_tail_plateau_kib {
        check_rss_tail_plateau(&samples, plateau_kib, config.rss_tail_min_samples);
    }
    if let Some(projected_kib) = config.projected_rss_growth_kib {
        check_projected_rss_growth(
            &samples,
            config.warmup_samples,
            config.project_events,
            projected_kib,
        );
    }
    if let Some(projected_kib) = config.projected_rss_segment_growth_kib {
        check_projected_rss_segment_growth(
            &samples,
            config.warmup_samples,
            config.project_events,
            projected_kib,
        );
    }
    if let Some(max_elapsed_seconds) = config.max_elapsed_seconds {
        check_elapsed_seconds(&samples, max_elapsed_seconds);
    }
    if let Some(max_cpu_seconds) = config.max_cpu_seconds {
        check_cpu_seconds(&samples, max_cpu_seconds);
    }
    if let Some(max_average_event_us) = config.max_average_event_us {
        check_average_event_latency(&samples, max_average_event_us);
    }
    if let Some(max_interval_event_us) = config.max_interval_event_us {
        check_interval_event_latency(&samples, max_interval_event_us);
    }
    check_block_io(
        &samples,
        config.max_disk_input_blocks,
        config.max_disk_output_blocks,
    );
}

fn run_stress(config: Config) -> Vec<Sample> {
    let route = stress_route();
    let state_route = stress_state_route();
    let mut graph = GraphCore::default();
    graph.configure_retention(
        &route,
        RetentionPolicyCore {
            latest_replay_policy: "bounded_history".to_string(),
            durability_class: "memory".to_string(),
            replay_window: format!("last_{}", config.history_limit),
            payload_retention_policy: "separate_store".to_string(),
            history_limit: Some(config.history_limit),
            lineage_retention_policy: config.lineage_retention.as_str().to_string(),
        },
    );
    let mut measured_routes = vec![route.clone()];
    if config.materialize_state {
        graph.configure_retention(
            &state_route,
            RetentionPolicyCore {
                latest_replay_policy: "bounded_history".to_string(),
                durability_class: "memory".to_string(),
                replay_window: format!("last_{}", config.history_limit),
                payload_retention_policy: "separate_store".to_string(),
                history_limit: Some(config.history_limit),
                lineage_retention_policy: config.lineage_retention.as_str().to_string(),
            },
        );
        graph.register_materialize_bytes(&route, &state_route);
        measured_routes.push(state_route.clone());
    }
    let producer = ProducerRefCore {
        producer_id: "heart".to_string(),
        kind: ProducerKind::Application,
    };
    let payload = vec![b'f'; config.payload_bytes];
    if let Some(seed_seq_source) = config.seed_seq_source {
        seed_sequence(&mut graph, &route, &producer, &payload, seed_seq_source);
    }
    let start = Instant::now();
    let baseline_usage = current_usage();
    let sample_capacity = (config.iterations / config.sample_every + 3) as usize;
    let mut samples = Vec::with_capacity(sample_capacity);
    let mut last_sample_step = 0;
    let mut last_sample_elapsed_seconds = 0.0;

    for step in 1..=config.iterations {
        #[cfg(feature = "coz-profiler")]
        coz::begin!("memory_retention_event");
        if config.materialize_state {
            graph.write_single_if_unrouted_and_materializer_drop(
                &route,
                &state_route,
                payload.clone(),
                producer.clone(),
                None,
            );
        } else {
            graph.write_single_if_unrouted_drop(&route, payload.clone(), producer.clone(), None);
        }
        #[cfg(feature = "coz-profiler")]
        {
            coz::end!("memory_retention_event");
            coz::progress!("memory_retention_event_processed");
        }
        if config.check_invariants_every != 0 && step % config.check_invariants_every == 0 {
            assert_retention_invariants(&graph, step);
        }
        if step == config.history_limit as u64
            || step % config.sample_every == 0
            || step == config.iterations
        {
            let sample = sample(
                &graph,
                &measured_routes,
                step,
                start.elapsed().as_secs_f64(),
                last_sample_step,
                last_sample_elapsed_seconds,
                baseline_usage,
            );
            print_sample(&sample);
            assert_retained_counts(&sample, config.history_limit * measured_routes.len(), 0, 0);
            assert_retention_invariants(&graph, step);
            last_sample_step = sample.step;
            last_sample_elapsed_seconds = sample.elapsed_seconds;
            samples.push(sample);
        }
    }

    samples
}

fn seed_sequence(
    graph: &mut GraphCore,
    route: &RouteRefCore,
    producer: &ProducerRefCore,
    payload: &[u8],
    seq_source: u64,
) {
    graph.write(route, payload.to_vec(), producer.clone(), None);
    if let Some(latest) = graph.latest.get_mut(route) {
        latest.seq_source = seq_source;
    }
    if let Some(history) = graph.history.get_mut(route) {
        if let Some(latest) = history.back_mut() {
            latest.seq_source = seq_source;
        }
    }
    if let Some(audit) = graph.route_audit.get_mut(route) {
        if let Some(latest) = audit.last_mut() {
            latest.seq_source = Some(seq_source);
        }
    }
}

fn assert_retention_invariants(graph: &GraphCore, step: u64) {
    let violations = graph.retention_violations();
    assert!(
        violations.is_empty(),
        "retention invariant failed at step {step}: {}",
        violations.join("; ")
    );
}

fn parse_args() -> Config {
    let mut config = Config {
        iterations: 1_000_000,
        history_limit: 8,
        sample_every: 100_000,
        rss_plateau_kib: None,
        rss_tail_plateau_kib: None,
        rss_tail_min_samples: 1,
        live_plateau_bytes: None,
        peak_plateau_bytes: None,
        projected_live_growth_bytes: None,
        projected_live_segment_growth_bytes: None,
        projected_peak_growth_bytes: None,
        projected_peak_segment_growth_bytes: None,
        projected_rss_growth_kib: None,
        projected_rss_segment_growth_kib: None,
        max_elapsed_seconds: None,
        max_cpu_seconds: None,
        max_average_event_us: None,
        max_interval_event_us: None,
        max_disk_input_blocks: None,
        max_disk_output_blocks: None,
        project_events: 1_000_000_000,
        warmup_samples: 2,
        check_invariants_every: 0,
        seed_seq_source: None,
        lineage_retention: LineageRetentionMode::None,
        lineage_store: LineageStoreMode::Retained,
        correlation_store: CorrelationStoreMode::Noop,
        metadata_mode: MetadataMode::Static,
        payload_bytes: b"frame".len(),
        materialize_state: false,
    };
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iterations" => {
                config.iterations = parse_next(&mut args, "--iterations");
            }
            "--history-limit" => {
                config.history_limit = parse_next(&mut args, "--history-limit");
            }
            "--sample-every" => {
                config.sample_every = parse_next(&mut args, "--sample-every");
            }
            "--rss-plateau-kib" => {
                config.rss_plateau_kib = Some(parse_next(&mut args, "--rss-plateau-kib"));
            }
            "--rss-tail-plateau-kib" => {
                config.rss_tail_plateau_kib = Some(parse_next(&mut args, "--rss-tail-plateau-kib"));
            }
            "--rss-tail-min-samples" => {
                config.rss_tail_min_samples = parse_next(&mut args, "--rss-tail-min-samples");
            }
            "--live-plateau-bytes" => {
                config.live_plateau_bytes = Some(parse_next(&mut args, "--live-plateau-bytes"));
            }
            "--peak-plateau-bytes" => {
                config.peak_plateau_bytes = Some(parse_next(&mut args, "--peak-plateau-bytes"));
            }
            "--projected-live-growth-bytes" => {
                config.projected_live_growth_bytes =
                    Some(parse_next(&mut args, "--projected-live-growth-bytes"));
            }
            "--projected-live-segment-growth-bytes" => {
                config.projected_live_segment_growth_bytes = Some(parse_next(
                    &mut args,
                    "--projected-live-segment-growth-bytes",
                ));
            }
            "--projected-peak-growth-bytes" => {
                config.projected_peak_growth_bytes =
                    Some(parse_next(&mut args, "--projected-peak-growth-bytes"));
            }
            "--projected-peak-segment-growth-bytes" => {
                config.projected_peak_segment_growth_bytes = Some(parse_next(
                    &mut args,
                    "--projected-peak-segment-growth-bytes",
                ));
            }
            "--projected-rss-growth-kib" => {
                config.projected_rss_growth_kib =
                    Some(parse_next(&mut args, "--projected-rss-growth-kib"));
            }
            "--projected-rss-segment-growth-kib" => {
                config.projected_rss_segment_growth_kib =
                    Some(parse_next(&mut args, "--projected-rss-segment-growth-kib"));
            }
            "--max-elapsed-seconds" => {
                config.max_elapsed_seconds = Some(parse_next(&mut args, "--max-elapsed-seconds"));
            }
            "--max-cpu-seconds" => {
                config.max_cpu_seconds = Some(parse_next(&mut args, "--max-cpu-seconds"));
            }
            "--max-average-event-us" => {
                config.max_average_event_us = Some(parse_next(&mut args, "--max-average-event-us"));
            }
            "--max-interval-event-us" => {
                config.max_interval_event_us =
                    Some(parse_next(&mut args, "--max-interval-event-us"));
            }
            "--max-disk-input-blocks" => {
                config.max_disk_input_blocks =
                    Some(parse_next(&mut args, "--max-disk-input-blocks"));
            }
            "--max-disk-output-blocks" => {
                config.max_disk_output_blocks =
                    Some(parse_next(&mut args, "--max-disk-output-blocks"));
            }
            "--project-events" => {
                config.project_events = parse_next(&mut args, "--project-events");
            }
            "--warmup-samples" => {
                config.warmup_samples = parse_next(&mut args, "--warmup-samples");
            }
            "--check-invariants-every" => {
                config.check_invariants_every = parse_next(&mut args, "--check-invariants-every");
            }
            "--seed-seq-source" => {
                config.seed_seq_source = Some(parse_next(&mut args, "--seed-seq-source"));
            }
            "--lineage-retention" => {
                let value: String = parse_next(&mut args, "--lineage-retention");
                config.lineage_retention = match value.as_str() {
                    "none" => LineageRetentionMode::None,
                    "retained" => LineageRetentionMode::Retained,
                    _ => panic!("--lineage-retention must be none or retained"),
                };
            }
            "--lineage-store" => {
                let value: String = parse_next(&mut args, "--lineage-store");
                config.lineage_store = match value.as_str() {
                    "retained" => LineageStoreMode::Retained,
                    "noop" => LineageStoreMode::Noop,
                    _ => panic!("--lineage-store must be retained or noop"),
                };
            }
            "--correlation-store" => {
                let value: String = parse_next(&mut args, "--correlation-store");
                config.correlation_store = match value.as_str() {
                    "retained" => CorrelationStoreMode::Retained,
                    "noop" => CorrelationStoreMode::Noop,
                    _ => panic!("--correlation-store must be retained or noop"),
                };
            }
            "--metadata-mode" => {
                let value: String = parse_next(&mut args, "--metadata-mode");
                config.metadata_mode = match value.as_str() {
                    "none" => MetadataMode::None,
                    "static" => MetadataMode::Static,
                    "unique" => MetadataMode::Unique,
                    _ => panic!("--metadata-mode must be none, static, or unique"),
                };
            }
            "--payload-bytes" => {
                config.payload_bytes = parse_next(&mut args, "--payload-bytes");
            }
            "--materialize-state" => {
                config.materialize_state = true;
            }
            "--unique-lineage-ids" => {
                config.metadata_mode = MetadataMode::Unique;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                panic!("unsupported argument {arg}");
            }
        }
    }
    if config.history_limit == 0 {
        panic!("--history-limit must be positive");
    }
    if config.sample_every == 0 {
        panic!("--sample-every must be positive");
    }
    if config.project_events == 0 {
        panic!("--project-events must be positive");
    }
    if config.rss_tail_min_samples == 0 {
        panic!("--rss-tail-min-samples must be positive");
    }
    if config.payload_bytes == 0 {
        panic!("--payload-bytes must be positive");
    }
    config
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, name: &str) -> T
where
    T: std::str::FromStr,
{
    let value = args
        .next()
        .unwrap_or_else(|| panic!("{name} requires a value"));
    value
        .parse()
        .unwrap_or_else(|_| panic!("{name} has invalid value {value:?}"))
}

fn print_help() {
    println!(
        "memory_retention_benchmark [--iterations N] [--history-limit N] [--sample-every N] \
         [--live-plateau-bytes N] [--peak-plateau-bytes N] [--rss-plateau-kib N] \
         [--rss-tail-plateau-kib N] [--rss-tail-min-samples N] [--project-events N] \
         [--projected-live-growth-bytes N] [--projected-live-segment-growth-bytes N] \
         [--projected-peak-growth-bytes N] [--projected-peak-segment-growth-bytes N] \
         [--projected-rss-growth-kib N] [--projected-rss-segment-growth-kib N] \
         [--max-elapsed-seconds N] [--max-cpu-seconds N] [--max-average-event-us N] \
         [--max-interval-event-us N] [--max-disk-input-blocks N] [--max-disk-output-blocks N] \
         [--warmup-samples N] \
         [--check-invariants-every N] [--seed-seq-source N] \
         [--lineage-retention none|retained] [--lineage-store retained|noop] \
         [--correlation-store retained|noop] [--metadata-mode none|static|unique] \
         [--payload-bytes N] \
         [--materialize-state] [--unique-lineage-ids]"
    );
}

fn stress_route() -> RouteRefCore {
    RouteRefCore {
        namespace: NamespaceRefCore {
            plane: Plane::Read,
            layer: Layer::Logical,
            owner: "heart".to_string(),
        },
        family: "runtime".to_string(),
        stream: "retention_stress".to_string(),
        variant: Variant::Event,
        schema: SchemaRefCore {
            schema_id: "RetentionStressBytes".to_string(),
            version: 1,
        },
    }
}

fn stress_state_route() -> RouteRefCore {
    RouteRefCore {
        namespace: NamespaceRefCore {
            plane: Plane::State,
            layer: Layer::Logical,
            owner: "heart".to_string(),
        },
        family: "runtime".to_string(),
        stream: "retention_stress_state".to_string(),
        variant: Variant::State,
        schema: SchemaRefCore {
            schema_id: "RetentionStressStateBytes".to_string(),
            version: 1,
        },
    }
}

fn sample(
    graph: &GraphCore,
    routes: &[RouteRefCore],
    step: u64,
    elapsed_seconds: f64,
    last_sample_step: u64,
    last_sample_elapsed_seconds: f64,
    baseline_usage: UsageSnapshot,
) -> Sample {
    let usage = current_usage().saturating_delta(baseline_usage);
    let interval_events = step.saturating_sub(last_sample_step);
    let interval_elapsed_seconds = (elapsed_seconds - last_sample_elapsed_seconds).max(0.0);
    Sample {
        step,
        history: routes
            .iter()
            .map(|route| graph.history.get(route).map_or(0, |history| history.len()))
            .sum(),
        payloads: routes
            .iter()
            .map(|route| graph.retained_payload_count(route))
            .sum(),
        lineage: graph.retained_lineage_event_count(),
        lineage_values: graph.active_lineage_value_count(),
        trace_index: 0,
        correlation_index: 0,
        live_allocated_bytes: LIVE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        peak_allocated_bytes: PEAK_ALLOCATED_BYTES.load(Ordering::Relaxed),
        current_rss_kib: current_rss_kib(),
        elapsed_seconds,
        cpu_seconds: usage.cpu_seconds,
        average_event_us: event_latency_us(elapsed_seconds, step),
        interval_event_us: event_latency_us(interval_elapsed_seconds, interval_events),
        input_blocks: usage.input_blocks,
        output_blocks: usage.output_blocks,
    }
}

fn event_latency_us(elapsed_seconds: f64, events: u64) -> f64 {
    if events == 0 {
        return 0.0;
    }
    elapsed_seconds * 1_000_000.0 / events as f64
}

fn print_sample(sample: &Sample) {
    println!(
        "step={} history={} payloads={} lineage={} lineage_values={} trace_index={} \
         correlation_index={} live_allocated_bytes={} peak_allocated_bytes={} \
         current_rss_kib={} elapsed_seconds={:.3} cpu_seconds={:.3} \
         average_event_us={:.3} interval_event_us={:.3} \
         input_blocks={} output_blocks={}",
        sample.step,
        sample.history,
        sample.payloads,
        sample.lineage,
        sample.lineage_values,
        sample.trace_index,
        sample.correlation_index,
        sample.live_allocated_bytes,
        sample.peak_allocated_bytes,
        sample.current_rss_kib,
        sample.elapsed_seconds,
        sample.cpu_seconds,
        sample.average_event_us,
        sample.interval_event_us,
        sample.input_blocks,
        sample.output_blocks,
    );
}

fn assert_retained_counts(
    sample: &Sample,
    history_limit: usize,
    lineage_count: usize,
    correlation_count: usize,
) {
    assert_eq!(
        sample.history, history_limit,
        "history retained too many events"
    );
    assert_eq!(
        sample.payloads, history_limit,
        "payload store retained too many payloads"
    );
    assert_eq!(
        sample.lineage, lineage_count,
        "lineage store retained too many records"
    );
    assert!(
        sample.lineage_values <= lineage_count.saturating_mul(2),
        "lineage intern table retained too many values"
    );
    assert_eq!(
        sample.trace_index, lineage_count,
        "trace index retained too many records"
    );
    assert_eq!(
        sample.correlation_index, correlation_count,
        "correlation index retained unexpected records"
    );
}

fn check_rss_plateau(samples: &[Sample], warmup_samples: usize, plateau_kib: u64) {
    if samples.len() <= warmup_samples + 1 {
        return;
    }
    let steady = &samples[warmup_samples..];
    let minimum = steady
        .iter()
        .map(|sample| sample.current_rss_kib)
        .min()
        .unwrap_or(0);
    let maximum = steady
        .iter()
        .map(|sample| sample.current_rss_kib)
        .max()
        .unwrap_or(0);
    let range = maximum.saturating_sub(minimum);
    assert!(
        range <= plateau_kib,
        "current RSS did not plateau: range {range} KiB exceeds {plateau_kib} KiB"
    );
}

fn final_tail_rss_plateau(samples: &[Sample], plateau_kib: u64) -> (u64, usize) {
    let Some(last_sample) = samples.last() else {
        return (0, 0);
    };
    let mut minimum = last_sample.current_rss_kib;
    let mut maximum = minimum;
    let mut count = 1;
    for sample in samples.iter().rev().skip(1) {
        let next_minimum = minimum.min(sample.current_rss_kib);
        let next_maximum = maximum.max(sample.current_rss_kib);
        if next_maximum.saturating_sub(next_minimum) > plateau_kib {
            break;
        }
        minimum = next_minimum;
        maximum = next_maximum;
        count += 1;
    }
    (maximum.saturating_sub(minimum), count)
}

fn check_rss_tail_plateau(samples: &[Sample], plateau_kib: u64, min_samples: usize) {
    let (range, count) = final_tail_rss_plateau(samples, plateau_kib);
    assert!(
        count >= min_samples,
        "current RSS final plateau held {count} samples, below required {min_samples} \
         samples at range {range} KiB"
    );
}

fn check_live_bytes_plateau(samples: &[Sample], warmup_samples: usize, plateau_bytes: usize) {
    if samples.len() <= warmup_samples + 1 {
        return;
    }
    let steady = &samples[warmup_samples..];
    let minimum = steady
        .iter()
        .map(|sample| sample.live_allocated_bytes)
        .min()
        .unwrap_or(0);
    let maximum = steady
        .iter()
        .map(|sample| sample.live_allocated_bytes)
        .max()
        .unwrap_or(0);
    let range = maximum.saturating_sub(minimum);
    assert!(
        range <= plateau_bytes,
        "live heap bytes did not plateau: range {range} exceeds {plateau_bytes}"
    );
}

fn check_peak_bytes_plateau(samples: &[Sample], warmup_samples: usize, plateau_bytes: usize) {
    if samples.len() <= warmup_samples + 1 {
        return;
    }
    let steady = &samples[warmup_samples..];
    let minimum = steady
        .iter()
        .map(|sample| sample.peak_allocated_bytes)
        .min()
        .unwrap_or(0);
    let maximum = steady
        .iter()
        .map(|sample| sample.peak_allocated_bytes)
        .max()
        .unwrap_or(0);
    let range = maximum.saturating_sub(minimum);
    assert!(
        range <= plateau_bytes,
        "peak heap bytes did not plateau: range {range} exceeds {plateau_bytes}"
    );
}

fn projected_growth<T>(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    value: T,
) -> u64
where
    T: Fn(&Sample) -> u64,
{
    if samples.len() <= warmup_samples + 1 {
        return 0;
    }
    let steady = &samples[warmup_samples..];
    let first = steady.first().expect("steady samples should not be empty");
    let last = steady.last().expect("steady samples should not be empty");
    let event_span = last.step.saturating_sub(first.step);
    if event_span == 0 {
        return 0;
    }
    let growth = value(last).saturating_sub(value(first));
    growth.saturating_mul(project_events) / event_span
}

fn projected_segment_growth<T>(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    value: T,
) -> u64
where
    T: Fn(&Sample) -> u64,
{
    if samples.len() <= warmup_samples + 1 {
        return 0;
    }
    let steady = &samples[warmup_samples..];
    let mut projected = 0;
    for pair in steady.windows(2) {
        let first = &pair[0];
        let last = &pair[1];
        let event_span = last.step.saturating_sub(first.step);
        if event_span == 0 {
            continue;
        }
        let growth = value(last).saturating_sub(value(first));
        let segment_projected = growth.saturating_mul(project_events) / event_span;
        projected = projected.max(segment_projected);
    }
    projected
}

fn check_projected_live_bytes_growth(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    limit_bytes: usize,
) {
    let projected = projected_growth(samples, warmup_samples, project_events, |sample| {
        sample.live_allocated_bytes as u64
    });
    assert!(
        projected <= limit_bytes as u64,
        "live heap bytes project to {projected} over {project_events} events, \
         exceeding {limit_bytes}"
    );
}

fn check_projected_live_bytes_segment_growth(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    limit_bytes: usize,
) {
    let projected = projected_segment_growth(samples, warmup_samples, project_events, |sample| {
        sample.live_allocated_bytes as u64
    });
    assert!(
        projected <= limit_bytes as u64,
        "live heap byte segment projects to {projected} over {project_events} events, \
         exceeding {limit_bytes}"
    );
}

fn check_projected_peak_bytes_growth(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    limit_bytes: usize,
) {
    let projected = projected_growth(samples, warmup_samples, project_events, |sample| {
        sample.peak_allocated_bytes as u64
    });
    assert!(
        projected <= limit_bytes as u64,
        "peak heap bytes project to {projected} over {project_events} events, \
         exceeding {limit_bytes}"
    );
}

fn check_projected_peak_bytes_segment_growth(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    limit_bytes: usize,
) {
    let projected = projected_segment_growth(samples, warmup_samples, project_events, |sample| {
        sample.peak_allocated_bytes as u64
    });
    assert!(
        projected <= limit_bytes as u64,
        "peak heap byte segment projects to {projected} over {project_events} events, \
         exceeding {limit_bytes}"
    );
}

fn check_projected_rss_growth(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    limit_kib: u64,
) {
    let projected = projected_growth(samples, warmup_samples, project_events, |sample| {
        sample.current_rss_kib
    });
    assert!(
        projected <= limit_kib,
        "current RSS projects to {projected} KiB over {project_events} events, \
         exceeding {limit_kib} KiB"
    );
}

fn check_projected_rss_segment_growth(
    samples: &[Sample],
    warmup_samples: usize,
    project_events: u64,
    limit_kib: u64,
) {
    let projected = projected_segment_growth(samples, warmup_samples, project_events, |sample| {
        sample.current_rss_kib
    });
    assert!(
        projected <= limit_kib,
        "current RSS segment projects to {projected} KiB over {project_events} events, \
         exceeding {limit_kib} KiB"
    );
}

fn check_elapsed_seconds(samples: &[Sample], max_elapsed_seconds: f64) {
    let Some(final_sample) = samples.last() else {
        return;
    };
    assert!(
        final_sample.elapsed_seconds <= max_elapsed_seconds,
        "elapsed seconds {:.3} exceeds {:.3}",
        final_sample.elapsed_seconds,
        max_elapsed_seconds
    );
}

fn check_cpu_seconds(samples: &[Sample], max_cpu_seconds: f64) {
    let Some(final_sample) = samples.last() else {
        return;
    };
    assert!(
        final_sample.cpu_seconds <= max_cpu_seconds,
        "CPU seconds {:.3} exceeds {:.3}",
        final_sample.cpu_seconds,
        max_cpu_seconds
    );
}

fn check_average_event_latency(samples: &[Sample], max_average_event_us: f64) {
    let Some(final_sample) = samples.last() else {
        return;
    };
    assert!(
        final_sample.average_event_us <= max_average_event_us,
        "average event latency {:.3}us exceeds {:.3}us",
        final_sample.average_event_us,
        max_average_event_us
    );
}

fn check_interval_event_latency(samples: &[Sample], max_interval_event_us: f64) {
    let Some(slowest_sample) = samples.iter().max_by(|left, right| {
        left.interval_event_us
            .partial_cmp(&right.interval_event_us)
            .unwrap_or(std::cmp::Ordering::Equal)
    }) else {
        return;
    };
    assert!(
        slowest_sample.interval_event_us <= max_interval_event_us,
        "interval event latency {:.3}us at step {} exceeds {:.3}us",
        slowest_sample.interval_event_us,
        slowest_sample.step,
        max_interval_event_us
    );
}

fn check_block_io(
    samples: &[Sample],
    max_input_blocks: Option<u64>,
    max_output_blocks: Option<u64>,
) {
    let Some(final_sample) = samples.last() else {
        return;
    };
    if let Some(max_input_blocks) = max_input_blocks {
        assert!(
            final_sample.input_blocks <= max_input_blocks,
            "block input {} exceeds {}",
            final_sample.input_blocks,
            max_input_blocks
        );
    }
    if let Some(max_output_blocks) = max_output_blocks {
        assert!(
            final_sample.output_blocks <= max_output_blocks,
            "block output {} exceeds {}",
            final_sample.output_blocks,
            max_output_blocks
        );
    }
}

impl UsageSnapshot {
    fn saturating_delta(self, baseline: Self) -> Self {
        Self {
            cpu_seconds: (self.cpu_seconds - baseline.cpu_seconds).max(0.0),
            input_blocks: self.input_blocks.saturating_sub(baseline.input_blocks),
            output_blocks: self.output_blocks.saturating_sub(baseline.output_blocks),
        }
    }
}

#[cfg(unix)]
fn current_usage() -> UsageSnapshot {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return zero_usage();
    }
    let usage = unsafe { usage.assume_init() };
    UsageSnapshot {
        cpu_seconds: timeval_seconds(usage.ru_utime) + timeval_seconds(usage.ru_stime),
        input_blocks: usage.ru_inblock.max(0) as u64,
        output_blocks: usage.ru_oublock.max(0) as u64,
    }
}

#[cfg(unix)]
fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

#[cfg(not(unix))]
fn current_usage() -> UsageSnapshot {
    zero_usage()
}

fn zero_usage() -> UsageSnapshot {
    UsageSnapshot {
        cpu_seconds: 0.0,
        input_blocks: 0,
        output_blocks: 0,
    }
}

fn current_rss_kib() -> u64 {
    if let Ok(value) = current_rss_kib_from_proc() {
        return value;
    }
    current_rss_kib_from_ps().unwrap_or(0)
}

fn current_rss_kib_from_proc() -> Result<u64, std::io::Error> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    let Some(resident_pages) = statm.split_whitespace().nth(1) else {
        return Ok(0);
    };
    let pages = resident_pages.parse::<u64>().unwrap_or(0);
    Ok(pages * 4)
}

fn current_rss_kib_from_ps() -> Result<u64, std::io::Error> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(text.trim().parse::<u64>().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_segment_growth_rejects_late_live_heap_slope() {
        let samples = vec![
            sample_for_projection(100_000, 10_000, 33_000),
            sample_for_projection(200_000, 10_000, 33_000),
            sample_for_projection(300_000, 10_000, 33_000),
            sample_for_projection(400_000, 11_000, 33_000),
        ];

        let projected = projected_segment_growth(&samples, 0, 1_000_000_000, |sample| {
            sample.live_allocated_bytes as u64
        });

        assert_eq!(projected, 10_000_000);
    }

    #[test]
    #[should_panic(expected = "live heap byte segment projects to")]
    fn live_heap_segment_check_fails_on_late_resumed_slope() {
        let samples = vec![
            sample_for_projection(100_000, 10_000, 33_000),
            sample_for_projection(200_000, 10_000, 33_000),
            sample_for_projection(300_000, 10_000, 33_000),
            sample_for_projection(400_000, 11_000, 33_000),
        ];

        check_projected_live_bytes_segment_growth(&samples, 0, 1_000_000_000, 1_000_000);
    }

    #[test]
    fn peak_heap_plateau_check_accepts_flat_high_water() {
        let samples = vec![
            sample_for_peak_projection(100_000, 10_000, 24_000),
            sample_for_peak_projection(200_000, 10_000, 24_000),
            sample_for_peak_projection(300_000, 10_000, 24_000),
        ];

        check_peak_bytes_plateau(&samples, 0, 0);
    }

    #[test]
    #[should_panic(expected = "peak heap bytes")]
    fn peak_heap_plateau_check_rejects_rising_high_water() {
        let samples = vec![
            sample_for_peak_projection(100_000, 10_000, 24_000),
            sample_for_peak_projection(200_000, 10_000, 24_001),
        ];

        check_peak_bytes_plateau(&samples, 0, 0);
    }

    #[test]
    #[should_panic(expected = "peak heap byte segment")]
    fn peak_heap_segment_check_fails_on_late_high_water_slope() {
        let samples = vec![
            sample_for_peak_projection(100_000, 10_000, 24_000),
            sample_for_peak_projection(200_000, 10_000, 24_000),
            sample_for_peak_projection(300_000, 10_000, 24_001),
        ];

        check_projected_peak_bytes_segment_growth(&samples, 0, 1_000_000_000, 1_000);
    }

    #[test]
    #[should_panic(expected = "current RSS segment projects to")]
    fn rss_segment_check_fails_on_late_resumed_slope() {
        let samples = vec![
            sample_for_projection(100_000, 10_000, 33_000),
            sample_for_projection(200_000, 10_000, 33_000),
            sample_for_projection(300_000, 10_000, 33_500),
        ];

        check_projected_rss_segment_growth(&samples, 0, 1_000_000_000, 1_000_000);
    }

    #[test]
    fn rss_tail_plateau_ignores_startup_growth() {
        let samples = vec![
            sample_for_projection(100_000, 10_000, 33_000),
            sample_for_projection(200_000, 10_000, 33_512),
            sample_for_projection(300_000, 10_000, 33_536),
            sample_for_projection(400_000, 10_000, 33_536),
        ];

        let (range, count) = final_tail_rss_plateau(&samples, 64);

        assert_eq!(range, 24);
        assert_eq!(count, 3);
        check_rss_tail_plateau(&samples, 64, 3);
    }

    #[test]
    #[should_panic(expected = "current RSS final plateau held")]
    fn rss_tail_plateau_check_rejects_short_final_tail() {
        let samples = vec![
            sample_for_projection(100_000, 10_000, 33_000),
            sample_for_projection(200_000, 10_000, 33_512),
            sample_for_projection(300_000, 10_000, 34_024),
            sample_for_projection(400_000, 10_000, 34_024),
        ];

        check_rss_tail_plateau(&samples, 64, 3);
    }

    #[test]
    fn elapsed_seconds_check_accepts_runs_under_limit() {
        let samples = vec![sample_for_elapsed(100_000, 1.25)];

        check_elapsed_seconds(&samples, 2.0);
    }

    #[test]
    #[should_panic(expected = "elapsed seconds")]
    fn elapsed_seconds_check_rejects_runs_over_limit() {
        let samples = vec![sample_for_elapsed(100_000, 2.25)];

        check_elapsed_seconds(&samples, 2.0);
    }

    #[test]
    fn cpu_seconds_check_accepts_runs_under_limit() {
        let samples = vec![sample_for_cpu(100_000, 1.25)];

        check_cpu_seconds(&samples, 2.0);
    }

    #[test]
    #[should_panic(expected = "CPU seconds")]
    fn cpu_seconds_check_rejects_runs_over_limit() {
        let samples = vec![sample_for_cpu(100_000, 2.25)];

        check_cpu_seconds(&samples, 2.0);
    }

    #[test]
    fn average_event_latency_check_accepts_runs_under_limit() {
        let samples = vec![sample_for_average_event_latency(100_000, 12.5)];

        check_average_event_latency(&samples, 20.0);
    }

    #[test]
    #[should_panic(expected = "average event latency")]
    fn average_event_latency_check_rejects_slow_runs() {
        let samples = vec![sample_for_average_event_latency(100_000, 25.0)];

        check_average_event_latency(&samples, 20.0);
    }

    #[test]
    fn interval_event_latency_check_accepts_runs_under_limit() {
        let samples = vec![sample_for_interval_event_latency(100_000, 12.5)];

        check_interval_event_latency(&samples, 20.0);
    }

    #[test]
    #[should_panic(expected = "interval event latency")]
    fn interval_event_latency_check_rejects_late_slow_runs() {
        let samples = vec![
            sample_for_interval_event_latency(100_000, 12.5),
            sample_for_interval_event_latency(200_000, 25.0),
        ];

        check_interval_event_latency(&samples, 20.0);
    }

    #[test]
    fn block_io_check_accepts_runs_under_limits() {
        let samples = vec![sample_for_block_io(100_000, 0, 0)];

        check_block_io(&samples, Some(0), Some(0));
    }

    #[test]
    #[should_panic(expected = "block output")]
    fn block_io_check_rejects_disk_writes() {
        let samples = vec![sample_for_block_io(100_000, 0, 1)];

        check_block_io(&samples, None, Some(0));
    }

    #[test]
    fn materialized_state_mode_bounds_source_and_state_retention() {
        let samples = run_stress(Config {
            iterations: 8,
            history_limit: 8,
            sample_every: 8,
            rss_plateau_kib: None,
            rss_tail_plateau_kib: None,
            rss_tail_min_samples: 1,
            live_plateau_bytes: None,
            peak_plateau_bytes: None,
            projected_live_growth_bytes: None,
            projected_live_segment_growth_bytes: None,
            projected_peak_growth_bytes: None,
            projected_peak_segment_growth_bytes: None,
            projected_rss_growth_kib: None,
            projected_rss_segment_growth_kib: None,
            max_elapsed_seconds: None,
            max_cpu_seconds: None,
            max_average_event_us: None,
            max_interval_event_us: None,
            max_disk_input_blocks: None,
            max_disk_output_blocks: None,
            project_events: 1_000_000_000,
            warmup_samples: 1,
            check_invariants_every: 1,
            seed_seq_source: None,
            lineage_retention: LineageRetentionMode::Retained,
            lineage_store: LineageStoreMode::Retained,
            correlation_store: CorrelationStoreMode::Retained,
            metadata_mode: MetadataMode::Static,
            payload_bytes: b"frame".len(),
            materialize_state: true,
        });

        let latest = samples.last().expect("stress run should sample");

        assert_eq!(latest.history, 16);
        assert_eq!(latest.payloads, 16);
        assert_eq!(latest.lineage, 0);
        assert_eq!(latest.lineage_values, 0);
        assert_eq!(latest.trace_index, 0);
        assert_eq!(latest.correlation_index, 0);
    }

    #[test]
    fn materialized_state_sparse_metadata_skips_correlation_index() {
        let samples = run_stress(Config {
            iterations: 8,
            history_limit: 8,
            sample_every: 8,
            rss_plateau_kib: None,
            rss_tail_plateau_kib: None,
            rss_tail_min_samples: 1,
            live_plateau_bytes: None,
            peak_plateau_bytes: None,
            projected_live_growth_bytes: None,
            projected_live_segment_growth_bytes: None,
            projected_peak_growth_bytes: None,
            projected_peak_segment_growth_bytes: None,
            projected_rss_growth_kib: None,
            projected_rss_segment_growth_kib: None,
            max_elapsed_seconds: None,
            max_cpu_seconds: None,
            max_average_event_us: None,
            max_interval_event_us: None,
            max_disk_input_blocks: None,
            max_disk_output_blocks: None,
            project_events: 1_000_000_000,
            warmup_samples: 1,
            check_invariants_every: 1,
            seed_seq_source: None,
            lineage_retention: LineageRetentionMode::Retained,
            lineage_store: LineageStoreMode::Retained,
            correlation_store: CorrelationStoreMode::Noop,
            metadata_mode: MetadataMode::None,
            payload_bytes: b"frame".len(),
            materialize_state: true,
        });

        let latest = samples.last().expect("stress run should sample");

        assert_eq!(latest.lineage, 0);
        assert_eq!(latest.lineage_values, 0);
        assert_eq!(latest.trace_index, 0);
        assert_eq!(latest.correlation_index, 0);
    }

    #[test]
    fn noop_lineage_store_suppresses_requested_retained_lineage() {
        let samples = run_stress(Config {
            iterations: 8,
            history_limit: 8,
            sample_every: 8,
            rss_plateau_kib: None,
            rss_tail_plateau_kib: None,
            rss_tail_min_samples: 1,
            live_plateau_bytes: None,
            peak_plateau_bytes: None,
            projected_live_growth_bytes: None,
            projected_live_segment_growth_bytes: None,
            projected_peak_growth_bytes: None,
            projected_peak_segment_growth_bytes: None,
            projected_rss_growth_kib: None,
            projected_rss_segment_growth_kib: None,
            max_elapsed_seconds: None,
            max_cpu_seconds: None,
            max_average_event_us: None,
            max_interval_event_us: None,
            max_disk_input_blocks: None,
            max_disk_output_blocks: None,
            project_events: 1_000_000_000,
            warmup_samples: 1,
            check_invariants_every: 1,
            seed_seq_source: None,
            lineage_retention: LineageRetentionMode::Retained,
            lineage_store: LineageStoreMode::Noop,
            correlation_store: CorrelationStoreMode::Noop,
            metadata_mode: MetadataMode::Static,
            payload_bytes: b"frame".len(),
            materialize_state: true,
        });

        let latest = samples.last().expect("stress run should sample");

        assert_eq!(latest.history, 16);
        assert_eq!(latest.payloads, 16);
        assert_eq!(latest.lineage, 0);
        assert_eq!(latest.lineage_values, 0);
        assert_eq!(latest.trace_index, 0);
        assert_eq!(latest.correlation_index, 0);
    }

    fn sample_for_projection(
        step: u64,
        live_allocated_bytes: usize,
        current_rss_kib: u64,
    ) -> Sample {
        Sample {
            live_allocated_bytes,
            peak_allocated_bytes: live_allocated_bytes,
            current_rss_kib,
            ..empty_sample(step)
        }
    }

    fn sample_for_peak_projection(
        step: u64,
        live_allocated_bytes: usize,
        peak_allocated_bytes: usize,
    ) -> Sample {
        Sample {
            live_allocated_bytes,
            peak_allocated_bytes,
            ..empty_sample(step)
        }
    }

    fn sample_for_elapsed(step: u64, elapsed_seconds: f64) -> Sample {
        Sample {
            elapsed_seconds,
            ..empty_sample(step)
        }
    }

    fn sample_for_block_io(step: u64, input_blocks: u64, output_blocks: u64) -> Sample {
        Sample {
            input_blocks,
            output_blocks,
            ..empty_sample(step)
        }
    }

    fn sample_for_cpu(step: u64, cpu_seconds: f64) -> Sample {
        Sample {
            cpu_seconds,
            ..empty_sample(step)
        }
    }

    fn sample_for_average_event_latency(step: u64, average_event_us: f64) -> Sample {
        Sample {
            average_event_us,
            ..empty_sample(step)
        }
    }

    fn sample_for_interval_event_latency(step: u64, interval_event_us: f64) -> Sample {
        Sample {
            interval_event_us,
            ..empty_sample(step)
        }
    }

    fn empty_sample(step: u64) -> Sample {
        Sample {
            step,
            history: 0,
            payloads: 0,
            lineage: 0,
            lineage_values: 0,
            trace_index: 0,
            correlation_index: 0,
            live_allocated_bytes: 0,
            peak_allocated_bytes: 0,
            current_rss_kib: 0,
            elapsed_seconds: 0.0,
            cpu_seconds: 0.0,
            average_event_us: 0.0,
            interval_event_us: 0.0,
            input_blocks: 0,
            output_blocks: 0,
        }
    }
}
