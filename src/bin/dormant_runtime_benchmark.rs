use std::env;
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Slots,
    Processes,
}

struct Config {
    mode: Mode,
    capacity_instances: usize,
    dormant_processes_per_runtime: usize,
    idle_seconds: f64,
    poll_iterations: usize,
    wake_iterations: usize,
    child_idle: bool,
    child_wake: bool,
    max_average_wake_us: Option<f64>,
    max_child_rss_per_process_kib: Option<f64>,
}

struct DormantRuntime {
    name: String,
    slots: Vec<DormantSlot>,
}

struct DormantSlot {
    runtime: String,
    slot: usize,
    address: String,
}

impl DormantRuntime {
    fn new(name: String, dormant_processes: usize) -> Self {
        let slots = (1..=dormant_processes)
            .map(|slot| DormantSlot {
                runtime: name.clone(),
                slot,
                address: format!("process://{name}/frozen-{slot}"),
            })
            .collect();
        Self { name, slots }
    }

    fn dormant_process_count(&self) -> usize {
        self.slots.len()
    }

    fn wake_one(&mut self) -> usize {
        let slot = self
            .slots
            .pop()
            .expect("runtime must have a dormant slot to wake");
        let fingerprint = runtime_slot_fingerprint(&slot);
        self.slots.push(slot);
        fingerprint
    }
}

struct SlotBenchmarkResult {
    controller_instances: usize,
    capacity_instances: usize,
    dormant_processes_per_runtime: usize,
    total_dormant_processes: usize,
    construction_seconds: f64,
    idle_wall_seconds: f64,
    idle_cpu_seconds: f64,
    idle_cpu_percent: f64,
    poll_iterations: usize,
    poll_seconds: f64,
    average_poll_us: f64,
    wake_iterations: usize,
    wake_seconds: f64,
    average_wake_us: f64,
    current_rss_kib: u64,
}

struct ProcessBenchmarkResult {
    controller_instances: usize,
    spawned_instances: usize,
    construction_seconds: f64,
    idle_wall_seconds: f64,
    child_cpu_seconds: f64,
    child_cpu_percent: f64,
    child_rss_total_kib: u64,
    child_rss_per_process_kib: f64,
    wake_seconds: f64,
    average_wake_us: f64,
}

fn main() {
    let config = Config::parse(env::args().skip(1).collect());
    if config.child_idle {
        sleep(Duration::from_secs_f64(config.idle_seconds));
        return;
    }
    if config.child_wake {
        wait_for_wake();
        return;
    }
    match config.mode {
        Mode::Slots => {
            let result = run_slot_benchmark(&config);
            check_wake_gate(result.average_wake_us, &config);
            print_slot_benchmark(result);
        }
        Mode::Processes => {
            let result = run_process_benchmark(&config);
            check_wake_gate(result.average_wake_us, &config);
            if let Some(max_rss) = config.max_child_rss_per_process_kib {
                if result.child_rss_per_process_kib > max_rss {
                    panic!(
                        "child RSS per process {:.3} KiB exceeds {:.3} KiB",
                        result.child_rss_per_process_kib, max_rss
                    );
                }
            }
            print_process_benchmark(result);
        }
    }
}

fn run_slot_benchmark(config: &Config) -> SlotBenchmarkResult {
    let start = Instant::now();
    let mut runtimes: Vec<DormantRuntime> = (0..config.capacity_instances)
        .map(|index| {
            DormantRuntime::new(
                format!("node-{index}"),
                config.dormant_processes_per_runtime,
            )
        })
        .collect();
    let construction_seconds = start.elapsed().as_secs_f64();
    let idle_usage_start = current_usage(libc::RUSAGE_SELF);
    let idle_start = Instant::now();
    sleep(Duration::from_secs_f64(config.idle_seconds));
    let idle_wall_seconds = idle_start.elapsed().as_secs_f64();
    let idle_cpu_seconds = current_usage(libc::RUSAGE_SELF)
        .saturating_delta(idle_usage_start)
        .cpu_seconds;
    let poll_start = Instant::now();
    let mut total_dormant_processes = 0;
    for _ in 0..config.poll_iterations {
        total_dormant_processes = runtimes
            .iter()
            .map(DormantRuntime::dormant_process_count)
            .sum();
    }
    let poll_seconds = poll_start.elapsed().as_secs_f64();
    let wake_start = Instant::now();
    let mut wake_fingerprint = 0usize;
    for iteration in 0..config.wake_iterations {
        let runtime = &mut runtimes[iteration % config.capacity_instances];
        wake_fingerprint ^= runtime.wake_one();
    }
    let wake_seconds = wake_start.elapsed().as_secs_f64();
    std::hint::black_box(
        runtimes
            .iter()
            .flat_map(|runtime| runtime.slots.iter())
            .fold(0usize, |acc, slot| {
                acc ^ runtime_slot_fingerprint(slot) ^ slot.runtime.len()
            })
            ^ runtimes
                .iter()
                .fold(0usize, |acc, runtime| acc ^ runtime.name.len()),
    );
    std::hint::black_box(wake_fingerprint);
    SlotBenchmarkResult {
        controller_instances: 1,
        capacity_instances: config.capacity_instances,
        dormant_processes_per_runtime: config.dormant_processes_per_runtime,
        total_dormant_processes,
        construction_seconds,
        idle_wall_seconds,
        idle_cpu_seconds,
        idle_cpu_percent: cpu_percent(idle_cpu_seconds, idle_wall_seconds, 1),
        poll_iterations: config.poll_iterations,
        poll_seconds,
        average_poll_us: poll_seconds * 1_000_000.0 / config.poll_iterations as f64,
        wake_iterations: config.wake_iterations,
        wake_seconds,
        average_wake_us: wake_seconds * 1_000_000.0 / config.wake_iterations as f64,
        current_rss_kib: current_rss_kib(std::process::id()),
    }
}

fn run_process_benchmark(config: &Config) -> ProcessBenchmarkResult {
    let current_exe = env::current_exe().expect("current executable path");
    let child_usage_start = current_usage(libc::RUSAGE_CHILDREN);
    let start = Instant::now();
    let mut children: Vec<Child> = (0..config.capacity_instances)
        .map(|_| {
            Command::new(&current_exe)
                .arg("--child-wake")
                .stdin(Stdio::piped())
                .spawn()
                .expect("spawn dormant runtime child")
        })
        .collect();
    let construction_seconds = start.elapsed().as_secs_f64();
    let idle_start = Instant::now();
    sleep(Duration::from_secs_f64(config.idle_seconds));
    let idle_wall_seconds = idle_start.elapsed().as_secs_f64();
    let child_rss_total_kib = children
        .iter()
        .map(|child| current_rss_kib(child.id()))
        .sum::<u64>();
    let wake_start = Instant::now();
    for child in &mut children {
        child
            .stdin
            .as_mut()
            .expect("dormant runtime child stdin")
            .write_all(&[1])
            .expect("wake dormant runtime child");
    }
    for child in &mut children {
        let status = child.wait().expect("wait for dormant runtime child");
        assert!(
            status.success(),
            "dormant runtime child exited unsuccessfully"
        );
    }
    let wake_seconds = wake_start.elapsed().as_secs_f64();
    let child_cpu_seconds = current_usage(libc::RUSAGE_CHILDREN)
        .saturating_delta(child_usage_start)
        .cpu_seconds;
    ProcessBenchmarkResult {
        controller_instances: 1,
        spawned_instances: config.capacity_instances,
        construction_seconds,
        idle_wall_seconds,
        child_cpu_seconds,
        child_cpu_percent: cpu_percent(
            child_cpu_seconds,
            idle_wall_seconds,
            config.capacity_instances,
        ),
        child_rss_total_kib,
        child_rss_per_process_kib: child_rss_total_kib as f64 / config.capacity_instances as f64,
        wake_seconds,
        average_wake_us: wake_seconds * 1_000_000.0 / config.capacity_instances as f64,
    }
}

fn wait_for_wake() {
    let mut byte = [0_u8; 1];
    std::io::stdin()
        .read_exact(&mut byte)
        .expect("read dormant runtime wake byte");
}

fn runtime_slot_fingerprint(slot: &DormantSlot) -> usize {
    slot.slot ^ slot.address.len()
}

fn print_slot_benchmark(result: SlotBenchmarkResult) {
    println!(
        "{{\n  \"mode\": \"slots\",\n  \"controller_instances\": {},\n  \
         \"capacity_instances\": {},\n  \
         \"dormant_processes_per_runtime\": {},\n  \"total_dormant_processes\": {},\n  \
         \"construction_seconds\": {:.9},\n  \"idle_wall_seconds\": {:.9},\n  \
         \"idle_cpu_seconds\": {:.9},\n  \"idle_cpu_percent\": {:.6},\n  \
         \"poll_iterations\": {},\n  \"poll_seconds\": {:.9},\n  \
         \"average_poll_us\": {:.6},\n  \"wake_iterations\": {},\n  \
         \"wake_seconds\": {:.9},\n  \"average_wake_us\": {:.6},\n  \
         \"current_rss_kib\": {}\n}}",
        result.controller_instances,
        result.capacity_instances,
        result.dormant_processes_per_runtime,
        result.total_dormant_processes,
        result.construction_seconds,
        result.idle_wall_seconds,
        result.idle_cpu_seconds,
        result.idle_cpu_percent,
        result.poll_iterations,
        result.poll_seconds,
        result.average_poll_us,
        result.wake_iterations,
        result.wake_seconds,
        result.average_wake_us,
        result.current_rss_kib,
    );
}

fn print_process_benchmark(result: ProcessBenchmarkResult) {
    println!(
        "{{\n  \"mode\": \"processes\",\n  \"controller_instances\": {},\n  \
         \"spawned_instances\": {},\n  \
         \"construction_seconds\": {:.9},\n  \"idle_wall_seconds\": {:.9},\n  \
         \"child_cpu_seconds\": {:.9},\n  \"child_cpu_percent\": {:.6},\n  \
         \"child_rss_total_kib\": {},\n  \"child_rss_per_process_kib\": {:.3},\n  \
         \"wake_seconds\": {:.9},\n  \"average_wake_us\": {:.6}\n}}",
        result.controller_instances,
        result.spawned_instances,
        result.construction_seconds,
        result.idle_wall_seconds,
        result.child_cpu_seconds,
        result.child_cpu_percent,
        result.child_rss_total_kib,
        result.child_rss_per_process_kib,
        result.wake_seconds,
        result.average_wake_us,
    );
}

fn current_rss_kib(pid: u32) -> u64 {
    current_rss_kib_from_proc(pid)
        .or_else(|_| current_rss_kib_from_ps(pid))
        .unwrap_or(0)
}

fn current_rss_kib_from_proc(pid: u32) -> Result<u64, std::io::Error> {
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm"))?;
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    Ok(resident_pages * page_size_kib())
}

fn current_rss_kib_from_ps(pid: u32) -> Result<u64, std::io::Error> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()?;
    if !output.status.success() {
        return Ok(0);
    }
    let rss = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0);
    Ok(rss)
}

fn page_size_kib() -> u64 {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return 4;
    }
    page_size as u64 / 1024
}

#[derive(Clone, Copy)]
struct UsageSnapshot {
    cpu_seconds: f64,
}

impl UsageSnapshot {
    fn saturating_delta(self, baseline: Self) -> Self {
        Self {
            cpu_seconds: (self.cpu_seconds - baseline.cpu_seconds).max(0.0),
        }
    }
}

fn current_usage(who: libc::c_int) -> UsageSnapshot {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(who, usage.as_mut_ptr()) };
    if result != 0 {
        return UsageSnapshot { cpu_seconds: 0.0 };
    }
    let usage = unsafe { usage.assume_init() };
    UsageSnapshot {
        cpu_seconds: timeval_seconds(usage.ru_utime) + timeval_seconds(usage.ru_stime),
    }
}

fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

fn cpu_percent(cpu_seconds: f64, wall_seconds: f64, runtime_count: usize) -> f64 {
    if wall_seconds <= 0.0 || runtime_count == 0 {
        return 0.0;
    }
    cpu_seconds / (wall_seconds * runtime_count as f64) * 100.0
}

fn check_wake_gate(average_wake_us: f64, config: &Config) {
    if let Some(max_wake_us) = config.max_average_wake_us {
        if average_wake_us > max_wake_us {
            panic!("average wake latency {average_wake_us:.6} us exceeds {max_wake_us:.6} us");
        }
    }
}

impl Config {
    fn parse(args: Vec<String>) -> Self {
        let mut config = Self {
            mode: Mode::Slots,
            capacity_instances: 1,
            dormant_processes_per_runtime: 10,
            idle_seconds: 0.25,
            poll_iterations: 100_000,
            wake_iterations: 100_000,
            child_idle: false,
            child_wake: false,
            max_average_wake_us: None,
            max_child_rss_per_process_kib: None,
        };
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--mode" => {
                    index += 1;
                    config.mode = parse_mode(args.get(index));
                }
                "--capacity-instances" | "--runtime-count" => {
                    index += 1;
                    config.capacity_instances =
                        parse_usize(args.get(index), "--capacity-instances");
                }
                "--dormant-processes" => {
                    index += 1;
                    config.dormant_processes_per_runtime =
                        parse_usize(args.get(index), "--dormant-processes");
                }
                "--idle-seconds" => {
                    index += 1;
                    config.idle_seconds = parse_f64(args.get(index), "--idle-seconds");
                }
                "--poll-iterations" => {
                    index += 1;
                    config.poll_iterations = parse_usize(args.get(index), "--poll-iterations");
                }
                "--wake-iterations" => {
                    index += 1;
                    config.wake_iterations = parse_usize(args.get(index), "--wake-iterations");
                }
                "--child-idle" => {
                    config.child_idle = true;
                }
                "--child-wake" => {
                    config.child_wake = true;
                }
                "--max-average-wake-us" => {
                    index += 1;
                    config.max_average_wake_us =
                        Some(parse_f64(args.get(index), "--max-average-wake-us"));
                }
                "--max-child-rss-per-process-kib" => {
                    index += 1;
                    config.max_child_rss_per_process_kib = Some(parse_f64(
                        args.get(index),
                        "--max-child-rss-per-process-kib",
                    ));
                }
                "--help" | "-h" => {
                    print_usage_and_exit();
                }
                other => panic!("unknown argument: {other}"),
            }
            index += 1;
        }
        if config.capacity_instances == 0 {
            panic!("--capacity-instances must be positive");
        }
        if config.dormant_processes_per_runtime == 0 {
            panic!("--dormant-processes must be positive");
        }
        if config.poll_iterations == 0 {
            panic!("--poll-iterations must be positive");
        }
        if config.wake_iterations == 0 {
            panic!("--wake-iterations must be positive");
        }
        if config.idle_seconds < 0.0 {
            panic!("--idle-seconds must be non-negative");
        }
        config
    }
}

fn parse_mode(value: Option<&String>) -> Mode {
    match value.map(String::as_str) {
        Some("slots") => Mode::Slots,
        Some("processes") => Mode::Processes,
        Some(other) => panic!("unknown --mode: {other}"),
        None => panic!("--mode requires a value"),
    }
}

fn parse_usize(value: Option<&String>, name: &str) -> usize {
    value
        .unwrap_or_else(|| panic!("{name} requires a value"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an integer"))
}

fn parse_f64(value: Option<&String>, name: &str) -> f64 {
    value
        .unwrap_or_else(|| panic!("{name} requires a value"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a number"))
}

fn print_usage_and_exit() -> ! {
    println!(
        "dormant_runtime_benchmark [--mode slots|processes] [--capacity-instances N] \
         [--dormant-processes N] [--idle-seconds N] [--poll-iterations N] \
         [--wake-iterations N] [--max-average-wake-us N] \
         [--max-child-rss-per-process-kib N]"
    );
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_benchmark_reports_requested_shape() {
        let result = run_slot_benchmark(&Config {
            mode: Mode::Slots,
            capacity_instances: 2,
            dormant_processes_per_runtime: 3,
            idle_seconds: 0.0,
            poll_iterations: 4,
            wake_iterations: 4,
            child_idle: false,
            child_wake: false,
            max_average_wake_us: None,
            max_child_rss_per_process_kib: None,
        });

        assert_eq!(result.controller_instances, 1);
        assert_eq!(result.capacity_instances, 2);
        assert_eq!(result.dormant_processes_per_runtime, 3);
        assert_eq!(result.total_dormant_processes, 6);
        assert_eq!(result.poll_iterations, 4);
        assert!(result.average_poll_us >= 0.0);
        assert_eq!(result.wake_iterations, 4);
        assert!(result.average_wake_us >= 0.0);
    }

    #[test]
    fn parser_rejects_zero_capacity_instances() {
        let result = std::panic::catch_unwind(|| {
            Config::parse(vec!["--capacity-instances".to_string(), "0".to_string()]);
        });

        assert!(result.is_err());
    }

    #[test]
    fn parser_accepts_legacy_runtime_count_alias() {
        let config = Config::parse(vec!["--runtime-count".to_string(), "2".to_string()]);

        assert_eq!(config.capacity_instances, 2);
    }
}
