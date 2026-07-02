use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_LOG_BACKUP_COUNT: usize = 5;
pub const DEFAULT_LOG_MAX_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_LOG_TOPIC: &str = "logs";
pub const DEFAULT_METRIC_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0];
pub const DEFAULT_METRICS_TOPIC: &str = "metrics.histograms";
pub const DEFAULT_OBSERVABILITY_NAMESPACE: &str = "manyfold.observability";

#[derive(Clone, Debug, PartialEq)]
pub struct LogRecordEnvelopeCore {
    pub timestamp_ns: u64,
    pub severity_text: String,
    pub logger_name: String,
    pub body: String,
    pub file_path: String,
    pub line_number: u64,
    pub attributes_json: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricHistogramRecordCore {
    pub timestamp_ns: u64,
    pub name: String,
    pub unit: String,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub explicit_bounds_json: String,
    pub bucket_counts_json: String,
    pub attributes_json: String,
}

#[derive(Clone, Debug)]
pub struct ObservabilityRuntimeCore {
    metrics: VecDeque<MetricHistogramRecordCore>,
    logs: VecDeque<LogRecordEnvelopeCore>,
    retained_records: usize,
    buckets: Vec<f64>,
}

impl ObservabilityRuntimeCore {
    pub fn new(retained_records: usize) -> Result<Self, String> {
        if retained_records == 0 {
            return Err("retained_records must be positive".to_string());
        }
        Ok(Self {
            metrics: VecDeque::new(),
            logs: VecDeque::new(),
            retained_records,
            buckets: DEFAULT_METRIC_BUCKETS.to_vec(),
        })
    }

    pub fn set_buckets(&mut self, buckets: Vec<f64>) -> Result<(), String> {
        validate_buckets(&buckets)?;
        self.buckets = buckets;
        Ok(())
    }

    pub fn record_histogram(
        &mut self,
        name: String,
        value: f64,
        unit: String,
        attributes_json: String,
        timestamp_ns: Option<u64>,
    ) -> Result<MetricHistogramRecordCore, String> {
        validate_nonblank_text("metric name", &name)?;
        validate_json_object("attributes_json", &attributes_json)?;
        if !value.is_finite() {
            return Err("histogram value must be finite".to_string());
        }
        let record = MetricHistogramRecordCore {
            timestamp_ns: timestamp_ns.unwrap_or_else(current_system_time_ns),
            name,
            unit,
            count: 1,
            sum: value,
            min: value,
            max: value,
            explicit_bounds_json: json_float_array(&self.buckets),
            bucket_counts_json: json_u64_array(&bucket_counts(value, &self.buckets)),
            attributes_json,
        };
        self.metrics.push_back(record.clone());
        self.trim();
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_log(
        &mut self,
        severity_text: String,
        logger_name: String,
        body: String,
        file_path: String,
        line_number: u64,
        attributes_json: String,
        timestamp_ns: Option<u64>,
    ) -> Result<LogRecordEnvelopeCore, String> {
        validate_nonblank_text("severity_text", &severity_text)?;
        validate_nonblank_text("logger_name", &logger_name)?;
        validate_json_object("attributes_json", &attributes_json)?;
        let record = LogRecordEnvelopeCore {
            timestamp_ns: timestamp_ns.unwrap_or_else(current_system_time_ns),
            severity_text,
            logger_name,
            body,
            file_path,
            line_number,
            attributes_json,
        };
        self.logs.push_back(record.clone());
        self.trim();
        Ok(record)
    }

    pub fn record_pubsub_publish(
        &mut self,
        topic: &str,
        payload_bytes: usize,
        delivered_subscribers: usize,
    ) -> Result<(), String> {
        let attributes_json = format!("{{\"topic\":\"{}\"}}", escape_json(topic));
        self.record_histogram(
            "manyfold.pubsub.publish.payload_bytes".to_string(),
            payload_bytes as f64,
            "By".to_string(),
            attributes_json.clone(),
            None,
        )?;
        self.record_histogram(
            "manyfold.pubsub.publish.delivered_subscribers".to_string(),
            delivered_subscribers as f64,
            "{subscriber}".to_string(),
            attributes_json.clone(),
            None,
        )?;
        self.record_log(
            "INFO".to_string(),
            "manyfold.pubsub".to_string(),
            format!(
                "published topic={} payload_bytes={} delivered_subscribers={}",
                topic, payload_bytes, delivered_subscribers
            ),
            String::new(),
            0,
            attributes_json,
            None,
        )?;
        Ok(())
    }

    pub fn metrics(&self) -> Vec<MetricHistogramRecordCore> {
        self.metrics.iter().cloned().collect()
    }

    pub fn logs(&self) -> Vec<LogRecordEnvelopeCore> {
        self.logs.iter().cloned().collect()
    }

    fn trim(&mut self) {
        while self.metrics.len() > self.retained_records {
            self.metrics.pop_front();
        }
        while self.logs.len() > self.retained_records {
            self.logs.pop_front();
        }
    }
}

fn bucket_counts(value: f64, buckets: &[f64]) -> Vec<u64> {
    let mut counts = vec![0; buckets.len() + 1];
    for (index, bucket) in buckets.iter().enumerate() {
        if value <= *bucket {
            counts[index] = 1;
            return counts;
        }
    }
    *counts.last_mut().expect("non-empty bucket counts") = 1;
    counts
}

fn current_system_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn json_float_array(values: &[f64]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn json_u64_array(values: &[u64]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn validate_json_object(field: &str, value: &str) -> Result<(), String> {
    let trimmed = value.trim();
    if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
        return Err(format!("{field} must be a JSON object string"));
    }
    Ok(())
}

fn validate_nonblank_text(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must be a non-empty string"));
    }
    Ok(())
}

fn validate_buckets(buckets: &[f64]) -> Result<(), String> {
    if buckets.is_empty() {
        return Err("buckets must not be empty".to_string());
    }
    if buckets.iter().any(|bucket| !bucket.is_finite()) {
        return Err("buckets must be finite".to_string());
    }
    if !buckets.windows(2).all(|window| window[0] <= window[1]) {
        return Err("buckets must be sorted".to_string());
    }
    Ok(())
}
