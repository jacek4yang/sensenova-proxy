//! Lightweight internal counters and gauges, exposed in Prometheus text
//! format at `GET /metrics`. No metrics framework dependency.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_active: AtomicI64,
    pub requests_queued: AtomicI64,
    pub upstream_requests_total: AtomicU64,
    pub upstream_429_total: AtomicU64,
    pub upstream_5xx_total: AtomicU64,
    pub upstream_transport_errors_total: AtomicU64,
    pub stream_interruptions_total: AtomicU64,
    pub stream_protocol_errors_total: AtomicU64,
    pub retries_total: AtomicU64,
    pub tool_calls_total: AtomicU64,
    pub queue_rejections_total: AtomicU64,
    pub circuit_state: AtomicI64,
    pub concurrency_limit: AtomicU64,
    // Aggregate latency bookkeeping (count + total microseconds) for means.
    queue_wait_us_total: AtomicU64,
    queue_wait_samples: AtomicU64,
    request_duration_us_total: AtomicU64,
    request_duration_samples: AtomicU64,
    time_to_first_event_us_total: AtomicU64,
    time_to_first_event_samples: AtomicU64,
    last_failure: Mutex<Option<(String, u64)>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_requests(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.requests_active.fetch_add(1, Ordering::AcqRel);
    }

    pub fn dec_requests(&self) {
        self.requests_active.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn set_queue_depth(&self, depth: usize) {
        self.requests_queued.store(depth as i64, Ordering::Relaxed);
    }

    pub fn set_circuit_state(&self, state: i64) {
        self.circuit_state.store(state, Ordering::Relaxed);
    }

    pub fn set_concurrency_limit(&self, limit: usize) {
        self.concurrency_limit
            .store(limit as u64, Ordering::Relaxed);
    }

    pub fn note_queue_wait(&self, wait: Duration) {
        self.queue_wait_us_total
            .fetch_add(wait.as_micros() as u64, Ordering::Relaxed);
        self.queue_wait_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_request_duration(&self, duration: Duration) {
        self.request_duration_us_total
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
        self.request_duration_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_time_to_first_event(&self, duration: Duration) {
        self.time_to_first_event_us_total
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
        self.time_to_first_event_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_failure(&self, class: &str) {
        let mut slot = self
            .last_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some((
            class.to_owned(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|time| time.as_secs())
                .unwrap_or(0),
        ));
    }

    /// Render the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let (queue_wait_mean, queue_samples) = self.mean(
            self.queue_wait_us_total.load(Ordering::Relaxed),
            self.queue_wait_samples.load(Ordering::Relaxed),
        );
        let (duration_mean, duration_samples) = self.mean(
            self.request_duration_us_total.load(Ordering::Relaxed),
            self.request_duration_samples.load(Ordering::Relaxed),
        );
        let (ttft_mean, ttft_samples) = self.mean(
            self.time_to_first_event_us_total.load(Ordering::Relaxed),
            self.time_to_first_event_samples.load(Ordering::Relaxed),
        );
        let last_failure = self
            .last_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut output = String::with_capacity(2 * 1024);
        let counter = |name: &str, help: &str, value: u64, output: &mut String| {
            output.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
            ));
        };
        let gauge = |name: &str, help: &str, value: i64, output: &mut String| {
            output.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
            ));
        };
        counter(
            "sensenova_proxy_requests_total",
            "Client requests accepted.",
            self.requests_total.load(Ordering::Relaxed),
            &mut output,
        );
        gauge(
            "sensenova_proxy_requests_active",
            "Client requests currently in flight.",
            self.requests_active.load(Ordering::Relaxed),
            &mut output,
        );
        gauge(
            "sensenova_proxy_requests_queued",
            "Client requests currently waiting for a concurrency permit.",
            self.requests_queued.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_upstream_requests_total",
            "Upstream HTTP requests attempted.",
            self.upstream_requests_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_upstream_429_total",
            "Upstream responses classified as rate limited.",
            self.upstream_429_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_upstream_5xx_total",
            "Upstream 5xx responses.",
            self.upstream_5xx_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_upstream_transport_errors_total",
            "Upstream transport failures.",
            self.upstream_transport_errors_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_stream_interruptions_total",
            "Upstream streams that failed after commit.",
            self.stream_interruptions_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_stream_protocol_errors_total",
            "Upstream stream protocol violations (malformed SSE/JSON).",
            self.stream_protocol_errors_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_retries_total",
            "Upstream retry attempts performed.",
            self.retries_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_tool_calls_total",
            "tool_use blocks observed in responses.",
            self.tool_calls_total.load(Ordering::Relaxed),
            &mut output,
        );
        counter(
            "sensenova_proxy_queue_rejections_total",
            "Requests rejected by the admission queue.",
            self.queue_rejections_total.load(Ordering::Relaxed),
            &mut output,
        );
        gauge(
            "sensenova_proxy_circuit_state",
            "Circuit breaker state (0 closed, 1 half open, 2 open).",
            self.circuit_state.load(Ordering::Relaxed),
            &mut output,
        );
        gauge(
            "sensenova_proxy_concurrency_limit",
            "Configured concurrency limit.",
            self.concurrency_limit.load(Ordering::Relaxed) as i64,
            &mut output,
        );
        gauge(
            "sensenova_proxy_queue_wait_ms_mean",
            "Mean queue wait in milliseconds.",
            queue_wait_mean,
            &mut output,
        );
        gauge(
            "sensenova_proxy_queue_wait_samples",
            "Queue wait samples.",
            queue_samples as i64,
            &mut output,
        );
        gauge(
            "sensenova_proxy_request_duration_ms_mean",
            "Mean total request duration in milliseconds.",
            duration_mean,
            &mut output,
        );
        gauge(
            "sensenova_proxy_request_duration_samples",
            "Request duration samples.",
            duration_samples as i64,
            &mut output,
        );
        gauge(
            "sensenova_proxy_time_to_first_event_ms_mean",
            "Mean time to first upstream event in milliseconds.",
            ttft_mean,
            &mut output,
        );
        gauge(
            "sensenova_proxy_time_to_first_event_samples",
            "Time-to-first-event samples.",
            ttft_samples as i64,
            &mut output,
        );
        if let Some((class, at)) = last_failure {
            output.push_str(&format!(
                "# HELP sensenova_proxy_last_failure_class Class of the most recent upstream failure.\n# TYPE sensenova_proxy_last_failure_class gauge\nsensenova_proxy_last_failure_class{{class=\"{class}\"}} 1\n# HELP sensenova_proxy_last_failure_unix_seconds Unix time of the most recent upstream failure.\n# TYPE sensenova_proxy_last_failure_unix_seconds gauge\nsensenova_proxy_last_failure_unix_seconds {at}\n"
            ));
        }
        output
    }

    fn mean(&self, total_us: u64, samples: u64) -> (i64, u64) {
        if let Some(mean) = total_us.checked_div(samples).map(|value| value / 1000) {
            (mean as i64, samples)
        } else {
            (0, 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_counters_and_means() {
        let metrics = Metrics::new();
        metrics.inc_requests();
        metrics.note_queue_wait(Duration::from_millis(20));
        metrics.note_request_duration(Duration::from_millis(120));
        metrics.note_time_to_first_event(Duration::from_millis(30));
        metrics.note_failure("rate_limited");
        let rendered = metrics.render();
        assert!(rendered.contains("sensenova_proxy_requests_total 1"));
        assert!(rendered.contains("sensenova_proxy_requests_active 1"));
        assert!(rendered.contains("sensenova_proxy_queue_wait_ms_mean 20"));
        assert!(rendered.contains("sensenova_proxy_request_duration_ms_mean 120"));
        assert!(rendered.contains("sensenova_proxy_time_to_first_event_ms_mean 30"));
        assert!(rendered.contains("class=\"rate_limited\""));
    }

    #[test]
    fn means_are_zero_without_samples() {
        let rendered = Metrics::new().render();
        assert!(rendered.contains("sensenova_proxy_queue_wait_ms_mean 0"));
        assert!(rendered.contains("sensenova_proxy_queue_wait_samples 0"));
    }
}
