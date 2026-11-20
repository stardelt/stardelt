//! The operator's Prometheus text (stardelt-operator#607 item 6).
//!
//! A port of its own (`METRICS_ADDR`, default `0.0.0.0:8082`), never an ingress: the
//! chart exposes it as a ClusterIP Service with `prometheus.io/*` annotations. One
//! connection, one request, `GET /metrics` only. Hand-rolled because it is 40 lines and
//! an HTTP server dependency would be a second one to keep licensed and patched.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static START: OnceLock<SystemTime> = OnceLock::new();
static LEADER: AtomicBool = AtomicBool::new(false);
static RECONCILE_ERRORS: AtomicU64 = AtomicU64::new(0);

const BUCKETS: [f64; 7] = [0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];

#[derive(Default)]
struct Series {
    ok: u64,
    error: u64,
    buckets: [u64; 7],
    sum: f64,
    count: u64,
}

static RECONCILES: Mutex<BTreeMap<&'static str, Series>> = Mutex::new(BTreeMap::new());
static STATUS_WRITES: Mutex<BTreeMap<&'static str, u64>> = Mutex::new(BTreeMap::new());

/// The process start time. Called once, first thing in `run_controller`.
pub fn init() {
    START.get_or_init(SystemTime::now);
}

/// Whether this replica holds the leader lease (operator#612). `false` without a
/// `POD_NAME` (a local `cargo run`) — there is no leader then.
pub fn set_leader(is_leader: bool) {
    LEADER.store(is_leader, Ordering::Relaxed);
}

/// One failed reconcile, from [`crate::controller::error_policy`].
pub fn record_reconcile_error() {
    RECONCILE_ERRORS.fetch_add(1, Ordering::Relaxed);
}

/// One finished reconcile: its result and how long it took, under `controller`.
pub fn record_reconcile(controller: &'static str, ok: bool, took: Duration) {
    let seconds = took.as_secs_f64();
    let mut map = RECONCILES.lock().unwrap_or_else(|error| error.into_inner());
    let series = map.entry(controller).or_default();
    if ok {
        series.ok += 1;
    } else {
        series.error += 1;
    }
    for (slot, bound) in series.buckets.iter_mut().zip(BUCKETS) {
        if bound >= seconds {
            *slot += 1;
        }
    }
    series.sum += seconds;
    series.count += 1;
}

/// One status patch, from the controller that wrote it. Rule 1 of #274: a steady-state
/// reconcile writes none, so this counter staying flat is what "quiet" means.
pub fn record_status_write(controller: &'static str) {
    let mut map = STATUS_WRITES
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    *map.entry(controller).or_default() += 1;
}

/// Runs one reconcile and records its result and duration under `controller`.
pub async fn timed<T, E>(
    controller: &'static str,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let start = std::time::Instant::now();
    let result = work.await;
    record_reconcile(controller, result.is_ok(), start.elapsed());
    result
}

fn seconds_between(from: SystemTime, to: SystemTime) -> f64 {
    to.duration_since(from)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64()
}

pub fn render() -> String {
    let start = START.get().copied().unwrap_or_else(SystemTime::now);
    let mut out = String::new();
    out.push_str("# HELP stardelt_operator_build_info Build information.\n# TYPE stardelt_operator_build_info gauge\n");
    out.push_str(&format!(
        "stardelt_operator_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    ));
    out.push_str("# HELP stardelt_operator_process_start_time_seconds Start time of the process since the Unix epoch.\n# TYPE stardelt_operator_process_start_time_seconds gauge\n");
    out.push_str(&format!(
        "stardelt_operator_process_start_time_seconds {:.3}\n",
        start
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs_f64()
    ));
    out.push_str("# HELP stardelt_operator_uptime_seconds Seconds since this process started.\n# TYPE stardelt_operator_uptime_seconds gauge\n");
    out.push_str(&format!(
        "stardelt_operator_uptime_seconds {:.3}\n",
        seconds_between(start, SystemTime::now())
    ));
    out.push_str("# HELP stardelt_operator_leader 1 when this replica holds the leader lease.\n# TYPE stardelt_operator_leader gauge\n");
    out.push_str(&format!(
        "stardelt_operator_leader {}\n",
        u8::from(LEADER.load(Ordering::Relaxed))
    ));
    out.push_str("# HELP stardelt_operator_reconcile_errors_total Failed reconciles since start.\n# TYPE stardelt_operator_reconcile_errors_total counter\n");
    out.push_str(&format!(
        "stardelt_operator_reconcile_errors_total {}\n",
        RECONCILE_ERRORS.load(Ordering::Relaxed)
    ));
    let reconciles = RECONCILES.lock().unwrap_or_else(|error| error.into_inner());
    out.push_str("# HELP stardelt_operator_reconciles_total Reconciles since start, by controller and result.\n# TYPE stardelt_operator_reconciles_total counter\n");
    for (controller, series) in reconciles.iter() {
        out.push_str(&format!(
            "stardelt_operator_reconciles_total{{controller=\"{controller}\",result=\"ok\"}} {}\n",
            series.ok
        ));
        out.push_str(&format!(
            "stardelt_operator_reconciles_total{{controller=\"{controller}\",result=\"error\"}} {}\n",
            series.error
        ));
    }
    out.push_str("# HELP stardelt_operator_reconcile_duration_seconds Seconds one reconcile took, by controller.\n# TYPE stardelt_operator_reconcile_duration_seconds histogram\n");
    for (controller, series) in reconciles.iter() {
        for (count, bound) in series.buckets.iter().zip(BUCKETS) {
            out.push_str(&format!(
                "stardelt_operator_reconcile_duration_seconds_bucket{{controller=\"{controller}\",le=\"{bound}\"}} {count}\n"
            ));
        }
        out.push_str(&format!(
            "stardelt_operator_reconcile_duration_seconds_bucket{{controller=\"{controller}\",le=\"+Inf\"}} {}\n",
            series.count
        ));
        out.push_str(&format!(
            "stardelt_operator_reconcile_duration_seconds_sum{{controller=\"{controller}\"}} {:.6}\n",
            series.sum
        ));
        out.push_str(&format!(
            "stardelt_operator_reconcile_duration_seconds_count{{controller=\"{controller}\"}} {}\n",
            series.count
        ));
    }
    let status_writes = STATUS_WRITES
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let mut controllers: BTreeSet<&'static str> = reconciles.keys().copied().collect();
    controllers.extend(status_writes.keys().copied());
    out.push_str("# HELP stardelt_operator_status_writes_total Status patches written, by controller.\n# TYPE stardelt_operator_status_writes_total counter\n");
    for controller in controllers {
        out.push_str(&format!(
            "stardelt_operator_status_writes_total{{controller=\"{controller}\"}} {}\n",
            status_writes.get(controller).copied().unwrap_or_default()
        ));
    }
    out
}

/// `(status, content-type, body)` for one request line. Pure, so the test needs no socket.
pub fn response_for(request_line: &str) -> (u16, &'static str, String) {
    let mut parts = request_line.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some("GET"), Some("/metrics")) => {
            (200, "text/plain; version=0.0.4; charset=utf-8", render())
        }
        (Some("GET"), _) => (404, "text/plain; charset=utf-8", "not found\n".to_string()),
        _ => (
            405,
            "text/plain; charset=utf-8",
            "method not allowed\n".to_string(),
        ),
    }
}

/// Serve until the process exits. One request per connection; the body is 8 KiB max.
pub async fn serve(addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr, "metrics listening");
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buffer = [0u8; 8192];
            let Ok(read) = socket.read(&mut buffer).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buffer[..read]);
            let (status, content_type, body) = response_for(request.lines().next().unwrap_or(""));
            let reason = if status == 200 { "OK" } else { "Error" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_text_has_the_series_a_scraper_needs() {
        init();
        set_leader(true);
        record_reconcile_error();
        let text = render();
        assert!(
            text.contains(&format!(
                "stardelt_operator_build_info{{version=\"{}\"}} 1",
                env!("CARGO_PKG_VERSION")
            )),
            "{text}"
        );
        assert!(
            text.contains("# TYPE stardelt_operator_leader gauge"),
            "{text}"
        );
        assert!(text.contains("stardelt_operator_leader 1"), "{text}");
        assert!(
            text.contains("stardelt_operator_reconcile_errors_total 1"),
            "{text}"
        );
        assert!(
            text.contains("stardelt_operator_process_start_time_seconds "),
            "{text}"
        );
        set_leader(false);
    }

    #[test]
    fn reconciles_are_counted_per_controller_with_their_duration() {
        record_reconcile("test_counted", true, Duration::from_millis(20));
        record_reconcile("test_counted", true, Duration::from_millis(20));
        record_reconcile("test_counted", false, Duration::from_secs(2));
        let text = render();
        for line in [
            "stardelt_operator_reconciles_total{controller=\"test_counted\",result=\"ok\"} 2",
            "stardelt_operator_reconciles_total{controller=\"test_counted\",result=\"error\"} 1",
            "stardelt_operator_reconcile_duration_seconds_bucket{controller=\"test_counted\",le=\"0.01\"} 0",
            "stardelt_operator_reconcile_duration_seconds_bucket{controller=\"test_counted\",le=\"0.05\"} 2",
            "stardelt_operator_reconcile_duration_seconds_bucket{controller=\"test_counted\",le=\"5\"} 3",
            "stardelt_operator_reconcile_duration_seconds_bucket{controller=\"test_counted\",le=\"+Inf\"} 3",
            "stardelt_operator_reconcile_duration_seconds_count{controller=\"test_counted\"} 3",
            "# TYPE stardelt_operator_reconcile_duration_seconds histogram",
        ] {
            assert!(text.contains(line), "missing {line:?} in:\n{text}");
        }
    }

    #[tokio::test]
    async fn timed_records_an_error_result() {
        let result: Result<(), &str> = timed("test_timed", async { Err("no") }).await;
        assert!(result.is_err());
        assert!(
            render().contains(
                "stardelt_operator_reconciles_total{controller=\"test_timed\",result=\"error\"} 1"
            ),
            "{}",
            render()
        );
    }

    #[test]
    fn a_status_write_is_counted_by_controller() {
        record_status_write("test_status_writes");
        record_status_write("test_status_writes");
        let text = render();
        assert!(
            text.contains(
                "stardelt_operator_status_writes_total{controller=\"test_status_writes\"} 2"
            ),
            "{text}"
        );
        assert!(
            text.contains("# TYPE stardelt_operator_status_writes_total counter"),
            "{text}"
        );
    }

    #[test]
    fn a_controller_with_no_write_reports_zero() {
        record_reconcile("test_quiet", true, Duration::ZERO);
        let text = render();
        assert!(
            text.contains("stardelt_operator_status_writes_total{controller=\"test_quiet\"} 0"),
            "{text}"
        );
    }

    #[test]
    fn only_a_metrics_get_is_answered() {
        assert_eq!(response_for("GET /metrics HTTP/1.1").0, 200);
        assert_eq!(response_for("GET /health HTTP/1.1").0, 404);
        assert_eq!(response_for("POST /metrics HTTP/1.1").0, 405);
        assert!(
            response_for("GET /metrics HTTP/1.1")
                .2
                .contains("stardelt_operator_build_info")
        );
    }
}
