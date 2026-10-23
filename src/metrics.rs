//! Prometheus metrics for observability.
//!
//! Counters: requests, errors, cache, rate limit, MCP, DB timeouts.
//! Histograms: `flx_http_request_duration_seconds{route,method,status}` and
//! `flx_db_query_duration_seconds{kind}`. Gauges: DB pool size/idle/max.
//!
//! Everything is lock-free atomics; label sets are kept small on purpose
//! (route = configured entity or a fixed bucket) so a path scanner cannot
//! explode the cardinality.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use once_cell::sync::Lazy;

/// Bucket upper bounds in seconds (Prometheus `le`). Covers 1 ms .. 30 s.
pub const LATENCY_BOUNDS: [f64; 14] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Fixed-bucket histogram, safe to update from any thread without locking.
pub struct Histogram {
    buckets: [AtomicU64; LATENCY_BOUNDS.len()],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        for (i, bound) in LATENCY_BOUNDS.iter().enumerate() {
            if secs <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum_micros.fetch_add(d.as_micros() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative bucket counts (Prometheus semantics), sum in seconds, count.
    pub fn snapshot(&self) -> ([u64; LATENCY_BOUNDS.len()], f64, u64) {
        let mut b = [0u64; LATENCY_BOUNDS.len()];
        for (i, v) in self.buckets.iter().enumerate() {
            b[i] = v.load(Ordering::Relaxed);
        }
        (
            b,
            self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.count.load(Ordering::Relaxed),
        )
    }

    fn render(&self, name: &str, labels: &str, out: &mut String) {
        use std::fmt::Write;
        let (b, sum, count) = self.snapshot();
        let sep = if labels.is_empty() { "" } else { "," };
        for (i, bound) in LATENCY_BOUNDS.iter().enumerate() {
            let _ = writeln!(out, "{}_bucket{{{}{}le=\"{}\"}} {}", name, labels, sep, bound, b[i]);
        }
        let _ = writeln!(out, "{}_bucket{{{}{}le=\"+Inf\"}} {}", name, labels, sep, count);
        let _ = writeln!(out, "{}_sum{{{}}} {}", name, labels, sum);
        let _ = writeln!(out, "{}_count{{{}}} {}", name, labels, count);
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Histogram family keyed by a pre-rendered label string (`a="x",b="y"`).
pub struct HistogramFamily {
    name: &'static str,
    help: &'static str,
    series: DashMap<String, Arc<Histogram>>,
    max_series: usize,
}

impl HistogramFamily {
    pub fn new(name: &'static str, help: &'static str, max_series: usize) -> Self {
        Self { name, help, series: DashMap::new(), max_series }
    }

    /// `kind` is used as the single label `kind="..."`.
    pub fn observe(&self, kind: &str, d: Duration) {
        self.observe_labels(&format!("kind=\"{}\"", kind), d);
    }

    pub fn observe_labels(&self, labels: &str, d: Duration) {
        if let Some(h) = self.series.get(labels) {
            h.observe(d);
            return;
        }
        if self.series.len() >= self.max_series {
            // Cardinality guard: fold everything else into one overflow series.
            self.series
                .entry("overflow=\"true\"".to_string())
                .or_insert_with(|| Arc::new(Histogram::new()))
                .observe(d);
            return;
        }
        self.series
            .entry(labels.to_string())
            .or_insert_with(|| Arc::new(Histogram::new()))
            .observe(d);
    }

    pub fn render(&self, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(out, "# HELP {} {}", self.name, self.help);
        let _ = writeln!(out, "# TYPE {} histogram", self.name);
        let mut keys: Vec<String> = self.series.iter().map(|e| e.key().clone()).collect();
        keys.sort();
        for k in keys {
            if let Some(h) = self.series.get(&k) {
                h.render(self.name, &k, out);
            }
        }
        out.push('\n');
    }
}

/// Simple metrics collector for the application
pub struct AppMetrics {
    pub total_requests: AtomicU64,
    pub request_errors: AtomicU64,
    pub db_pool_active: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub rate_limit_hits: AtomicU64,
    pub db_timeouts: AtomicU64,
    /// MCP HTTP requests received on `MCP_PATH` (JSON-RPC envelopes, any method)
    pub mcp_requests: AtomicU64,
    /// MCP `tools/call` invocations
    pub mcp_tool_calls: AtomicU64,
    /// MCP `tools/call` invocations that returned `isError: true`
    pub mcp_tool_errors: AtomicU64,
    /// HTTP latency by `route`, `method`, `status`.
    pub http_duration: HistogramFamily,
    /// Database statement latency by `kind` (query, begin, tx_query, tx_execute, commit, rollback).
    pub db_duration: HistogramFamily,
}

impl AppMetrics {
    pub fn new() -> Self {
        AppMetrics {
            total_requests: AtomicU64::new(0),
            request_errors: AtomicU64::new(0),
            db_pool_active: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            rate_limit_hits: AtomicU64::new(0),
            db_timeouts: AtomicU64::new(0),
            mcp_requests: AtomicU64::new(0),
            mcp_tool_calls: AtomicU64::new(0),
            mcp_tool_errors: AtomicU64::new(0),
            http_duration: HistogramFamily::new(
                "flx_http_request_duration_seconds",
                "HTTP request latency by route, method and status",
                2_000,
            ),
            db_duration: HistogramFamily::new(
                "flx_db_query_duration_seconds",
                "Database statement latency by kind",
                16,
            ),
        }
    }

    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.request_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rate_limit_hit(&self) {
        self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_db_timeout(&self) {
        self.db_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_mcp_request(&self) {
        self.mcp_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_mcp_tool_call(&self, is_error: bool) {
        self.mcp_tool_calls.fetch_add(1, Ordering::Relaxed);
        if is_error {
            self.mcp_tool_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[allow(dead_code)]
    pub fn set_db_pool_active(&self, count: u64) {
        self.db_pool_active.store(count, Ordering::Relaxed);
    }

    /// Record one finished HTTP request.
    pub fn record_http(&self, route: &str, method: &str, status: u16, elapsed: Duration) {
        if status >= 500 {
            self.record_error();
        }
        let labels = format!("route=\"{}\",method=\"{}\",status=\"{}\"", route, method, status);
        self.http_duration.observe_labels(&labels, elapsed);
    }

    /// Get cache hit ratio (0.0 to 1.0)
    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    /// Get error rate (0.0 to 1.0)
    pub fn error_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        let errors = self.request_errors.load(Ordering::Relaxed);
        if total == 0 { 0.0 } else { errors as f64 / total as f64 }
    }

    /// Get rate limiting hit percentage
    pub fn rate_limit_percentage(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        let limited = self.rate_limit_hits.load(Ordering::Relaxed);
        if total == 0 { 0.0 } else { (limited as f64 / total as f64) * 100.0 }
    }

    /// Prometheus exposition. `pool` is the live DB pool snapshot, if any.
    pub fn to_prometheus_format(&self, pool: Option<crate::database::state::PoolStats>) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(8 * 1024);
        let mut counter = |name: &str, help: &str, v: u64| {
            let _ = writeln!(out, "# HELP {} {}\n# TYPE {} counter\n{} {}\n", name, help, name, name, v);
        };
        counter("flx_total_requests", "Total number of requests processed", self.total_requests.load(Ordering::Relaxed));
        counter("flx_request_errors", "Total number of 5xx responses", self.request_errors.load(Ordering::Relaxed));
        counter("flx_cache_hits", "Total cache hits (L1 or L2)", self.cache_hits.load(Ordering::Relaxed));
        counter("flx_cache_misses", "Total cache misses (served from the database)", self.cache_misses.load(Ordering::Relaxed));
        counter("flx_rate_limit_hits", "Total rate limit rejections", self.rate_limit_hits.load(Ordering::Relaxed));
        counter("flx_db_timeouts", "Database statements aborted by the client-side timeout", self.db_timeouts.load(Ordering::Relaxed));
        counter("flx_mcp_requests", "Total MCP HTTP requests", self.mcp_requests.load(Ordering::Relaxed));
        counter("flx_mcp_tool_calls", "Total MCP tool invocations", self.mcp_tool_calls.load(Ordering::Relaxed));
        counter("flx_mcp_tool_errors", "Total MCP tool invocations that returned an error result", self.mcp_tool_errors.load(Ordering::Relaxed));

        let mut gauge = |name: &str, help: &str, v: f64| {
            let _ = writeln!(out, "# HELP {} {}\n# TYPE {} gauge\n{} {}\n", name, help, name, name, v);
        };
        gauge("flx_cache_hit_ratio", "Cache hit ratio (0-1)", self.cache_hit_ratio());
        gauge("flx_error_rate", "Error rate (0-1)", self.error_rate());
        gauge("flx_rate_limit_percentage", "Rate limiting percentage", self.rate_limit_percentage());
        let (size, idle, max) = pool.map(|p| (p.size, p.idle, p.max)).unwrap_or((0, 0, 0));
        gauge("flx_db_pool_size", "Open database connections", size as f64);
        gauge("flx_db_pool_idle", "Idle database connections", idle as f64);
        gauge("flx_db_pool_max", "Configured maximum database connections", max as f64);
        gauge("flx_db_pool_active", "Database connections currently executing", size.saturating_sub(idle) as f64);

        self.http_duration.render(&mut out);
        self.db_duration.render(&mut out);
        out
    }
}

impl Default for AppMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// Global metrics instance
pub static METRICS: Lazy<Arc<AppMetrics>> = Lazy::new(|| Arc::new(AppMetrics::new()));

/// Request timing guard - records latency when dropped
#[allow(dead_code)]
pub struct RequestTimer {
    start: Instant,
    _name: String,
}

impl RequestTimer {
    #[allow(dead_code)]
    pub fn new(name: impl Into<String>) -> Self {
        RequestTimer { start: Instant::now(), _name: name.into() }
    }

    #[allow(dead_code)]
    pub fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        let elapsed = self.elapsed_ms();
        if elapsed > 100 {
            eprintln!("SLOW_REQUEST: {} took {}ms", self._name, elapsed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let h = Histogram::new();
        h.observe(Duration::from_millis(3)); // <= 0.005
        h.observe(Duration::from_millis(40)); // <= 0.05
        h.observe(Duration::from_secs(60)); // beyond last bound
        let (b, sum, count) = h.snapshot();
        assert_eq!(count, 3);
        assert_eq!(b[0], 0, "1ms bucket");
        assert_eq!(b[2], 1, "5ms bucket");
        assert_eq!(b[6], 2, "100ms bucket");
        assert_eq!(b[LATENCY_BOUNDS.len() - 1], 2, "30s bucket excludes the 60s sample");
        assert!((sum - 60.043).abs() < 0.001);
    }

    #[test]
    fn family_renders_prometheus_text_and_guards_cardinality() {
        let f = HistogramFamily::new("t_seconds", "test", 2);
        f.observe_labels("route=\"a\"", Duration::from_millis(1));
        f.observe_labels("route=\"b\"", Duration::from_millis(1));
        f.observe_labels("route=\"c\"", Duration::from_millis(1));
        let mut out = String::new();
        f.render(&mut out);
        assert!(out.contains("# TYPE t_seconds histogram"));
        assert!(out.contains("t_seconds_bucket{route=\"a\",le=\"0.001\"} 1"));
        assert!(out.contains("t_seconds_bucket{route=\"a\",le=\"+Inf\"} 1"));
        assert!(out.contains("t_seconds_count{route=\"a\"} 1"));
        assert!(out.contains("overflow=\"true\""), "third series folds into overflow");
        assert!(!out.contains("route=\"c\""));
    }

    #[test]
    fn exposition_includes_pool_gauges() {
        let m = AppMetrics::new();
        m.record_http("orders", "GET", 200, Duration::from_millis(2));
        m.record_http("orders", "GET", 500, Duration::from_millis(2));
        let txt = m.to_prometheus_format(Some(crate::database::state::PoolStats { size: 4, idle: 1, max: 8 }));
        assert!(txt.contains("flx_db_pool_active 3"));
        assert!(txt.contains("flx_request_errors 1"));
        assert!(txt.contains("flx_http_request_duration_seconds_count{route=\"orders\",method=\"GET\",status=\"200\"} 1"));
    }
}
