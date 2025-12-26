// Prometheus metrics for observability
// Tracks: request latency, DB pool usage, cache hit ratio, rate limiter stats

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use once_cell::sync::Lazy;
use std::time::Instant;

/// Simple metrics collector for the application
pub struct AppMetrics {
    pub total_requests: AtomicU64,
    pub request_errors: AtomicU64,
    pub db_pool_active: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub rate_limit_hits: AtomicU64,
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
        }
    }

    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_error(&self) {
        self.request_errors.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rate_limit_hit(&self) {
        self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn set_db_pool_active(&self, count: u64) {
        self.db_pool_active.store(count, Ordering::Relaxed);
    }

    /// Get cache hit ratio (0.0 to 1.0)
    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get error rate (0.0 to 1.0)
    pub fn error_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        let errors = self.request_errors.load(Ordering::Relaxed);
        
        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }

    /// Get rate limiting hit percentage
    pub fn rate_limit_percentage(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        let limited = self.rate_limit_hits.load(Ordering::Relaxed);
        
        if total == 0 {
            0.0
        } else {
            (limited as f64 / total as f64) * 100.0
        }
    }

    /// Get Prometheus-format metrics output
    pub fn to_prometheus_format(&self) -> String {
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let request_errors = self.request_errors.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let rate_limit_hits = self.rate_limit_hits.load(Ordering::Relaxed);
        let db_pool_active = self.db_pool_active.load(Ordering::Relaxed);

        format!(
            "# HELP flx_total_requests Total number of requests processed\n\
             # TYPE flx_total_requests counter\n\
             flx_total_requests {}\n\
             \n\
             # HELP flx_request_errors Total number of request errors\n\
             # TYPE flx_request_errors counter\n\
             flx_request_errors {}\n\
             \n\
             # HELP flx_cache_hits Total cache hits\n\
             # TYPE flx_cache_hits counter\n\
             flx_cache_hits {}\n\
             \n\
             # HELP flx_cache_misses Total cache misses\n\
             # TYPE flx_cache_misses counter\n\
             flx_cache_misses {}\n\
             \n\
             # HELP flx_cache_hit_ratio Cache hit ratio (0-1)\n\
             # TYPE flx_cache_hit_ratio gauge\n\
             flx_cache_hit_ratio {}\n\
             \n\
             # HELP flx_error_rate Error rate (0-1)\n\
             # TYPE flx_error_rate gauge\n\
             flx_error_rate {}\n\
             \n\
             # HELP flx_rate_limit_hits Total rate limit rejections\n\
             # TYPE flx_rate_limit_hits counter\n\
             flx_rate_limit_hits {}\n\
             \n\
             # HELP flx_rate_limit_percentage Rate limiting percentage\n\
             # TYPE flx_rate_limit_percentage gauge\n\
             flx_rate_limit_percentage {}\n\
             \n\
             # HELP flx_db_pool_active Active database connections\n\
             # TYPE flx_db_pool_active gauge\n\
             flx_db_pool_active {}\n",
            total_requests,
            request_errors,
            cache_hits,
            cache_misses,
            self.cache_hit_ratio(),
            self.error_rate(),
            rate_limit_hits,
            self.rate_limit_percentage(),
            db_pool_active
        )
    }
}

// Global metrics instance
pub static METRICS: Lazy<Arc<AppMetrics>> = Lazy::new(|| {
    Arc::new(AppMetrics::new())
});

/// Request timing guard - records latency when dropped
#[allow(dead_code)]
pub struct RequestTimer {
    start: Instant,
    _name: String,
}

impl RequestTimer {
    #[allow(dead_code)]
    pub fn new(name: impl Into<String>) -> Self {
        RequestTimer {
            start: Instant::now(),
            _name: name.into(),
        }
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
            // Log slow requests (> 100ms)
            eprintln!("SLOW_REQUEST: {} took {}ms", self._name, elapsed);
        }
    }
}
