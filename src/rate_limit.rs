use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct Entry {
    count: u32,
    window_start: Instant,
}

pub struct RateLimiter {
    inner: Mutex<HashMap<String, Entry>>, // key -> entry
    window: Duration,
}

impl RateLimiter {
    pub fn new(window_secs: u64) -> Self {
        Self { inner: Mutex::new(HashMap::new()), window: Duration::from_secs(window_secs) }
    }

    /// Returns true if allowed, false if rate-limited.
    pub fn check_and_increment(&self, key: &str, limit: u32) -> bool {
        let mut map = self.inner.lock().unwrap();
        let now = Instant::now();
        let entry = map.entry(key.to_string()).or_insert(Entry { count: 0, window_start: now });

        if now.duration_since(entry.window_start) >= self.window {
            entry.count = 0;
            entry.window_start = now;
        }

        if entry.count >= limit {
            return false;
        }
        entry.count += 1;
        true
    }
}

pub static RL_WINDOW_LOGIN: Lazy<RateLimiter> = Lazy::new(|| RateLimiter::new(60));
pub static RL_WINDOW_MUTATE: Lazy<RateLimiter> = Lazy::new(|| RateLimiter::new(60));
