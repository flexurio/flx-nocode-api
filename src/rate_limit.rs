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
    max_keys: usize,
}

impl RateLimiter {
    pub fn new(window_secs: u64) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            window: Duration::from_secs(window_secs),
            max_keys: 10_000,
        }
    }

    /// Returns true if allowed, false if rate-limited.
    pub fn check_and_increment(&self, key: &str, limit: u32) -> bool {
        let mut map = self.inner.lock().unwrap();
        let now = Instant::now();
        if !map.contains_key(key) && map.len() >= self.max_keys {
            // Drop oldest-ish key to keep memory bounded (simple O(n) scan)
            if let Some((old_key, _)) = map
                .iter()
                .min_by_key(|(_, e)| e.window_start)
                .map(|(k, v)| (k.clone(), v.window_start))
            {
                map.remove(&old_key);
            }
        }
        let entry = map.entry(key.to_string()).or_insert(Entry {
            count: 0,
            window_start: now,
        });

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
pub static RL_WINDOW_MUTATE: Lazy<RateLimiter> = Lazy::new(|| RateLimiter::new(1));
pub static RL_WINDOW_GET: Lazy<RateLimiter> = Lazy::new(|| RateLimiter::new(1));
