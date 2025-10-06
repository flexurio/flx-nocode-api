use once_cell::sync::Lazy;
use ahash::AHashMap;
use parking_lot::Mutex;
use std::time::{Duration, Instant};

struct Entry {
    count: u32,
    window_start: Instant,
}

pub struct RateLimiter {
    inner: Mutex<AHashMap<String, Entry>>, // key -> entry (using faster AHashMap)
    window: Duration,
    max_keys: usize,
}

impl RateLimiter {
    pub fn new(window_secs: u64) -> Self {
        Self {
            inner: Mutex::new(AHashMap::with_capacity(1000)), // Pre-allocate capacity
            window: Duration::from_secs(window_secs),
            max_keys: 10_000,
        }
    }

    /// Returns true if allowed, false if rate-limited.
    pub fn check_and_increment(&self, key: &str, limit: u32) -> bool {
        let now = Instant::now();
        
        // Phase 1: Quick check if cleanup is needed (minimize lock time)
        let needs_cleanup = {
            let map = self.inner.lock();
            !map.contains_key(key) && map.len() >= self.max_keys
        };
        
        // Phase 2: Find key to remove if needed (outside main lock)
        let old_key = if needs_cleanup {
            let map = self.inner.lock();
            map.iter()
                .min_by_key(|(_, e)| e.window_start)
                .map(|(k, _)| k.clone())
        } else {
            None
        };
        
        // Phase 3: Update with minimal lock time
        let mut map = self.inner.lock();
        
        if let Some(k) = old_key {
            map.remove(&k);
        }
        
        let entry = map.entry(key.to_string()).or_insert(Entry {
            count: 0,
            window_start: now,
        });

        if now.duration_since(entry.window_start) >= self.window {
            entry.count = 1;
            entry.window_start = now;
            return true;
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
// Window used for login failure tracking (e.g., 5 minutes)
pub static RL_WINDOW_LOGIN_FAIL: Lazy<RateLimiter> = Lazy::new(|| RateLimiter::new(300));
