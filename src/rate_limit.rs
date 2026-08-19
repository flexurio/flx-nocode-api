use once_cell::sync::Lazy;
use ahash::AHashMap;
use parking_lot::Mutex;
use std::time::{Duration, Instant};
use std::sync::Arc;

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
        let mut map = self.inner.lock();

        // Evict expired entries when capacity is getting high
        if map.len() >= self.max_keys && !map.contains_key(key) {
            let window = self.window;
            map.retain(|_, e| now.duration_since(e.window_start) < window);
            // If still at capacity after evicting expired entries, drop oldest key
            if map.len() >= self.max_keys
                && let Some(old_key) = map
                    .iter()
                    .min_by_key(|(_, e)| e.window_start)
                    .map(|(k, _)| k.clone())
            {
                map.remove(&old_key);
            }
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

// ----------------------------------------------------------------------------
// Rate limit key prefix cache
// ----------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum RateOp { Get, Post, Put, Patch, Delete, Trace, Import }

impl RateOp { #[inline] fn as_str(self) -> &'static str { match self { RateOp::Get=>"get", RateOp::Post=>"post", RateOp::Put=>"put", RateOp::Patch=>"patch", RateOp::Delete=>"delete", RateOp::Trace=>"trace", RateOp::Import=>"import"} } }

static PREFIX_CACHE: Lazy<Mutex<AHashMap<String, Arc<String>>>> = Lazy::new(|| Mutex::new(AHashMap::with_capacity(256)));

#[inline]
fn cache_key(op: RateOp, route: &str) -> String {
    if route.is_empty() { op.as_str().to_string() } else { let op_s=op.as_str(); let mut s=String::with_capacity(op_s.len()+route.len()+1); s.push_str(op_s); s.push('|'); s.push_str(route); s }
}

pub fn prefix(op: RateOp, route: &str) -> Arc<String> {
    let ck = cache_key(op, route);
    {
        let map = PREFIX_CACHE.lock();
        if let Some(p) = map.get(&ck) { return p.clone(); }
    }
    let mut base = String::new();
    base.push_str(op.as_str());
    base.push(':');
    if !route.is_empty() {
        base.push_str(route);
        base.push(':');
    }
    let arc = Arc::new(base);
    let mut map = PREFIX_CACHE.lock();
    map.insert(ck, arc.clone());
    arc
}

#[inline]
pub fn build_key(op: RateOp, route: &str, suffix: &str) -> String {
    let p = prefix(op, route);
    let mut s = String::with_capacity(p.len() + suffix.len());
    s.push_str(&p);
    s.push_str(suffix);
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    // --- RateLimiter ---

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let rl = RateLimiter::new(60);
        assert!(rl.check_and_increment("key1", 5));
        assert!(rl.check_and_increment("key1", 5));
        assert!(rl.check_and_increment("key1", 5));
    }

    #[test]
    fn test_rate_limiter_blocks_after_limit_exceeded() {
        let rl = RateLimiter::new(60);
        // Fill exactly to limit
        for _ in 0..3 {
            assert!(rl.check_and_increment("block_key", 3));
        }
        // Next call should be blocked
        assert!(!rl.check_and_increment("block_key", 3), "Should be rate-limited after exceeding limit");
    }

    #[test]
    fn test_rate_limiter_different_keys_are_independent() {
        let rl = RateLimiter::new(60);
        for _ in 0..3 {
            rl.check_and_increment("key_a", 3);
        }
        // key_b is a different key — should still be allowed
        assert!(rl.check_and_increment("key_b", 3), "Different key should have independent counter");
    }

    #[test]
    fn test_rate_limiter_limit_of_one() {
        let rl = RateLimiter::new(60);
        assert!(rl.check_and_increment("single", 1));
        assert!(!rl.check_and_increment("single", 1), "Second call should be blocked for limit=1");
    }

    #[test]
    fn test_rate_limiter_window_reset_after_expiry() {
        let rl = RateLimiter::new(1); // 1-second window
        for _ in 0..2 {
            rl.check_and_increment("win_key", 2);
        }
        assert!(!rl.check_and_increment("win_key", 2), "Should be blocked before window resets");
        thread::sleep(Duration::from_millis(1100));
        assert!(rl.check_and_increment("win_key", 2), "Should be allowed after window resets");
    }

    // --- prefix and build_key ---

    #[test]
    fn test_prefix_contains_op_and_route() {
        let p = prefix(RateOp::Get, "users");
        assert!(p.starts_with("get:"), "Prefix should start with op name");
        assert!(p.contains("users"), "Prefix should contain route");
    }

    #[test]
    fn test_prefix_empty_route() {
        let p = prefix(RateOp::Get, "");
        assert_eq!(p.as_str(), "get:");
    }

    #[test]
    fn test_prefix_all_ops() {
        let ops = [
            (RateOp::Get, "get"),
            (RateOp::Post, "post"),
            (RateOp::Put, "put"),
            (RateOp::Patch, "patch"),
            (RateOp::Delete, "delete"),
            (RateOp::Trace, "trace"),
            (RateOp::Import, "import"),
        ];
        for (op, expected_prefix) in ops {
            let p = prefix(op, "test_route");
            assert!(p.starts_with(expected_prefix), "Op {:?} prefix should start with {}", op, expected_prefix);
        }
    }

    #[test]
    fn test_prefix_caching_returns_same_arc() {
        let p1 = prefix(RateOp::Delete, "items");
        let p2 = prefix(RateOp::Delete, "items");
        assert!(Arc::ptr_eq(&p1, &p2), "Cached prefix should return the same Arc");
    }

    #[test]
    fn test_build_key_format() {
        let key = build_key(RateOp::Post, "orders", "127.0.0.1");
        assert!(key.starts_with("post:orders:"), "Build key should have op:route: prefix");
        assert!(key.ends_with("127.0.0.1"), "Build key should end with suffix");
    }

    #[test]
    fn test_build_key_empty_route() {
        let key = build_key(RateOp::Get, "", "192.168.1.1");
        assert!(key.starts_with("get:"));
        assert!(key.ends_with("192.168.1.1"));
    }
}
