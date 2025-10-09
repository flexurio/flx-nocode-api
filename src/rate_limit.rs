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
