//! Coordinated shutdown: once the HTTP server has stopped accepting requests,
//! background workers finish the entry they are on, stop pulling new work,
//! and the audit log is flushed before the process exits.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
static INFLIGHT: AtomicUsize = AtomicUsize::new(0);

pub fn begin() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
}

pub fn is_shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::Relaxed)
}

/// Marks one unit of background work as in flight for its lifetime.
pub struct InflightGuard;

impl InflightGuard {
    pub fn new() -> Self {
        INFLIGHT.fetch_add(1, Ordering::SeqCst);
        InflightGuard
    }
}

impl Default for InflightGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        INFLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn inflight() -> usize {
    INFLIGHT.load(Ordering::SeqCst)
}

/// Wait until no background work is in flight, or `timeout` elapses.
/// Returns `true` when everything drained.
pub async fn drain(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while inflight() > 0 {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn guard_counts_and_drain_waits() {
        let g = InflightGuard::new();
        assert!(inflight() >= 1);
        assert!(!drain(Duration::from_millis(60)).await);
        drop(g);
        assert!(drain(Duration::from_millis(60)).await);
    }
}
