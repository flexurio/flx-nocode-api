use anyhow::{anyhow, Result};
use once_cell::sync::{Lazy, OnceCell};
use redis::{aio::ConnectionManager, AsyncCommands, Client, IntoConnectionInfo};
use std::env;
use std::sync::Arc;
use std::time::Duration;

static REDIS_CLIENT: OnceCell<Arc<Client>> = OnceCell::new();

// A `ConnectionManager` multiplexes all callers over one TCP connection (cheap
// to clone) AND transparently reconnects with backoff when the socket breaks.
// The previous `MultiplexedConnection` never reconnected: once Redis restarted,
// every cache operation failed for the rest of the process lifetime.
//
// NOTE: never run blocking commands (BRPOP/BLMOVE) on this shared connection;
// they would stall every other caller. Queue workers open their own connection.
static REDIS_CONN: tokio::sync::OnceCell<ConnectionManager> = tokio::sync::OnceCell::const_new();

/// Upper bound for a single read-cache operation (GET/SET). A stalled Redis
/// must degrade to "cache miss", never hold an HTTP request hostage.
static CACHE_OP_TIMEOUT: Lazy<Duration> = Lazy::new(|| {
    let ms: u64 = env::var("REDIS_CACHE_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    Duration::from_millis(ms.max(5))
});

/// Timeout for establishing the shared connection on first use.
static CONNECT_TIMEOUT: Lazy<Duration> = Lazy::new(|| {
    let ms: u64 = env::var("REDIS_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    Duration::from_millis(ms.max(50))
});

/// Sanitize a key component to allow only safe characters
fn sanitize_key_component(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ':' | '_' | '-' | '.'))
        .collect()
}

/// Build a consistent Redis key prefix per tenant and route.
/// Example: "flx:tenantA:products"
pub fn build_key_prefix(tenant: &str, route: &str) -> String {
    let t = sanitize_key_component(tenant);
    let r = sanitize_key_component(route);
    format!("flx:{}:{}", t, r)
}

fn build_redis_connection_url() -> Result<String> {
    // Read env with sensible defaults
    let host = env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".into());
    let port = env::var("REDIS_PORT").unwrap_or_else(|_| "6379".into());
    let password = env::var("REDIS_PASSWORD").unwrap_or_default();
    let db = env::var("REDIS_DB").unwrap_or_else(|_| "0".into());

    // Build URL: redis://[:password@]host:port/db
    let auth_part = if password.is_empty() {
        "".to_string()
    } else {
        format!(":{}@", urlencoding::encode(&password))
    };
    Ok(format!("redis://{}{}:{}/{}", auth_part, host, port, db))
}

pub(crate) async fn get_manager() -> Result<Arc<Client>> {
    if let Some(client) = REDIS_CLIENT.get() {
        return Ok(client.clone());
    }

    // Ensure .env is loaded (no-op if already loaded)
    unsafe {
        let _ = dotenv::EnvLoader::new().load_and_modify();
    }

    let url = build_redis_connection_url()?;
    let info = url
        .as_str()
        .into_connection_info()
        .map_err(|e| anyhow!("Invalid Redis URL: {}", e))?;
    let client = Client::open(info).map_err(|e| anyhow!("Create Redis client failed: {}", e))?;
    
    // Test connection
    let mut test_conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| anyhow!("Connect Redis failed: {}", e))?;
    let _: String = redis::cmd("PING")
        .query_async(&mut test_conn)
        .await
        .map_err(|e| anyhow!("Redis PING failed: {}", e))?;

    let arc_client = Arc::new(client);
    REDIS_CLIENT
        .set(arc_client.clone())
        .map_err(|_| anyhow!("Redis client already initialized"))?;
    Ok(arc_client)
}

/// Returns a clone of the shared, auto-reconnecting connection, establishing it
/// once on first use. Cloning is cheap (shares the same underlying TCP socket).
pub(crate) async fn get_connection() -> Result<ConnectionManager> {
    let conn = REDIS_CONN
        .get_or_try_init(|| async {
            let client = get_manager().await?;
            tokio::time::timeout(*CONNECT_TIMEOUT, client.get_connection_manager())
                .await
                .map_err(|_| anyhow!("Redis connect timed out after {:?}", *CONNECT_TIMEOUT))?
                .map_err(|e| anyhow!("Failed to get Redis connection: {}", e))
        })
        .await?;
    Ok(conn.clone())
}

/// Set a string value by key with optional TTL seconds (None -> persist).
/// Bounded by `REDIS_CACHE_TIMEOUT_MS` so a stalled Redis cannot block a request.
pub async fn redis_set(key: &str, value: &str, ttl_secs: Option<usize>) -> Result<()> {
    let fut = async {
        let mut conn = get_connection().await?;
        if let Some(ttl) = ttl_secs {
            // Single round-trip: SET key value EX ttl
            let _: () = conn
                .set_ex(key, value, ttl as u64)
                .await
                .map_err(|e| anyhow!("Redis SET EX failed: {}", e))?;
        } else {
            let _: () = conn
                .set(key, value)
                .await
                .map_err(|e| anyhow!("Redis SET failed: {}", e))?;
        }
        Ok(())
    };
    tokio::time::timeout(*CACHE_OP_TIMEOUT, fut)
        .await
        .map_err(|_| anyhow!("Redis SET timed out after {:?}", *CACHE_OP_TIMEOUT))?
}

/// Get a string value by key. Returns Ok(None) if missing.
/// Bounded by `REDIS_CACHE_TIMEOUT_MS`; a timeout is reported as an error so the
/// caller falls back to the database.
pub async fn redis_get(key: &str) -> Result<Option<String>> {
    let fut = async {
        let mut conn = get_connection().await?;
        let val: Option<String> = conn
            .get(key)
            .await
            .map_err(|e| anyhow!("Redis GET failed: {}", e))?;
        Ok(val)
    };
    tokio::time::timeout(*CACHE_OP_TIMEOUT, fut)
        .await
        .map_err(|_| anyhow!("Redis GET timed out after {:?}", *CACHE_OP_TIMEOUT))?
}

/// Convenience: set JSON value by key (stored as string)
pub async fn redis_set_json<T: serde::Serialize>(key: &str, value: &T, ttl_secs: Option<usize>) -> Result<()> {
    let s = serde_json::to_string(value)?;
    redis_set(key, &s, ttl_secs).await
}

/// Convenience: get JSON value by key
pub async fn redis_get_json<T: serde::de::DeserializeOwned>(key: &str) -> Result<Option<T>> {
    match redis_get(key).await? {
        Some(s) => Ok(Some(serde_json::from_str::<T>(&s)?)),
        None => Ok(None),
    }
}

/// Delete all keys matching the given prefix (prefix*) using SCAN + UNLINK.
/// Unlike KEYS, SCAN walks the keyspace incrementally without blocking the
/// Redis server for the duration of the scan on large datasets.
/// Returns the number of keys deleted.
pub async fn redis_delete_by_prefix(prefix: &str) -> Result<usize> {
    let pattern = format!("{}*", prefix);
    let mut conn = get_connection().await?;
    let mut cursor: u64 = 0;
    let mut total_deleted: usize = 0;
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow!("Redis SCAN failed: {}", e))?;

        if !keys.is_empty() {
            let deleted: i64 = redis::cmd("UNLINK")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(|e| anyhow!("Redis UNLINK failed: {}", e))?;
            total_deleted += deleted.max(0) as usize;
        }

        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }
    Ok(total_deleted)
}
