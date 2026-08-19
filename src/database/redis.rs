use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, IntoConnectionInfo};
use std::env;
use std::sync::Arc;

// Use connection pool instead of single ConnectionManager for better concurrency
static REDIS_CLIENT: OnceCell<Arc<Client>> = OnceCell::new();

// A `MultiplexedConnection` is designed to be cheaply cloned and shared across
// concurrent callers (it multiplexes all requests over one underlying TCP
// connection via an internal task), so we initialize it once and clone it on
// every call instead of paying a fresh TCP connect + handshake per operation.
static REDIS_CONN: tokio::sync::OnceCell<MultiplexedConnection> = tokio::sync::OnceCell::const_new();

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
    let _ = dotenv::dotenv();

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

// Returns a clone of the shared multiplexed connection, establishing it once
// on first use. Cloning is cheap (shares the same underlying TCP connection).
async fn get_connection() -> Result<MultiplexedConnection> {
    let conn = REDIS_CONN
        .get_or_try_init(|| async {
            let client = get_manager().await?;
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| anyhow!("Failed to get Redis connection: {}", e))
        })
        .await?;
    Ok(conn.clone())
}

/// Set a string value by key with optional TTL seconds (None -> persist)
pub async fn redis_set(key: &str, value: &str, ttl_secs: Option<usize>) -> Result<()> {
    let mut conn = get_connection().await?;
    if let Some(ttl) = ttl_secs {
        let mut pipe = redis::pipe();
        pipe.set(key, value).ignore().expire(key, ttl as i64);
        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow!("Redis SET/EXPIRE failed: {}", e))?;
    } else {
        let _: () = conn
            .set(key, value)
            .await
            .map_err(|e| anyhow!("Redis SET failed: {}", e))?;
    }
    Ok(())
}

/// Get a string value by key. Returns Ok(None) if missing.
pub async fn redis_get(key: &str) -> Result<Option<String>> {
    let mut conn = get_connection().await?;
    let val: Option<String> = conn
        .get(key)
        .await
        .map_err(|e| anyhow!("Redis GET failed: {}", e))?;
    Ok(val)
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
