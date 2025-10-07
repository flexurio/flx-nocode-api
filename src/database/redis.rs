use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use redis::{aio::ConnectionManager, AsyncCommands, Client, IntoConnectionInfo};
use std::env;

static REDIS_MANAGER: OnceCell<ConnectionManager> = OnceCell::new();

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

async fn get_manager() -> Result<&'static ConnectionManager> {
    if let Some(mgr) = REDIS_MANAGER.get() {
        return Ok(mgr);
    }

    // Ensure .env is loaded (no-op if already loaded)
    let _ = dotenv::dotenv();

    let url = build_redis_connection_url()?;
    let info = url
        .as_str()
        .into_connection_info()
        .map_err(|e| anyhow!("Invalid Redis URL: {}", e))?;
    let client = Client::open(info).map_err(|e| anyhow!("Create Redis client failed: {}", e))?;
    let conn = client
        .get_connection_manager()
        .await
        .map_err(|e| anyhow!("Connect Redis failed: {}", e))?;

    REDIS_MANAGER
        .set(conn)
        .map_err(|_| anyhow!("Redis manager already initialized"))?;
    Ok(REDIS_MANAGER.get().unwrap())
}

/// Set a string value by key with optional TTL seconds (None -> persist)
pub async fn redis_set(key: &str, value: &str, ttl_secs: Option<usize>) -> Result<()> {
    let mut conn = get_manager().await?.clone();
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
    let mut conn = get_manager().await?.clone();
    let val: Option<String> = conn
        .get(key)
        .await
        .map_err(|e| anyhow!("Redis GET failed: {}", e))?;
    Ok(val)
}

/// Convenience: set JSON value by key (stored as string)
pub async fn redis_set_json<T: serde::Serialize>(key: &str, value: &T, ttl_secs: Option<usize>) -> Result<()> {
    let s = sonic_rs::to_string(value)?;
    redis_set(key, &s, ttl_secs).await
}

/// Convenience: get JSON value by key
pub async fn redis_get_json<T: serde::de::DeserializeOwned>(key: &str) -> Result<Option<T>> {
    match redis_get(key).await? {
        Some(s) => Ok(Some(sonic_rs::from_str::<T>(&s)?)),
        None => Ok(None),
    }
}
