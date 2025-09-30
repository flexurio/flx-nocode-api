use anyhow::Result;
use base64::Engine;
use serde_json::{Map, Value};
use std::sync::Arc;
use tiberius::{AuthMethod, Client, EncryptionLevel, Query, Row};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use super::state::{rehydrate_placeholders, DbParam, DbRepository, DbTransaction};

// Placeholder conversion centralized in state::rehydrate_placeholders

// Replace bare TRUE/FALSE literals (common in other SQL dialects) with MSSQL-friendly forms.
// This scan avoids changing text inside single-quoted string literals.
fn normalize_mssql_booleans(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '\'' {
            // handle escaped '' inside strings
            out.push(c);
            if in_str {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '\'' {
                    // escaped quote inside string, keep in string
                    out.push('\'');
                    i += 2;
                    continue;
                } else {
                    in_str = false;
                    i += 1;
                    continue;
                }
            } else {
                in_str = true;
                i += 1;
                continue;
            }
        }
        if !in_str {
            // Try TRUE/FALSE tokens with word boundaries
            // Check for TRUE
            if i + 4 <= bytes.len() {
                let token = &sql[i..i + 4];
                if token.eq_ignore_ascii_case("true") {
                    // ensure word boundary before and after
                    let prev_ok = i == 0 || !sql.as_bytes()[i - 1].is_ascii_alphanumeric();
                    let next_ok = i + 4 == bytes.len()
                        || !sql.as_bytes()[i + 4].is_ascii_alphanumeric();
                    if prev_ok && next_ok {
                        out.push_str("CAST(1 AS bit)");
                        i += 4;
                        continue;
                    }
                }
            }
            // Check for FALSE
            if i + 5 <= bytes.len() {
                let token = &sql[i..i + 5];
                if token.eq_ignore_ascii_case("false") {
                    let prev_ok = i == 0 || !sql.as_bytes()[i - 1].is_ascii_alphanumeric();
                    let next_ok = i + 5 == bytes.len()
                        || !sql.as_bytes()[i + 5].is_ascii_alphanumeric();
                    if prev_ok && next_ok {
                        out.push_str("CAST(0 AS bit)");
                        i += 5;
                        continue;
                    }
                }
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

pub struct MssqlRepo {
    pub client: Arc<tokio::sync::Mutex<Client<tokio_util::compat::Compat<tokio::net::TcpStream>>>>,
}

// Minimal conversion from MSSQL Row to serde_json::Value
fn mssql_row_to_json(row: &Row) -> Value {
    let mut obj = Map::new();
    for col in row.columns() {
        let name = col.name();
        // Try common types by downcasting; fallback to string
        let val = if let Ok(v) = row.try_get::<i64, _>(name) {
            v.map(|n| Value::Number(n.into())).unwrap_or(Value::Null)
        } else if let Ok(v) = row.try_get::<f64, _>(name) {
            v.and_then(serde_json::Number::from_f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        } else if let Ok(v) = row.try_get::<&str, _>(name) {
            v.map(|s| Value::String(s.to_string())).unwrap_or(Value::Null)
        } else if let Ok(v) = row.try_get::<&[u8], _>(name) {
            v.map(|b| Value::String(base64::engine::general_purpose::STANDARD.encode(b)))
                .unwrap_or(Value::Null)
        } else if let Ok(v) = row.try_get::<bool, _>(name) {
            v.map(Value::Bool).unwrap_or(Value::Null)
        } else if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(name) {
            v.map(|dt| Value::String(dt.to_string())).unwrap_or(Value::Null)
        } else if let Ok(v) = row.try_get::<&str, _>(name) {
            v.map(|s| Value::String(s.to_string())).unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        obj.insert(name.to_string(), val);
    }
    Value::Object(obj)
}

#[async_trait::async_trait]
impl DbRepository for MssqlRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        let mut client = self.client.lock().await;
        let norm = normalize_mssql_booleans(sql);
        let stream = client.simple_query(&norm).await?;
        let rows: Vec<Row> = stream.into_first_result().await?;
        Ok(rows.iter().map(mssql_row_to_json).collect())
    }

    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error> {
        let mut client = self.client.lock().await;
        let norm = normalize_mssql_booleans(sql);
        let stream = client.simple_query(&norm).await?;
        let rows: Vec<Row> = stream.into_first_result().await?;
        // Expect single row single column
        let first = rows.first().ok_or_else(|| anyhow::anyhow!("No rows"))?;
        let v: i32 = first.try_get::<i32, _>(0)?.unwrap_or(0);
        Ok(v)
    }

    async fn query_with_params(&self, sql: &str, params: Vec<DbParam>) -> Result<Vec<Value>, anyhow::Error> {
        let mut client = self.client.lock().await;
    let converted = rehydrate_placeholders(sql, "mssql");
        let norm = normalize_mssql_booleans(&converted);
        let mut q = Query::new(norm);
        for p in params {
            match p {
                DbParam::I64(v) => { q.bind(v); },
                DbParam::F64(v) => { q.bind(v); },
                DbParam::Str(v) => { q.bind(v); },
                DbParam::Bool(v) => { q.bind(v); },
                DbParam::Null => { q.bind(Option::<i32>::None); },
            }
        }
        let stream = q.query(&mut *client).await?;
        let rows: Vec<Row> = stream.into_first_result().await?;
        Ok(rows.into_iter().map(|r| mssql_row_to_json(&r)).collect())
    }

    async fn get_total_rows_with_params(&self, sql: &str, params: Vec<DbParam>) -> Result<i32, anyhow::Error> {
        let mut client = self.client.lock().await;
    let converted = rehydrate_placeholders(sql, "mssql");
        let norm = normalize_mssql_booleans(&converted);
        let mut q = Query::new(norm);
        for p in params {
            match p {
                DbParam::I64(v) => { q.bind(v); },
                DbParam::F64(v) => { q.bind(v); },
                DbParam::Str(v) => { q.bind(v); },
                DbParam::Bool(v) => { q.bind(v); },
                DbParam::Null => { q.bind(Option::<i32>::None); },
            }
        }
        let stream = q.query(&mut *client).await?;
        let row = stream
            .into_first_result()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No rows"))?;
        let v: i32 = row.try_get::<i32, _>(0)?.unwrap_or(0);
        Ok(v)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error> {
        // Start a transaction by issuing BEGIN TRAN
        {
            let mut client = self.client.lock().await;
            client.simple_query("BEGIN TRAN").await?;
        }
        // Clone the shared client handle into the transaction to satisfy 'static lifetime
        Ok(Box::new(MssqlTransaction { client: self.client.clone() }))
    }
}

pub struct MssqlTransaction {
    client: Arc<tokio::sync::Mutex<Client<tokio_util::compat::Compat<tokio::net::TcpStream>>>>,
}

#[async_trait::async_trait]
impl DbTransaction for MssqlTransaction {
    async fn query_with_params(&mut self, sql: &str, params: Vec<DbParam>) -> Result<Vec<Value>, anyhow::Error> {
        let mut client = self.client.lock().await;
    let converted = rehydrate_placeholders(sql, "mssql");
        let norm = normalize_mssql_booleans(&converted);
        let mut q = Query::new(norm);
        for p in params {
            match p {
                DbParam::I64(v) => { q.bind(v); },
                DbParam::F64(v) => { q.bind(v); },
                DbParam::Str(v) => { q.bind(v); },
                DbParam::Bool(v) => { q.bind(v); },
                DbParam::Null => { q.bind(Option::<i32>::None); },
            }
        }
        let stream = q.query(&mut *client).await?;
        let rows: Vec<Row> = stream.into_first_result().await?;
        Ok(rows.into_iter().map(|r| mssql_row_to_json(&r)).collect())
    }

    async fn commit(self: Box<Self>) -> Result<(), anyhow::Error> {
        let mut client = self.client.lock().await;
        client.simple_query("COMMIT TRAN").await?;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), anyhow::Error> {
        let mut client = self.client.lock().await;
        client.simple_query("ROLLBACK TRAN").await?;
        Ok(())
    }
}

// Helper to connect via TDS from connection string format similar to other DB URLs
pub async fn connect_mssql(url: &str, timeout_secs: u64) -> Result<Client<tokio_util::compat::Compat<tokio::net::TcpStream>>> {
    // Accept formats like: mssql://user:pass@host:port/db
    let parsed = url::Url::parse(url)?;
    let host = parsed.host_str().ok_or_else(|| anyhow::anyhow!("Missing host"))?;
    let port = parsed.port().unwrap_or(1433);
    let database = parsed.path().trim_start_matches('/');
    let username = parsed.username();
    let password = parsed.password().unwrap_or("");
    // Optional toggles via URL query or env vars
    let mut trust_cert_flag = false;
    let mut enc_level: Option<EncryptionLevel> = None;
    for (k, v) in parsed.query_pairs() {
        match k.as_ref() {
            "trust_cert" | "trustservercertificate" => {
                let vv = v.to_ascii_lowercase();
                trust_cert_flag = matches!(vv.as_str(), "1" | "true" | "yes");
            }
            "encrypt" | "encryption" => {
                let vv = v.to_ascii_lowercase();
                enc_level = Some(match vv.as_str() {
                    "off" => EncryptionLevel::Off,
                    "not_supported" | "notsupported" => EncryptionLevel::NotSupported,
                    _ => EncryptionLevel::Required,
                });
            }
            _ => {}
        }
    }
    // Env overrides
    if std::env::var("MSSQL_TRUST_CERT").map(|s| matches!(s.to_lowercase().as_str(), "1"|"true"|"yes")).unwrap_or(false) {
        trust_cert_flag = true;
    }
    if let Ok(s) = std::env::var("MSSQL_ENCRYPTION") {
        enc_level = Some(match s.to_lowercase().as_str() {
            "off" => EncryptionLevel::Off,
            "not_supported" | "notsupported" => EncryptionLevel::NotSupported,
            _ => EncryptionLevel::Required,
        });
    }

    let addr = format!("{}:{}", host, port);
    let tcp = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), tokio::net::TcpStream::connect(addr)).await??;
    tcp.set_nodelay(true)?;
    let mut config = tiberius::Config::new();
    config.host(host);
    config.port(port);
    if !database.is_empty() { config.database(database); }
    config.authentication(AuthMethod::sql_server(username.to_string(), password.to_string()));
    // TLS
    config.encryption(enc_level.unwrap_or(EncryptionLevel::Required));
    if trust_cert_flag {
        // Equivalent to TrustServerCertificate=Yes in connection strings
        config.trust_cert();
    }

    let client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    Ok(client)
}
