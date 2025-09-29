use anyhow::Result;
use base64::Engine;
use serde_json::{Map, Value};
use std::sync::Arc;
use tiberius::{AuthMethod, Client, Query, Row};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use super::state::{DbParam, DbRepository, DbTransaction};

fn convert_placeholders_to_mssql(sql: &str) -> String {
    // Replace each '?' with @P1, @P2, ...
    let mut out = String::with_capacity(sql.len());
    let mut idx = 1;
    for ch in sql.chars() {
        if ch == '?' {
            out.push_str("@P");
            out.push_str(&idx.to_string());
            idx += 1;
        } else {
            out.push(ch);
        }
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
        let stream = client.simple_query(sql).await?;
        let rows: Vec<Row> = stream.into_first_result().await?;
        Ok(rows.iter().map(mssql_row_to_json).collect())
    }

    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error> {
        let mut client = self.client.lock().await;
        let stream = client.simple_query(sql).await?;
        let rows: Vec<Row> = stream.into_first_result().await?;
        // Expect single row single column
        let first = rows.first().ok_or_else(|| anyhow::anyhow!("No rows"))?;
        let v: i32 = first.try_get::<i32, _>(0)?.unwrap_or(0);
        Ok(v)
    }

    async fn query_with_params(&self, sql: &str, params: Vec<DbParam>) -> Result<Vec<Value>, anyhow::Error> {
        let mut client = self.client.lock().await;
        let converted = convert_placeholders_to_mssql(sql);
        let mut q = Query::new(converted);
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
        let converted = convert_placeholders_to_mssql(sql);
        let mut q = Query::new(converted);
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
        let converted = convert_placeholders_to_mssql(sql);
        let mut q = Query::new(converted);
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

    let addr = format!("{}:{}", host, port);
    let tcp = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), tokio::net::TcpStream::connect(addr)).await??;
    tcp.set_nodelay(true)?;
    let mut config = tiberius::Config::new();
    config.host(host);
    config.port(port);
    if !database.is_empty() { config.database(database); }
    config.authentication(AuthMethod::sql_server(username.to_string(), password.to_string()));
    // Use TLS via rustls feature
    config.encryption(tiberius::EncryptionLevel::Required);

    let client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    Ok(client)
}
