use base64::Engine;
use sonic_rs::Value;
use crate::json_compat::{value_from_f64, value_from_string};
use crate::ISDEBUG; // conditional debug logging
use sqlx::{
    postgres::{PgRow, Postgres},
    Column, Pool, Row, Transaction,
};

use super::state::{rehydrate_placeholders, DbParam, DbRepository, DbTransaction};


pub struct PostgresRepo {
    pub pool: Pool<Postgres>,
}

pub fn pgrows_to_json(rows: Vec<PgRow>) -> Vec<Value> {
    // Pre-allocate with exact capacity
    let mut json_array = Vec::with_capacity(rows.len());
    
    if rows.is_empty() {
        return json_array;
    }

    for row in rows {
        let columns_count = row.columns().len();
        let mut obj = sonic_rs::Object::with_capacity(columns_count);
        for column in row.columns() {
            let name = column.name();
            // Single debug string, then lowercase for substring checks
            let raw_dbg = format!("{:?}", column.type_info());
            let t = raw_dbg.to_ascii_lowercase();
            let value = if t.contains("int8") {
                match row.try_get::<Option<i64>, _>(name) {
                    Ok(Some(v)) => Value::from(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(e) => { if *ISDEBUG { eprintln!("[pg conv] {} int8 {:?}", name, e); } sonic_rs::Value::default() }
                }
            } else if t.contains("int4") || t.contains("int2") {
                match row.try_get::<Option<i32>, _>(name) {
                    Ok(Some(v)) => Value::from(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(e) => { if *ISDEBUG { eprintln!("[pg conv] {} intx {:?}", name, e); } sonic_rs::Value::default() }
                }
            } else if t.contains("float") || t.contains("numeric") {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => value_from_f64(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(e) => { if *ISDEBUG { eprintln!("[pg conv] {} f64 {:?}", name, e); } sonic_rs::Value::default() }
                }
            } else if t.contains("char") || t.contains("text") || t.contains("uuid") || t.contains("varchar") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => value_from_string(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(e) => { if *ISDEBUG { eprintln!("[pg conv] {} str {:?}", name, e); } sonic_rs::Value::default() }
                }
            } else if t.contains("bytea") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => value_from_string(base64::engine::general_purpose::STANDARD.encode(v)),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(e) => { if *ISDEBUG { eprintln!("[pg conv] {} bytea {:?}", name, e); } sonic_rs::Value::default() }
                }
            } else if t.contains("bool") {
                match row.try_get::<Option<bool>, _>(name) {
                    Ok(Some(v)) => Value::from(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(e) => { if *ISDEBUG { eprintln!("[pg conv] {} bool {:?}", name, e); } sonic_rs::Value::default() }
                }
            } else if t.contains("json") {
                row.try_get::<Option<String>, _>(name)
                    .ok()
                    .and_then(|opt| opt.and_then(|s| sonic_rs::from_str::<Value>(&s).ok()))
                    .unwrap_or(sonic_rs::Value::default())
            } else if t.contains("timestamptz") || t.contains("timestamp") {
                row.try_get::<Option<chrono::NaiveDateTime>, _>(name)
                    .map(|opt| opt.map(|dt| Value::from(dt.to_string().as_str())).unwrap_or_default())
                    .unwrap_or(sonic_rs::Value::default())
            } else if t.contains("date") {
                row.try_get::<Option<chrono::NaiveDate>, _>(name)
                    .map(|opt| opt.map(|d| Value::from(d.to_string().as_str())).unwrap_or_default())
                    .unwrap_or(sonic_rs::Value::default())
            } else if t.contains("time") { // naive time
                row.try_get::<Option<chrono::NaiveTime>, _>(name)
                    .map(|opt| opt.map(|tm| Value::from(tm.to_string().as_str())).unwrap_or_default())
                    .unwrap_or(sonic_rs::Value::default())
            } else {
                // fallback simple
                row.try_get::<Option<String>, _>(name)
                    .map(|opt| opt.map(value_from_string).unwrap_or_default())
                    .unwrap_or(sonic_rs::Value::default())
            };
            obj.insert(name, value);
        }
        json_array.push(Value::from(obj));
    }

    json_array.shrink_to_fit();
    json_array
}

#[async_trait::async_trait]
impl DbRepository for PostgresRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        match sqlx::query(sql).fetch_all(&self.pool).await {
            Ok(rows) => {
                // Jika berhasil, konversi hasilnya ke dalam JSON
                let rows: Vec<PgRow> = rows.into_iter().collect();
                let result_val = pgrows_to_json(rows);
                return Ok(result_val);
            }
            Err(e) => {
                // Jika terjadi error, kembalikan error
                return Err(anyhow::anyhow!("Error executing query: {}", e));
            }
        }
    }

    async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        // Convert '?' placeholders to PostgreSQL-style $1, $2, ...
        let converted = rehydrate_placeholders(sql, "postgres");

        let mut q = sqlx::query(&converted);
        for p in params {
            q = match p {
                DbParam::I64(v) => q.bind(v),
                DbParam::F64(v) => q.bind(v),
                DbParam::Str(v) => q.bind(v),
                DbParam::Bool(v) => q.bind(v),
                DbParam::Null => q.bind(Option::<i32>::None),
            };
        }
        match q.fetch_all(&self.pool).await {
            Ok(rows) => {
                let rows: Vec<PgRow> = rows.into_iter().collect();
                Ok(pgrows_to_json(rows))
            }
            Err(e) => Err(anyhow::anyhow!("Error executing query: {}", e)),
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error> {
        let tx = self.pool.begin().await?;
        Ok(Box::new(PostgresTransaction { tx }))
    }
}

pub struct PostgresTransaction {
    tx: Transaction<'static, Postgres>,
}

#[async_trait::async_trait]
impl DbTransaction for PostgresTransaction {
    async fn query_with_params(
        &mut self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        // Convert '?' placeholders to PostgreSQL-style $1, $2, ...
        let converted = rehydrate_placeholders(sql, "postgres");

        let mut q = sqlx::query(&converted);
        for p in params {
            q = match p {
                DbParam::I64(v) => q.bind(v),
                DbParam::F64(v) => q.bind(v),
                DbParam::Str(v) => q.bind(v),
                DbParam::Bool(v) => q.bind(v),
                DbParam::Null => q.bind(Option::<i32>::None),
            };
        }

        match q.fetch_all(&mut *self.tx).await {
            Ok(rows) => {
                let rows: Vec<PgRow> = rows.into_iter().collect();
                Ok(pgrows_to_json(rows))
            }
            Err(e) => Err(anyhow::anyhow!("Error executing query: {}", e)),
        }
    }

    async fn commit(self: Box<Self>) -> Result<(), anyhow::Error> {
        self.tx
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Transaction commit failed: {}", e))
    }

    async fn rollback(self: Box<Self>) -> Result<(), anyhow::Error> {
        self.tx
            .rollback()
            .await
            .map_err(|e| anyhow::anyhow!("Transaction rollback failed: {}", e))
    }
}
