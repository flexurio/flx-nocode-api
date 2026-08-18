use base64::Engine;
use serde_json::{Map, Value};
use sqlx::SqlitePool;
use sqlx::{
    sqlite::{Sqlite, SqliteRow},
    Column, Row, Transaction,
};

use super::state::{DbParam, DbRepository, DbTransaction};

pub struct SqliteRepo {
    pub pool: SqlitePool,
}

pub fn sqliterows_to_json(rows: Vec<SqliteRow>) -> Vec<Value> {
    // Pre-allocate with exact capacity
    let mut json_array = Vec::with_capacity(rows.len());
    
    if rows.is_empty() {
        return json_array;
    }

    for row in rows {
        let columns_count = row.columns().len();
        let mut obj = Map::with_capacity(columns_count);

        for column in row.columns() {
            let name = column.name();
            let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();

            let value = if type_info_debug.contains("INT") {
                match row.try_get::<Option<i64>, _>(name) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("REAL")
                || type_info_debug.contains("FLOAT")
                || type_info_debug.contains("DOUBLE")
            {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("TEXT") || type_info_debug.contains("CHAR") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("BLOB") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("DATE") || type_info_debug.contains("TIME") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    _ => Value::Null,
                }
            } else {
                // fallback
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            };

            obj.insert(name.to_string(), value);
        }

        json_array.push(Value::Object(obj));
    }

    json_array.shrink_to_fit();
    json_array
}

#[async_trait::async_trait]
impl DbRepository for SqliteRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // jalankan query dan ambil hasilnya ke dalam Value
        // SAFETY: `sql` is built by our internal query compiler (storage::sql_store),
        // not raw-concatenated user input; user-supplied values are always bound
        // as parameters, never interpolated into the SQL text.
        match sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(&self.pool).await {
            Ok(rows) => {
                // Jika berhasil, konversi hasilnya ke dalam JSON
                let rows: Vec<SqliteRow> = rows.into_iter().collect();
                let result_val = sqliterows_to_json(rows);
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
        // SAFETY: see comment on `query` above — `sql` comes from our internal
        // query compiler; `params` are always bound, never interpolated.
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
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
                let rows: Vec<SqliteRow> = rows.into_iter().collect();
                Ok(sqliterows_to_json(rows))
            }
            Err(e) => Err(anyhow::anyhow!("Error executing query: {}", e)),
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error> {
        let tx = self.pool.begin().await?;
        Ok(Box::new(SqliteTransaction { tx }))
    }
}

pub struct SqliteTransaction {
    tx: Transaction<'static, Sqlite>,
}

#[async_trait::async_trait]
impl DbTransaction for SqliteTransaction {
    async fn query_with_params(
        &mut self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        // SAFETY: see comment on `query` above — `sql` comes from our internal
        // query compiler; `params` are always bound, never interpolated.
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
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
                let rows: Vec<SqliteRow> = rows.into_iter().collect();
                Ok(sqliterows_to_json(rows))
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
