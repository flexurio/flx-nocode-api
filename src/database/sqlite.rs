use base64::Engine;
use sonic_rs::Value;
use crate::json_compat::{value_from_f64, value_from_string};
use sqlx::SqlitePool;
use sqlx::{
    sqlite::{Sqlite, SqliteRow},
    Column, Row, Transaction,
};

use super::state::{DbParam, DbRepository, DbTransaction};

type Map<K, V> = std::collections::HashMap<K, V>;

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
        let mut obj = sonic_rs::Object::with_capacity(columns_count);

        for column in row.columns() {
            let name = column.name();
            let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();

            let value = if type_info_debug.contains("INT") {
                match row.try_get::<Option<i64>, _>(name) {
                    Ok(Some(v)) => Value::from(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_) => sonic_rs::Value::default(),
                }
            } else if type_info_debug.contains("REAL")
                || type_info_debug.contains("FLOAT")
                || type_info_debug.contains("DOUBLE")
            {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => value_from_f64(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_) => sonic_rs::Value::default(),
                }
            } else if type_info_debug.contains("TEXT") || type_info_debug.contains("CHAR") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => value_from_string(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_) => sonic_rs::Value::default(),
                }
            } else if type_info_debug.contains("BLOB") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => value_from_string(base64::engine::general_purpose::STANDARD.encode(v)),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_) => sonic_rs::Value::default(),
                }
            } else if type_info_debug.contains("DATE") || type_info_debug.contains("TIME") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => value_from_string(v),
                    _ => sonic_rs::Value::default(),
                }
            } else {
                // fallback
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => value_from_string(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_) => sonic_rs::Value::default(),
                }
            };

            obj.insert(name, value);
        }

        json_array.push(Value::from(obj));
    }

    json_array.shrink_to_fit();
    json_array
}

#[async_trait::async_trait]
impl DbRepository for SqliteRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // jalankan query dan ambil hasilnya ke dalam Value
        match sqlx::query(sql).fetch_all(&self.pool).await {
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
        let mut q = sqlx::query(sql);
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
        let mut q = sqlx::query(sql);
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
