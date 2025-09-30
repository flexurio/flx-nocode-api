use base64::Engine;
use serde_json::{Map, Value};
use sqlx::{
    postgres::{PgRow, Postgres},
    Column, Pool, Row, Transaction,
};

use super::state::{rehydrate_placeholders, DbParam, DbRepository, DbTransaction};

pub struct PostgresRepo {
    pub pool: Pool<Postgres>,
}

pub fn pgrows_to_json(rows: Vec<PgRow>) -> Vec<Value> {
    let mut json_array = Vec::new();

    for row in rows {
        let mut obj = Map::new();

        for column in row.columns() {
            let name = column.name();
            let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();

            let value = if type_info_debug.contains("INT8") {
                match row.try_get::<Option<i64>, _>(name) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<i64>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("INT4") || type_info_debug.contains("INT2") {
                match row.try_get::<Option<i32>, _>(name) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<i32>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("FLOAT") || type_info_debug.contains("NUMERIC") {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<f64>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("CHAR")
                || type_info_debug.contains("TEXT")
                || type_info_debug.contains("UUID")
                || type_info_debug.contains("VARCHAR")
            {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<String>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("BYTEA") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<Vec<u8>>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("BOOL") {
                match row.try_get::<Option<bool>, _>(name) {
                    Ok(Some(v)) => Value::Bool(v),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<bool>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("JSON") {
                row.try_get::<Option<String>, _>(name)
                    .ok()
                    .and_then(|opt| opt.and_then(|s| serde_json::from_str::<Value>(&s).ok()))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("TIMESTAMPTZ")
                || type_info_debug.contains("TIMESTAMP")
            {
                row.try_get::<Option<chrono::NaiveDateTime>, _>(name)
                    .map(|opt| {
                        opt.map(|dt| Value::String(dt.to_string()))
                            .unwrap_or(Value::Null)
                    })
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("DATE") {
                row.try_get::<Option<chrono::NaiveDate>, _>(name)
                    .map(|opt| {
                        opt.map(|d| Value::String(d.to_string()))
                            .unwrap_or(Value::Null)
                    })
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("TIME") {
                row.try_get::<Option<chrono::NaiveTime>, _>(name)
                    .map(|opt| {
                        opt.map(|t| Value::String(t.to_string()))
                            .unwrap_or(Value::Null)
                    })
                    .unwrap_or(Value::Null)
            } else {
                // fallback
                row.try_get::<Option<String>, _>(name)
                    .map(|opt| opt.map(Value::String).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null)
            };

            obj.insert(name.to_string(), value);
        }

        json_array.push(Value::Object(obj));
    }

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
