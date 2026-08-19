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

#[derive(Clone, Copy, Debug)]
enum PgColType {
    Int8,
    Int4Or2,
    FloatNumeric,
    TextLike,
    Bytea,
    Bool,
    Json,
    Timestamp,
    Date,
    Time,
    Fallback,
}

struct PgColMeta {
    name: String,
    col_type: PgColType,
}

fn determine_pg_col_type(column: &sqlx::postgres::PgColumn) -> PgColType {
    let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();

    if type_info_debug.contains("INT8") {
        PgColType::Int8
    } else if type_info_debug.contains("INT4") || type_info_debug.contains("INT2") {
        PgColType::Int4Or2
    } else if type_info_debug.contains("FLOAT") || type_info_debug.contains("NUMERIC") {
        PgColType::FloatNumeric
    } else if type_info_debug.contains("CHAR")
        || type_info_debug.contains("TEXT")
        || type_info_debug.contains("UUID")
        || type_info_debug.contains("VARCHAR")
    {
        PgColType::TextLike
    } else if type_info_debug.contains("BYTEA") {
        PgColType::Bytea
    } else if type_info_debug.contains("BOOL") {
        PgColType::Bool
    } else if type_info_debug.contains("JSON") {
        PgColType::Json
    } else if type_info_debug.contains("TIMESTAMPTZ") || type_info_debug.contains("TIMESTAMP") {
        PgColType::Timestamp
    } else if type_info_debug.contains("DATE") {
        PgColType::Date
    } else if type_info_debug.contains("TIME") {
        PgColType::Time
    } else {
        PgColType::Fallback
    }
}

pub fn pgrows_to_json(rows: Vec<PgRow>) -> Vec<Value> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Inspect column schema ONCE from the first row
    let meta_list: Vec<PgColMeta> = rows[0]
        .columns()
        .iter()
        .map(|col| PgColMeta {
            name: col.name().to_string(),
            col_type: determine_pg_col_type(col),
        })
        .collect();

    let mut json_array = Vec::with_capacity(rows.len());

    for row in rows {
        let mut obj = Map::with_capacity(meta_list.len());

        for (idx, meta) in meta_list.iter().enumerate() {
            let name = &meta.name;
            let value = match meta.col_type {
                PgColType::Int8 => match row.try_get::<Option<i64>, _>(idx) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<i64>: {:?}", name, e);
                        Value::Null
                    }
                },
                PgColType::Int4Or2 => match row.try_get::<Option<i32>, _>(idx) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<i32>: {:?}", name, e);
                        Value::Null
                    }
                },
                PgColType::FloatNumeric => match row.try_get::<Option<f64>, _>(idx) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<f64>: {:?}", name, e);
                        Value::Null
                    }
                },
                PgColType::TextLike => match row.try_get::<Option<String>, _>(idx) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<String>: {:?}", name, e);
                        Value::Null
                    }
                },
                PgColType::Bytea => match row.try_get::<Option<Vec<u8>>, _>(idx) {
                    Ok(Some(v)) => {
                        Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<Vec<u8>>: {:?}", name, e);
                        Value::Null
                    }
                },
                PgColType::Bool => match row.try_get::<Option<bool>, _>(idx) {
                    Ok(Some(v)) => Value::Bool(v),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<bool>: {:?}", name, e);
                        Value::Null
                    }
                },
                PgColType::Json => row
                    .try_get::<Option<String>, _>(idx)
                    .ok()
                    .and_then(|opt| opt.and_then(|s| serde_json::from_str::<Value>(&s).ok()))
                    .unwrap_or(Value::Null),
                PgColType::Timestamp => row
                    .try_get::<Option<chrono::NaiveDateTime>, _>(idx)
                    .map(|opt| {
                        opt.map(|dt| Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
                            .unwrap_or(Value::Null)
                    })
                    .unwrap_or(Value::Null),
                PgColType::Date => row
                    .try_get::<Option<chrono::NaiveDate>, _>(idx)
                    .map(|opt| {
                        opt.map(|d| Value::String(d.to_string()))
                            .unwrap_or(Value::Null)
                    })
                    .unwrap_or(Value::Null),
                PgColType::Time => row
                    .try_get::<Option<chrono::NaiveTime>, _>(idx)
                    .map(|opt| {
                        opt.map(|t| Value::String(t.to_string()))
                            .unwrap_or(Value::Null)
                    })
                    .unwrap_or(Value::Null),
                PgColType::Fallback => row
                    .try_get::<Option<String>, _>(idx)
                    .map(|opt| opt.map(Value::String).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null),
            };

            obj.insert(meta.name.clone(), value);
        }

        json_array.push(Value::Object(obj));
    }

    json_array.shrink_to_fit();
    json_array
}

#[async_trait::async_trait]
impl DbRepository for PostgresRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // SAFETY: `sql` is built by our internal query compiler (storage::sql_store),
        // not raw-concatenated user input; user-supplied values are always bound
        // as parameters, never interpolated into the SQL text.
        match sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(&self.pool).await {
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

        // SAFETY: see comment on `query` above — `converted` is our own internal
        // query text with placeholders rewritten; params are always bound.
        let mut q = sqlx::query(sqlx::AssertSqlSafe(converted));
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

        // SAFETY: see comment on `query` above — `converted` is our own internal
        // query text with placeholders rewritten; params are always bound.
        let mut q = sqlx::query(sqlx::AssertSqlSafe(converted));
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
