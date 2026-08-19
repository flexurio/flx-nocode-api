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

#[derive(Clone, Copy, Debug)]
enum SqliteColType {
    Int,
    RealFloatDouble,
    TextLike,
    Blob,
    DateTime,
    Fallback,
}

struct SqliteColMeta {
    name: String,
    col_type: SqliteColType,
}

fn determine_sqlite_col_type(column: &sqlx::sqlite::SqliteColumn) -> SqliteColType {
    let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();

    if type_info_debug.contains("INT") {
        SqliteColType::Int
    } else if type_info_debug.contains("REAL")
        || type_info_debug.contains("FLOAT")
        || type_info_debug.contains("DOUBLE")
    {
        SqliteColType::RealFloatDouble
    } else if type_info_debug.contains("TEXT") || type_info_debug.contains("CHAR") {
        SqliteColType::TextLike
    } else if type_info_debug.contains("BLOB") {
        SqliteColType::Blob
    } else if type_info_debug.contains("DATE") || type_info_debug.contains("TIME") {
        SqliteColType::DateTime
    } else {
        SqliteColType::Fallback
    }
}

pub fn sqliterows_to_json(rows: Vec<SqliteRow>) -> Vec<Value> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Inspect column schema ONCE from the first row
    let meta_list: Vec<SqliteColMeta> = rows[0]
        .columns()
        .iter()
        .map(|col| SqliteColMeta {
            name: col.name().to_string(),
            col_type: determine_sqlite_col_type(col),
        })
        .collect();

    let mut json_array = Vec::with_capacity(rows.len());

    for row in rows {
        let mut obj = Map::with_capacity(meta_list.len());

        for (idx, meta) in meta_list.iter().enumerate() {
            let value = match meta.col_type {
                SqliteColType::Int => match row.try_get::<Option<i64>, _>(idx) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    _ => Value::Null,
                },
                SqliteColType::RealFloatDouble => match row.try_get::<Option<f64>, _>(idx) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    _ => Value::Null,
                },
                SqliteColType::TextLike => match row.try_get::<Option<String>, _>(idx) {
                    Ok(Some(v)) => Value::String(v),
                    _ => Value::Null,
                },
                SqliteColType::Blob => match row.try_get::<Option<Vec<u8>>, _>(idx) {
                    Ok(Some(v)) => {
                        Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    _ => Value::Null,
                },
                SqliteColType::DateTime => match row.try_get::<Option<String>, _>(idx) {
                    Ok(Some(v)) => Value::String(v),
                    _ => Value::Null,
                },
                SqliteColType::Fallback => match row.try_get::<Option<String>, _>(idx) {
                    Ok(Some(v)) => Value::String(v),
                    _ => Value::Null,
                },
            };

            obj.insert(meta.name.clone(), value);
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
