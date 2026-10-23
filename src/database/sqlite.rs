use base64::Engine;
use serde_json::{Map, Value};
use sqlx::SqlitePool;
use sqlx::{
    sqlite::{Sqlite, SqliteRow},
    Column, Row, Transaction,
};

use super::state::{DbParam, DbRepository, DbTransaction};

/// SQLite allows many concurrent readers (WAL) but exactly one writer. Giving
/// writers their own single-connection pool serialises them in-process instead
/// of letting N connections fight over the file lock (`database is locked`).
pub struct SqliteRepo {
    /// Read pool (SELECT / PRAGMA / EXPLAIN / WITH).
    pub pool: SqlitePool,
    /// Write pool, `max_connections = 1`; also used for every transaction.
    pub write_pool: SqlitePool,
}

/// True when the statement only reads. Anything else goes to the writer.
pub fn is_read_only_sql(sql: &str) -> bool {
    let head: String = sql
        .trim_start()
        .chars()
        .take(8)
        .collect::<String>()
        .to_ascii_uppercase();
    head.starts_with("SELECT") || head.starts_with("WITH") || head.starts_with("PRAGMA") || head.starts_with("EXPLAIN")
}

impl SqliteRepo {
    fn pool_for(&self, sql: &str) -> &SqlitePool {
        if is_read_only_sql(sql) { &self.pool } else { &self.write_pool }
    }
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
        match sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(self.pool_for(sql)).await {
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
        match q.fetch_all(self.pool_for(sql)).await {
            Ok(rows) => {
                let rows: Vec<SqliteRow> = rows.into_iter().collect();
                Ok(sqliterows_to_json(rows))
            }
            Err(e) => Err(anyhow::anyhow!("Error executing query: {}", e)),
        }
    }

    fn pool_stats(&self) -> Option<super::state::PoolStats> {
        Some(super::state::PoolStats {
            size: self.pool.size() + self.write_pool.size(),
            idle: (self.pool.num_idle() + self.write_pool.num_idle()) as u32,
            max: self.pool.options().get_max_connections() + 1,
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error> {
        let tx = self.write_pool.begin().await?;
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

    async fn execute(
        &mut self,
        sql: &str,
        params: Vec<DbParam>,
    ) -> Result<u64, anyhow::Error> {
        // SAFETY: same as `query_with_params` — internal SQL text, bound params.
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
        match q.execute(&mut *self.tx).await {
            Ok(res) => Ok(res.rows_affected()),
            Err(e) => Err(anyhow::anyhow!("Error executing statement: {}", e)),
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

#[cfg(test)]
mod routing_tests {
    use super::is_read_only_sql;

    #[test]
    fn classifies_statements() {
        assert!(is_read_only_sql("  select 1"));
        assert!(is_read_only_sql("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(is_read_only_sql("PRAGMA journal_mode"));
        assert!(!is_read_only_sql("INSERT INTO t VALUES (1)"));
        assert!(!is_read_only_sql("UPDATE t SET a = 1"));
        assert!(!is_read_only_sql("CREATE TABLE t (a)"));
    }
}
