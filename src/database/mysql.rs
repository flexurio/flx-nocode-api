use base64::Engine;
use serde_json::{Map, Value};
use sqlx::{
    mysql::{MySql, MySqlRow},
    Column, MySqlPool, Row, Transaction,
};

use super::state::{DbParam, DbRepository, DbTransaction};

pub struct MySqlRepo {
    pub pool: MySqlPool,
}

#[derive(Clone, Copy, Debug)]
enum MySqlColType {
    LongUnsigned,
    LongSigned,
    VarcharBinary,
    TextLike,
    Datetime,
    Timestamp,
    Time,
    Binary,
    Blob,
    FloatDouble,
    TinyOrBit,
    Json,
    Fallback,
}

struct MySqlColMeta {
    name: String,
    col_type: MySqlColType,
}

fn determine_mysql_col_type(column: &sqlx::mysql::MySqlColumn) -> MySqlColType {
    let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();
    let is_unsigned = type_info_debug.contains("UNSIGNED");
    let has_binary_flag = type_info_debug.contains("BINARY");
    let has_longtext_blob =
        type_info_debug.contains("BLOB") && type_info_debug.contains("MAX_SIZE: SOME(4294967295)");

    if type_info_debug.contains("LONG") {
        if is_unsigned {
            MySqlColType::LongUnsigned
        } else {
            MySqlColType::LongSigned
        }
    } else if (type_info_debug.contains("VARSTRING") || type_info_debug.contains("VARCHAR"))
        && has_binary_flag
    {
        MySqlColType::VarcharBinary
    } else if type_info_debug.contains("VARSTRING")
        || type_info_debug.contains("VARCHAR")
        || type_info_debug.contains("DECIMAL")
        || type_info_debug.contains("NUMERIC")
        || type_info_debug.contains("ENUM")
        || type_info_debug.contains("SET")
        || has_longtext_blob
    {
        MySqlColType::TextLike
    } else if type_info_debug.contains("DATETIME") {
        MySqlColType::Datetime
    } else if type_info_debug.contains("TIMESTAMP") {
        MySqlColType::Timestamp
    } else if type_info_debug.contains("TIME") {
        MySqlColType::Time
    } else if type_info_debug.contains("VARBINARY")
        || (type_info_debug.contains("BINARY")
            && !type_info_debug.contains("VARSTRING")
            && !type_info_debug.contains("VARCHAR")
            && !type_info_debug.contains("DATETIME")
            && !type_info_debug.contains("TIMESTAMP")
            && !type_info_debug.contains("TIME"))
    {
        MySqlColType::Binary
    } else if type_info_debug.contains("BLOB") {
        MySqlColType::Blob
    } else if type_info_debug.contains("FLOAT") || type_info_debug.contains("DOUBLE") {
        MySqlColType::FloatDouble
    } else if type_info_debug.contains("TINY") || type_info_debug.contains("BIT") {
        MySqlColType::TinyOrBit
    } else if type_info_debug.contains("JSON") {
        MySqlColType::Json
    } else {
        MySqlColType::Fallback
    }
}

pub fn mysqlrows_to_json(rows: Vec<MySqlRow>) -> Vec<Value> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Inspect column schema ONCE from the first row
    let meta_list: Vec<MySqlColMeta> = rows[0]
        .columns()
        .iter()
        .map(|col| MySqlColMeta {
            name: col.name().to_string(),
            col_type: determine_mysql_col_type(col),
        })
        .collect();

    let mut json_array = Vec::with_capacity(rows.len());

    for row in rows {
        let mut obj = Map::with_capacity(meta_list.len());

        for (idx, meta) in meta_list.iter().enumerate() {
            let name = &meta.name;
            let value = match meta.col_type {
                MySqlColType::LongUnsigned => match row.try_get::<Option<u64>, _>(idx) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<u64>: {:?}", name, e);
                        Value::Null
                    }
                },
                MySqlColType::LongSigned => match row.try_get::<Option<i64>, _>(idx) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<i64>: {:?}", name, e);
                        Value::Null
                    }
                },
                MySqlColType::VarcharBinary => match row.try_get::<Option<Vec<u8>>, _>(idx) {
                    Ok(Some(v)) => match String::from_utf8(v.clone()) {
                        Ok(s) => Value::String(s),
                        Err(_) => {
                            Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                        }
                    },
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!(
                            "VARSTRING with BINARY flag - Failed to get {} as Option<Vec<u8>>: {:?}",
                            name, e
                        );
                        Value::Null
                    }
                },
                MySqlColType::TextLike => match row.try_get::<Option<String>, _>(idx) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(_) => match row.try_get::<Option<Vec<u8>>, _>(idx) {
                        Ok(Some(v)) => match String::from_utf8(v.clone()) {
                            Ok(s) => Value::String(s),
                            Err(_) => {
                                Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                            }
                        },
                        Ok(None) => Value::Null,
                        Err(e) => {
                            eprintln!("VARSTRING Failed to get {} as Option<String>: {:?}", name, e);
                            Value::Null
                        }
                    },
                },
                MySqlColType::Datetime => {
                    match row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                        Ok(Some(dt)) => Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            eprintln!(
                                "DATETIME Failed to get {} as Option<chrono::NaiveDateTime>: {:?}",
                                name, e
                            );
                            Value::Null
                        }
                    }
                }
                MySqlColType::Timestamp => {
                    match row.try_get::<Option<chrono::DateTime<chrono::Local>>, _>(idx) {
                        Ok(Some(dt)) => Value::String(dt.to_rfc3339()),
                        Ok(None) => Value::Null,
                        Err(_) => match row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                            Ok(Some(dt)) => {
                                Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                            }
                            Ok(None) => Value::Null,
                            Err(e) => {
                                eprintln!(
                                    "TIMESTAMP Failed to get {} as chrono types: {:?}",
                                    name, e
                                );
                                Value::Null
                            }
                        },
                    }
                }
                MySqlColType::Time => match row.try_get::<Option<chrono::NaiveTime>, _>(idx) {
                    Ok(Some(t)) => Value::String(t.to_string()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!(
                            "TIME Failed to get {} as Option<chrono::NaiveTime>: {:?}",
                            name, e
                        );
                        Value::Null
                    }
                },
                MySqlColType::Binary | MySqlColType::Blob => {
                    match row.try_get::<Option<Vec<u8>>, _>(idx) {
                        Ok(Some(v)) => {
                            Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                        }
                        Ok(None) => Value::Null,
                        Err(e) => {
                            eprintln!(
                                "Binary/Blob Failed to get {} as Option<Vec<u8>>: {:?}",
                                name, e
                            );
                            Value::Null
                        }
                    }
                }
                MySqlColType::FloatDouble => match row.try_get::<Option<f64>, _>(idx) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<f64>: {:?}", name, e);
                        Value::Null
                    }
                },
                MySqlColType::TinyOrBit => match row.try_get::<Option<i32>, _>(idx) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<i32>: {:?}", name, e);
                        Value::Null
                    }
                },
                MySqlColType::Json => row
                    .try_get::<String, _>(idx)
                    .ok()
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .unwrap_or(Value::Null),
                MySqlColType::Fallback => row
                    .try_get::<String, _>(idx)
                    .map(Value::String)
                    .unwrap_or_else(|_| {
                        row.try_get::<Option<Vec<u8>>, _>(idx)
                            .map(|opt_bytes| match opt_bytes {
                                Some(bytes) => Value::String(
                                    base64::engine::general_purpose::STANDARD.encode(bytes),
                                ),
                                None => Value::Null,
                            })
                            .unwrap_or(Value::Null)
                    }),
            };

            obj.insert(meta.name.clone(), value);
        }

        json_array.push(Value::Object(obj));
    }

    // Shrink to fit to reclaim unused memory
    json_array.shrink_to_fit();
    json_array
}

#[async_trait::async_trait]
impl DbRepository for MySqlRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // jalankan query dan ambil hasilnya ke dalam Value
        // SAFETY: `sql` is built by our internal query compiler (storage::sql_store),
        // not raw-concatenated user input; user-supplied values are always bound
        // as parameters, never interpolated into the SQL text.
        match sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(&self.pool).await {
            Ok(rows) => {
                // Jika berhasil, konversi hasilnya ke dalam JSON
                let rows: Vec<MySqlRow> = rows.into_iter().collect();
                let result_val = mysqlrows_to_json(rows);
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
                let rows: Vec<MySqlRow> = rows.into_iter().collect();
                Ok(mysqlrows_to_json(rows))
            }
            Err(e) => Err(anyhow::anyhow!("Error executing query: {}", e)),
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn DbTransaction>, anyhow::Error> {
        let tx = self.pool.begin().await?;
        Ok(Box::new(MySqlTransaction { tx }))
    }
}

pub struct MySqlTransaction {
    tx: Transaction<'static, MySql>,
}

#[async_trait::async_trait]
impl DbTransaction for MySqlTransaction {
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
                let rows: Vec<MySqlRow> = rows.into_iter().collect();
                Ok(mysqlrows_to_json(rows))
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
