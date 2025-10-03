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

pub fn mysqlrows_to_json(rows: Vec<MySqlRow>) -> Vec<Value> {
    // Pre-allocate with exact capacity to avoid reallocations
    let mut json_array = Vec::with_capacity(rows.len());
    
    if rows.is_empty() {
        return json_array; // Early return for empty result
    }

    for row in rows {
        let columns_count = row.columns().len();
        let mut obj = Map::with_capacity(columns_count); // Pre-allocate columns

        for column in row.columns() {
            let name = column.name();
            let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();
            let is_unsigned = type_info_debug.contains("UNSIGNED");

            // Check if column has BINARY flag
            let has_binary_flag = type_info_debug.contains("BINARY");

            let value = if type_info_debug.contains("LONG") {
                if is_unsigned {
                    match row.try_get::<Option<u64>, _>(name) {
                        Ok(Some(v)) => Value::Number(v.into()),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            eprintln!("Failed to get {} as Option<u64>: {:?}", name, e);
                            Value::Null
                        }
                    }
                } else {
                    match row.try_get::<Option<i64>, _>(name) {
                        Ok(Some(v)) => Value::Number(v.into()),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            eprintln!("Failed to get {} as Option<i64>: {:?}", name, e);
                            Value::Null
                        }
                    }
                }
            } else if (type_info_debug.contains("VARSTRING") || type_info_debug.contains("VARCHAR"))
                && has_binary_flag
            {
                // Handle VARCHAR with BINARY flag (VARBINARY-like behavior)
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        // Try to convert to UTF-8 string first, fallback to base64 if failed
                        match String::from_utf8(v.clone()) {
                            Ok(s) => Value::String(s),
                            Err(_) => {
                                Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                            }
                        }
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("VARSTRING with BINARY flag - Failed to get {} as Option<Vec<u8>>: {:?}, {} \n", name, e, type_info_debug);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("VARSTRING")
                || type_info_debug.contains("VARCHAR")
                || type_info_debug.contains("DECIMAL")
                || type_info_debug.contains("NUMERIC")
                || type_info_debug.contains("ENUM")
                || type_info_debug.contains("SET")
            {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!(
                            "VARSTRING Failed to get {} as Option<String>: {:?}, {} \n",
                            name, e, type_info_debug
                        );
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("DATETIME") {
                // Handle DATETIME regardless of BINARY flag - put this BEFORE general BINARY check
                match row.try_get::<Option<chrono::NaiveDateTime>, _>(name) {
                    Ok(Some(dt)) => Value::String(dt.to_string()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("DATETIME Failed to get {} as Option<chrono::NaiveDateTime>: {:?}, {} \n", name, e, type_info_debug);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("TIMESTAMP") {
                match row.try_get::<Option<chrono::DateTime<chrono::Local>>, _>(name) {
                    Ok(Some(dt)) => Value::String(dt.to_rfc3339()),
                    Ok(None) => Value::Null,
                    Err(_) => match row.try_get::<Option<chrono::NaiveDateTime>, _>(name) {
                        Ok(Some(dt)) => Value::String(dt.to_string()),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            eprintln!(
                                "TIMESTAMP Failed to get {} as chrono types: {:?}, {} \n",
                                name, e, type_info_debug
                            );
                            Value::Null
                        }
                    },
                }
            } else if type_info_debug.contains("TIME") {
                match row.try_get::<Option<chrono::NaiveTime>, _>(name) {
                    Ok(Some(t)) => Value::String(t.to_string()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!(
                            "TIME Failed to get {} as Option<chrono::NaiveTime>: {:?}, {} \n",
                            name, e, type_info_debug
                        );
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("VARBINARY")
                || (type_info_debug.contains("BINARY")
                    && !type_info_debug.contains("VARSTRING")
                    && !type_info_debug.contains("VARCHAR")
                    && !type_info_debug.contains("DATETIME")
                    && !type_info_debug.contains("TIMESTAMP")
                    && !type_info_debug.contains("TIME"))
            {
                // Only handle pure BINARY/VARBINARY types, exclude date/time types and VARCHAR types
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("VARBINARY / BINARY Failed to get {} as Option<Vec<u8>> (VARBINARY): {:?}, {} \n", name, e, type_info_debug);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("BLOB") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        Value::String(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<Vec<u8>>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("FLOAT") || type_info_debug.contains("DOUBLE") {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<f64>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("TINY") || type_info_debug.contains("BIT") {
                match row.try_get::<Option<i32>, _>(name) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        eprintln!("Failed to get {} as Option<i32>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("JSON") {
                row.try_get::<String, _>(name)
                    .ok()
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .unwrap_or(Value::Null)
            } else {
                // fallback default string conversion
                eprintln!("Unknown column type for {}: {}", name, type_info_debug);
                row.try_get::<String, _>(name)
                    .map(Value::String)
                    .unwrap_or_else(|e| {
                        eprintln!("Failed to get {} as String (fallback): {:?}", name, e);
                        // Try as binary data if string fails
                        row.try_get::<Option<Vec<u8>>, _>(name)
                            .map(|opt_bytes| match opt_bytes {
                                Some(bytes) => Value::String(
                                    base64::engine::general_purpose::STANDARD.encode(bytes),
                                ),
                                None => Value::Null,
                            })
                            .unwrap_or(Value::Null)
                    })
            };

            obj.insert(name.to_string(), value);
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
        match sqlx::query(sql).fetch_all(&self.pool).await {
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
