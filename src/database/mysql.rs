use base64::Engine;
use crate::ISDEBUG; // for conditional lightweight debug logging
use sonic_rs::Value;
use crate::json_compat::{value_from_f64, value_from_string};
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
        let mut obj = sonic_rs::Object::with_capacity(columns_count); // Pre-allocate columns

        for column in row.columns() {
            let name = column.name();
            // Avoid expensive format! + to_uppercase(); use once, then reuse lowercase variant.
            // Debug impl cukup stabil untuk pattern keywords; kita ambil &str lalu buat lowercase.
            let raw_dbg = format!("{:?}", column.type_info());
            // Lowercase untuk pencarian substring case-insensitive.
            let type_lc = raw_dbg.to_ascii_lowercase();
            let is_unsigned = type_lc.contains("unsigned");
            let has_binary_flag = type_lc.contains("binary");

            let value = if type_lc.contains("long") {
                if is_unsigned {
                    match row.try_get::<Option<u64>, _>(name) {
                        Ok(Some(v)) => Value::from(v),
                        Ok(None) => sonic_rs::Value::default(),
                        Err(e) => {
                            if *ISDEBUG { eprintln!("[mysql conv] get {} as u64: {:?}", name, e); }
                            sonic_rs::Value::default()
                        }
                    }
                } else {
                    match row.try_get::<Option<i64>, _>(name) {
                        Ok(Some(v)) => Value::from(v),
                        Ok(None) => sonic_rs::Value::default(),
                        Err(e) => {
                            if *ISDEBUG { eprintln!("[mysql conv] get {} as i64: {:?}", name, e); }
                            sonic_rs::Value::default()
                        }
                    }
                }
            } else if (type_lc.contains("varstring") || type_lc.contains("varchar")) && has_binary_flag
            {
                // Handle VARCHAR with BINARY flag (VARBINARY-like behavior)
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        // Try to convert to UTF-8 string first, fallback to base64 if failed
                        match String::from_utf8(v.clone()) {
                            Ok(s) => value_from_string(s),
                            Err(_) => {
                                value_from_string(base64::engine::general_purpose::STANDARD.encode(v))
                            }
                        }
                    }
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default()
                }
            } else if type_lc.contains("varstring")
                || type_lc.contains("varchar")
                || type_lc.contains("decimal")
                || type_lc.contains("numeric")
                || type_lc.contains("enum")
                || type_lc.contains("set")
            {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => value_from_string(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("datetime") {
                // Handle DATETIME regardless of BINARY flag - put this BEFORE general BINARY check
                match row.try_get::<Option<chrono::NaiveDateTime>, _>(name) {
                    Ok(Some(dt)) => Value::from(dt.to_string().as_str()),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("timestamp") {
                match row.try_get::<Option<chrono::DateTime<chrono::Local>>, _>(name) {
                    Ok(Some(dt)) => value_from_string(dt.to_rfc3339()),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_) => match row.try_get::<Option<chrono::NaiveDateTime>, _>(name) {
                        Ok(Some(dt)) => Value::from(dt.to_string().as_str()),
                        Ok(None) => sonic_rs::Value::default(),
                        Err(_e) => sonic_rs::Value::default(),
                    },
                }
            } else if type_lc.contains(" time") || type_lc == "time" { // crude guard to avoid matching 'timestamp'
                match row.try_get::<Option<chrono::NaiveTime>, _>(name) {
                    Ok(Some(t)) => Value::from(t.to_string().as_str()),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("varbinary")
                || (type_lc.contains("binary")
                    && !type_lc.contains("varstring")
                    && !type_lc.contains("varchar")
                    && !type_lc.contains("datetime")
                    && !type_lc.contains("timestamp")
                    && !type_lc.contains(" time"))
            {
                // Only handle pure BINARY/VARBINARY types, exclude date/time types and VARCHAR types
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        value_from_string(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("blob") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => {
                        value_from_string(base64::engine::general_purpose::STANDARD.encode(v))
                    }
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("float") || type_lc.contains("double") {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => value_from_f64(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("tiny") || type_lc.contains("bit") {
                match row.try_get::<Option<i32>, _>(name) {
                    Ok(Some(v)) => Value::from(v),
                    Ok(None) => sonic_rs::Value::default(),
                    Err(_e) => sonic_rs::Value::default(),
                }
            } else if type_lc.contains("json") {
                row.try_get::<String, _>(name)
                    .ok()
                    .and_then(|s| sonic_rs::from_str::<Value>(&s).ok())
                    .unwrap_or(sonic_rs::Value::default())
            } else {
                // Fallback simple string / binary attempt tanpa spam log
                row.try_get::<String, _>(name)
                    .map(value_from_string)
                    .unwrap_or_else(|_| {
                        row.try_get::<Option<Vec<u8>>, _>(name)
                            .map(|opt_bytes| match opt_bytes {
                                Some(bytes) => value_from_string(
                                    base64::engine::general_purpose::STANDARD.encode(bytes),
                                ),
                                None => sonic_rs::Value::default(),
                            })
                            .unwrap_or(sonic_rs::Value::default())
                    })
            };

            obj.insert(name, value);
        }

        json_array.push(Value::from(obj));
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
