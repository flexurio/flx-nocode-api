use std::sync::Arc;
use base64::Engine;
use serde_json::{Map, Value};
use sqlx::{mysql::MySqlRow, postgres::{PgRow, Postgres}, Column, MySqlPool, Pool, Row};

pub struct AppState {
    pub db: Arc<dyn DbRepository>,
    pub db_type: String,
    pub secret: String,
    pub encrypt_key: String,
}


#[async_trait::async_trait]
pub trait DbRepository: Send + Sync {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error>;
    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error>;
}

pub struct MySqlRepo {
    pub pool: MySqlPool,
}

pub fn mysqlrows_to_json(rows: Vec<MySqlRow>) -> Vec<Value> {
    let mut json_array = Vec::new();

    for row in rows {
        let mut obj = Map::new();

        for column in row.columns() {
            let name = column.name();
            let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();
            let is_unsigned = type_info_debug.contains("UNSIGNED");

            let value = if type_info_debug.contains("LONG") {
                if is_unsigned {
                    match row.try_get::<Option<u64>, _>(name) {
                        Ok(Some(v)) => Value::Number(v.into()),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            println!("Failed to get {} as Option<u64>: {:?}", name, e);
                            Value::Null
                        }
                    }
                } else {
                    match row.try_get::<Option<i64>, _>(name) {
                        Ok(Some(v)) => Value::Number(v.into()),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            println!("Failed to get {} as Option<i64>: {:?}", name, e);
                            Value::Null
                        }
                    }
                }
            } else if type_info_debug.contains("VARSTRING") || type_info_debug.contains("VARCHAR") 
                || type_info_debug.contains("DECIMAL") || type_info_debug.contains("NUMERIC")
                || type_info_debug.contains("ENUM") || type_info_debug.contains("SET")  {
                    match row.try_get::<Option<String>, _>(name) {
                        Ok(Some(v)) => Value::String(v),
                        Ok(None) => Value::Null,
                        Err(e) => {
                            println!("Failed to get {} as Option<u64>: {:?}", name, e);
                            Value::Null
                        }
                    }

            } else if type_info_debug.contains("BLOB") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<u64>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("FLOAT") || type_info_debug.contains("DOUBLE") {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<u64>: {:?}", name, e);
                        Value::Null
                    }
                }

            } else if type_info_debug.contains("TINY") || type_info_debug.contains("BIT") {
                match row.try_get::<Option<i32>, _>(name) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<u64>: {:?}", name, e);
                        Value::Null
                    }
                }

            } else if type_info_debug.contains("JSON") {
                row.try_get::<String, _>(name)
                    .ok()
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("DATETIME") {
                row.try_get::<chrono::NaiveDateTime, _>(name)
                    .map(|dt| Value::String(dt.to_string()))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("TIMESTAMP") {
                match row.try_get::<chrono::DateTime<chrono::Local>, _>(name) {
                    Ok(dt) => Value::String(dt.to_rfc3339()), // atau dt.to_string()
                    Err(_) => {
                        match row.try_get::<chrono::NaiveDateTime, _>(name) {
                            Ok(dt) => Value::String(dt.to_string()),
                            Err(_) => {
                                Value::Null
                            }
                        }
                    }
                }
            } else if type_info_debug.contains("TIME") {
                row.try_get::<chrono::NaiveTime, _>(name)
                    .map(|t| Value::String(t.to_string()))
                    .unwrap_or(Value::Null)
            } else {
                // fallback default string conversion
                row.try_get::<String, _>(name)
                    .map(Value::String)
                    .unwrap_or_else(|_| Value::Null)
            };

            obj.insert(name.to_string(), value);
        }

        json_array.push(Value::Object(obj));
    }

    json_array
}



#[async_trait::async_trait]
impl DbRepository for MySqlRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // jalankan query dan ambil hasilnya ke dalam Value
        match sqlx::query(sql)
            .fetch_all(&self.pool)
            .await {
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

    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error> {
        // Menghitung total baris dari tabel
        let row: (i32,) = sqlx::query_as(sql)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }
}

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
                    Ok(Some(v)) => serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        println!("Failed to get {} as Option<f64>: {:?}", name, e);
                        Value::Null
                    }
                }
            } else if type_info_debug.contains("CHAR") || type_info_debug.contains("TEXT") 
                || type_info_debug.contains("UUID") || type_info_debug.contains("VARCHAR") {
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
                    Ok(Some(v)) => Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
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
            } else if type_info_debug.contains("TIMESTAMPTZ") || type_info_debug.contains("TIMESTAMP") {
                row.try_get::<Option<chrono::NaiveDateTime>, _>(name)
                    .map(|opt| opt.map(|dt| Value::String(dt.to_string())).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("DATE") {
                row.try_get::<Option<chrono::NaiveDate>, _>(name)
                    .map(|opt| opt.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("TIME") {
                row.try_get::<Option<chrono::NaiveTime>, _>(name)
                    .map(|opt| opt.map(|t| Value::String(t.to_string())).unwrap_or(Value::Null))
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
        match sqlx::query(sql)
            .fetch_all(&self.pool)
            .await {
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


    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error> {
        // Menghitung total baris dari tabel PostgreSQL
        let row: (i32,) = sqlx::query_as(sql)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }
}


pub fn concat_column_values(values: Vec<Value>, column_name: &str, separator: &str) -> String {
    let mut result = Vec::new();

    for value in values {
        if let Value::Object(obj) = value {
            if let Some(v) = obj.get(column_name) {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "".to_string(),
                    _ => "".to_string(),
                };
                result.push(s);
            }
        }
    }

    result.join(separator)
}
