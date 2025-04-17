use std::sync::Arc;
use base64::Engine;
use serde_json::{Map, Value};
use sqlx::{Pool, Column, mysql::MySqlRow, MySqlPool, Row, postgres::Postgres};

pub struct AppState {
    pub db: Arc<dyn DbRepository>,
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

            let value = if type_info_debug.contains("LONGLONG") {
                if is_unsigned {
                    row.try_get::<u64, _>(name)
                        .map(|v| Value::Number(v.into()))
                        .unwrap_or_else(|e| {
                            println!("Failed to get {} as u64: {:?}", name, e);
                            Value::Null
                        })
                } else {
                    row.try_get::<i64, _>(name)
                        .map(|v| Value::Number(v.into()))
                        .unwrap_or_else(|e| {
                            println!("Failed to get {} as i64: {:?}", name, e);
                            Value::Null
                        })
                }
            } else if type_info_debug.contains("VARSTRING") || type_info_debug.contains("VARCHAR") 
                || type_info_debug.contains("DATE") || type_info_debug.contains("TIME")
                || type_info_debug.contains("DECIMAL") || type_info_debug.contains("NUMERIC")
                || type_info_debug.contains("ENUM") || type_info_debug.contains("SET")  {
                row.try_get::<String, _>(name)
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("BLOB") {
                row.try_get::<Vec<u8>, _>(name)
                    .map(|v| Value::String(base64::engine::general_purpose::STANDARD.encode(v)))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("FLOAT") || type_info_debug.contains("DOUBLE") {
                row.try_get::<f64, _>(name)
                    .map(|v| serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("BIT") {
                row.try_get::<i64, _>(name)
                    .map(|v| Value::Bool(v != 0))
                    .unwrap_or(Value::Null)
            } else if type_info_debug.contains("JSON") {
                row.try_get::<String, _>(name)
                    .ok()
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
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

#[async_trait::async_trait]
impl DbRepository for PostgresRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        println!("SQL Query: {:?}", sql);
        // Query dari PostgreSQL
        Ok(vec![]) // dummy
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
