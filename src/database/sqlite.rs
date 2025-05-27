use base64::Engine;
use serde_json::{Map, Value};
use sqlx::{sqlite::SqliteRow, Column, Row};
use sqlx::SqlitePool;

use super::state::DbRepository;

pub struct SqliteRepo {
    pub pool: SqlitePool,
}

pub fn sqliterows_to_json(rows: Vec<SqliteRow>) -> Vec<Value> {
    let mut json_array = Vec::new();

    for row in rows {
        let mut obj = Map::new();

        for column in row.columns() {
            let name = column.name();
            let type_info_debug = format!("{:?}", column.type_info()).to_uppercase();

            let value = if type_info_debug.contains("INT") {
                match row.try_get::<Option<i64>, _>(name) {
                    Ok(Some(v)) => Value::Number(v.into()),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("REAL") || type_info_debug.contains("FLOAT") || type_info_debug.contains("DOUBLE") {
                match row.try_get::<Option<f64>, _>(name) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("TEXT") || type_info_debug.contains("CHAR") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("BLOB") {
                match row.try_get::<Option<Vec<u8>>, _>(name) {
                    Ok(Some(v)) => Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            } else if type_info_debug.contains("DATE") || type_info_debug.contains("TIME") {
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    _ => Value::Null,
                }
            } else {
                // fallback
                match row.try_get::<Option<String>, _>(name) {
                    Ok(Some(v)) => Value::String(v),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            };

            obj.insert(name.to_string(), value);
        }

        json_array.push(Value::Object(obj));
    }

    json_array
}

#[async_trait::async_trait]
impl DbRepository for SqliteRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // jalankan query dan ambil hasilnya ke dalam Value
        match sqlx::query(sql)
            .fetch_all(&self.pool)
            .await {
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

    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error> {
        // Menghitung total baris dari tabel
        let row: (i32,) = sqlx::query_as(sql)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }
}
