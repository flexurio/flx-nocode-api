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
    // Early return
    if rows.is_empty() { return Vec::new(); }

    // Column meta classification to avoid per-row format!("{:?}") cost.
    #[derive(Clone, Copy)]
    enum Kind { ULong, Long, VarBinaryText, VarStringLike, DateTime, Timestamp, Time, VarBinary, Blob, Float, TinyBit, Json, Other }
    use Kind::*;

    // Build meta from first row (all rows share schema)
    let first = &rows[0];
    let mut metas: Vec<(String, Kind)> = Vec::with_capacity(first.columns().len());
    for c in first.columns() {
        let name = c.name().to_string();
        let dbg = format!("{:?}", c.type_info()).to_ascii_lowercase();
        let is_unsigned = dbg.contains("unsigned");
        let has_binary = dbg.contains("binary");
        let k = if dbg.contains("long") { if is_unsigned { ULong } else { Long } }
            else if (dbg.contains("varstring") || dbg.contains("varchar")) && has_binary { VarBinaryText }
            else if dbg.contains("varstring") || dbg.contains("varchar") || dbg.contains("decimal") || dbg.contains("numeric") || dbg.contains("enum") || dbg.contains("set") { VarStringLike }
            else if dbg.contains("datetime") { DateTime }
            else if dbg.contains("timestamp") { Timestamp }
            else if dbg.contains(" time") || dbg == "time" { Time }
            else if dbg.contains("varbinary") || (dbg.contains("binary") && !dbg.contains("varstring") && !dbg.contains("varchar") && !dbg.contains("datetime") && !dbg.contains("timestamp") && !dbg.contains(" time")) { VarBinary }
            else if dbg.contains("blob") { Blob }
            else if dbg.contains("float") || dbg.contains("double") { Float }
            else if dbg.contains("tiny") || dbg.contains("bit") { TinyBit }
            else if dbg.contains("json") { Json }
            else { Other };
        metas.push((name, k));
    }

    let mut out: Vec<Value> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut obj = sonic_rs::Object::with_capacity(metas.len());
        for (col_name, k) in metas.iter() {
            let v = match k {
                ULong => match row.try_get::<Option<u64>, _>(col_name.as_str()) { Ok(Some(v)) => Value::from(v), Ok(None) => Value::default(), Err(e) => { if *ISDEBUG { eprintln!("[mysql conv] get {} as u64: {:?}", col_name, e); } Value::default() } },
                Long => match row.try_get::<Option<i64>, _>(col_name.as_str()) { Ok(Some(v)) => Value::from(v), Ok(None) => Value::default(), Err(e) => { if *ISDEBUG { eprintln!("[mysql conv] get {} as i64: {:?}", col_name, e); } Value::default() } },
                VarBinaryText => match row.try_get::<Option<Vec<u8>>, _>(col_name.as_str()) { Ok(Some(bytes)) => match String::from_utf8(bytes.clone()) { Ok(s) => value_from_string(s), Err(_) => value_from_string(base64::engine::general_purpose::STANDARD.encode(bytes)) }, Ok(None) => Value::default(), Err(_) => Value::default() },
                VarStringLike => match row.try_get::<Option<String>, _>(col_name.as_str()) { Ok(Some(s)) => value_from_string(s), Ok(None) => Value::default(), Err(_) => Value::default() },
                DateTime => match row.try_get::<Option<chrono::NaiveDateTime>, _>(col_name.as_str()) { Ok(Some(dt)) => Value::from(dt.to_string().as_str()), _ => Value::default() },
                Timestamp => match row.try_get::<Option<chrono::DateTime<chrono::Local>>, _>(col_name.as_str()) { Ok(Some(dt)) => value_from_string(dt.to_rfc3339()), Ok(None) => Value::default(), Err(_) => match row.try_get::<Option<chrono::NaiveDateTime>, _>(col_name.as_str()) { Ok(Some(dt)) => Value::from(dt.to_string().as_str()), _ => Value::default() } },
                Time => match row.try_get::<Option<chrono::NaiveTime>, _>(col_name.as_str()) { Ok(Some(t)) => Value::from(t.to_string().as_str()), _ => Value::default() },
                VarBinary | Blob => match row.try_get::<Option<Vec<u8>>, _>(col_name.as_str()) { Ok(Some(bytes)) => value_from_string(base64::engine::general_purpose::STANDARD.encode(bytes)), _ => Value::default() },
                Float => match row.try_get::<Option<f64>, _>(col_name.as_str()) { Ok(Some(f)) => value_from_f64(f), _ => Value::default() },
                TinyBit => match row.try_get::<Option<i32>, _>(col_name.as_str()) { Ok(Some(i)) => Value::from(i), _ => Value::default() },
                Json => row.try_get::<String, _>(col_name.as_str()).ok().and_then(|s| sonic_rs::from_str::<Value>(&s).ok()).unwrap_or(Value::default()),
                Other => row.try_get::<String, _>(col_name.as_str())
                    .map(value_from_string)
                    .unwrap_or_else(|_| {
                        row.try_get::<Option<Vec<u8>>, _>(col_name.as_str())
                            .map(|opt_bytes| opt_bytes.map(|b| value_from_string(base64::engine::general_purpose::STANDARD.encode(b))).unwrap_or_default())
                            .unwrap_or_default()
                    }),
            };
            obj.insert(col_name.as_str(), v);
        }
        out.push(Value::from(obj));
    }
    out.shrink_to_fit();
    out
}

#[async_trait::async_trait]
impl DbRepository for MySqlRepo {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error> {
        // jalankan query dan ambil hasilnya ke dalam Value
        match sqlx::query(sql).fetch_all(&self.pool).await {
            Ok(rows) => {
                // Jika berhasil, konversi hasilnya ke dalam JSON
                let rows: Vec<MySqlRow> = rows.into_iter().collect();
                let vec_val = mysqlrows_to_json(rows);
                return Ok(vec_val);
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
                let vec_val = mysqlrows_to_json(rows);
                Ok(vec_val)
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
                let vec_val = mysqlrows_to_json(rows);
                Ok(vec_val)
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
