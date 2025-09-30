use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::database::state::{DbParam, DbRepository};
use crate::storage::ast::{Filter, Query, Val, JoinKind};
use crate::storage::traits::{BackendCapabilities, DataStore};

pub struct SqlStore {
    inner: Arc<dyn DbRepository>,
    db_type: String, // "mysql" | "postgres" | "sqlite" | "mssql"
}

/// Value type for INSERT fields supporting bound params and raw SQL expressions.
#[derive(Debug, Clone)]
pub enum InsertValue {
    /// A value to be bound as a parameter
    Param(DbParam),
    /// A raw SQL fragment (trusted, typically from config/query_converter)
    Raw(String),
    /// A raw SQL fragment containing `?` placeholders with its own params
    RawWithParams { sql: String, params: Vec<DbParam> },
}

impl SqlStore {
    pub fn new(inner: Arc<dyn DbRepository>, db_type: String) -> Self {
        Self { inner, db_type }
    }

    /// Compile the query into (SQL, params) for debugging or advanced use-cases.
    pub fn preview_sql(&self, q: &Query) -> (String, Vec<DbParam>) {
        self.compile_query(q)
    }

    fn next_placeholder(&self, i: usize) -> String {
        match self.db_type.as_str() {
            "postgres" => format!("${}", i),
            _ => "?".to_string(),
        }
    }

    fn compile_filter(&self, f: &Filter, params: &mut Vec<DbParam>, idx: &mut usize) -> String {
        match f {
            Filter::Eq(col, v) => {
                *idx += 1;
                params.push(to_param(v));
                format!("{} = {}", col, self.next_placeholder(*idx))
            }
            Filter::Ne(col, v) => {
                *idx += 1;
                params.push(to_param(v));
                format!("{} <> {}", col, self.next_placeholder(*idx))
            }
            Filter::Gt(col, v) => {
                *idx += 1;
                params.push(to_param(v));
                format!("{} > {}", col, self.next_placeholder(*idx))
            }
            Filter::Gte(col, v) => {
                *idx += 1;
                params.push(to_param(v));
                format!("{} >= {}", col, self.next_placeholder(*idx))
            }
            Filter::Lt(col, v) => {
                *idx += 1;
                params.push(to_param(v));
                format!("{} < {}", col, self.next_placeholder(*idx))
            }
            Filter::Lte(col, v) => {
                *idx += 1;
                params.push(to_param(v));
                format!("{} <= {}", col, self.next_placeholder(*idx))
            }
            Filter::Like(col, pat) => {
                *idx += 1;
                params.push(DbParam::Str(pat.clone()));
                format!("{} LIKE {}", col, self.next_placeholder(*idx))
            }
            Filter::ILike(col, pat) => {
                *idx += 1;
                params.push(DbParam::Str(pat.clone()));
                match self.db_type.as_str() {
                    "postgres" => format!("{} ILIKE {}", col, self.next_placeholder(*idx)),
                    _ => format!("LOWER({}) LIKE LOWER({})", col, self.next_placeholder(*idx)),
                }
            }
            Filter::NotLike(col, pat) => {
                *idx += 1;
                params.push(DbParam::Str(pat.clone()));
                format!("{} NOT LIKE {}", col, self.next_placeholder(*idx))
            }
            Filter::IsNull(col) => format!("{} IS NULL", col),
            Filter::IsNotNull(col) => format!("{} IS NOT NULL", col),
            Filter::In(col, xs) => {
                if xs.is_empty() {
                    return "1=0".into();
                }
                let mut phs = Vec::with_capacity(xs.len());
                for v in xs {
                    *idx += 1;
                    params.push(to_param(v));
                    phs.push(self.next_placeholder(*idx));
                }
                format!("{} IN ({})", col, phs.join(","))
            }
            Filter::NotIn(col, xs) => {
                if xs.is_empty() {
                    return "1=1".into();
                }
                let mut phs = Vec::with_capacity(xs.len());
                for v in xs {
                    *idx += 1;
                    params.push(to_param(v));
                    phs.push(self.next_placeholder(*idx));
                }
                format!("{} NOT IN ({})", col, phs.join(","))
            }
            Filter::Between(col, a, b) => {
                *idx += 1;
                params.push(to_param(a));
                let p1 = self.next_placeholder(*idx);
                *idx += 1;
                params.push(to_param(b));
                let p2 = self.next_placeholder(*idx);
                format!("{} BETWEEN {} AND {}", col, p1, p2)
            }
            Filter::And(fs) => {
                let inner = fs
                    .iter()
                    .map(|g| self.compile_filter(g, params, idx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                if inner.is_empty() { inner } else { format!("({})", inner) }
            }
            Filter::Or(fs) => {
                let inner = fs
                    .iter()
                    .map(|g| self.compile_filter(g, params, idx))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                if inner.is_empty() { inner } else { format!("({})", inner) }
            }
        }
    }

    fn compile_query(&self, q: &Query) -> (String, Vec<DbParam>) {
        let mut sql = String::new();
        let mut params = Vec::<DbParam>::new();
        let mut idx = 0usize;

        // SELECT
        if q.projection.is_empty() {
            sql.push_str(&format!("SELECT {}* FROM {}", if q.distinct {"DISTINCT "} else {""}, q.collection));
        } else {
            let cols = q.projection.join(",");
            sql.push_str(&format!("SELECT {}{} FROM {}", if q.distinct {"DISTINCT "} else {""}, cols, q.collection));
        }

        // JOINs
        if !q.joins.is_empty() {
            for j in &q.joins {
                match j.kind {
                    JoinKind::Inner => {
                        sql.push_str(&format!(" INNER JOIN {} ON {}", j.table, j.on));
                    }
                    JoinKind::Left => {
                        sql.push_str(&format!(" LEFT JOIN {} ON {}", j.table, j.on));
                    }
                }
            }
        }

        // WHERE
        if let Some(f) = &q.filter {
            let clause = self.compile_filter(f, &mut params, &mut idx);
            if !clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&clause);
            }
        }

        // GROUP BY
        if !q.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&q.group_by.join(", "));
        }

        // HAVING (raw-safe expressions from config)
        if !q.having_raw.is_empty() {
            sql.push_str(" HAVING ");
            sql.push_str(&q.having_raw.join(", "));
        }

        // ORDER
        if !q.sort.is_empty() {
            let ord = q
                .sort
                .iter()
                .map(|s| format!("{} {}", s.field, if s.asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>()
                .join(",");
            sql.push_str(" ORDER BY ");
            sql.push_str(&ord);
        }

        // LIMIT/OFFSET with MSSQL support
        match self.db_type.as_str() {
            "mssql" => {
                // MSSQL requires ORDER BY for OFFSET/FETCH, ensure exists
                if q.limit.is_some() || q.offset.is_some() {
                    if q.sort.is_empty() {
                        // Fallback ORDER BY 1 to satisfy syntax; caller should set a deterministic order
                        sql.push_str(" ORDER BY 1");
                    }
                    let off = q.offset.unwrap_or(0);
                    let lim = q.limit.unwrap_or(100);
                    sql.push_str(&format!(" OFFSET {} ROWS FETCH NEXT {} ROWS ONLY", off, lim));
                }
            }
            _ => {
                if let Some(l) = q.limit {
                    sql.push_str(&format!(" LIMIT {}", l));
                }
                if let Some(o) = q.offset {
                    sql.push_str(&format!(" OFFSET {}", o));
                }
            }
        }

        (sql, params)
    }

    fn build_insert(&self, collection: &str, doc: &Value) -> anyhow::Result<(String, Vec<DbParam>)> {
        let obj = doc
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("insert expects object"))?;
        let cols: Vec<_> = obj.keys().cloned().collect();
        let mut params = Vec::<DbParam>::new();
        let mut idx = 0usize;
        let placeholders: Vec<_> = obj
            .values()
            .map(|v| {
                idx += 1;
                params.push(json_to_param(v));
                self.next_placeholder(idx)
            })
            .collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            collection,
            cols.join(","),
            placeholders.join(",")
        );
        Ok((sql, params))
    }

    pub fn preview_insert(&self, collection: &str, doc: &Value) -> anyhow::Result<(String, Vec<DbParam>)> {
        self.build_insert(collection, doc)
    }

    /// Build an INSERT statement from explicit column-value pairs that may include expressions.
    pub fn preview_insert_with(
        &self,
        collection: &str,
        fields: &[(String, InsertValue)],
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        if fields.is_empty() {
            return Err(anyhow::anyhow!("insert fields cannot be empty"));
        }
        let mut params: Vec<DbParam> = Vec::new();
        let mut idx = 0usize; // for placeholder numbering in postgres

        let mut col_names: Vec<String> = Vec::with_capacity(fields.len());
        let mut val_frags: Vec<String> = Vec::with_capacity(fields.len());

        for (col, val) in fields.iter() {
            col_names.push(col.clone());
            match val {
                InsertValue::Param(p) => {
                    idx += 1;
                    params.push(p.clone());
                    val_frags.push(self.next_placeholder(idx));
                }
                InsertValue::Raw(sql) => {
                    val_frags.push(sql.clone());
                }
                InsertValue::RawWithParams { sql, params: p } => {
                    let reb = self.rebind_fragment(sql, p, &mut idx, &mut params);
                    val_frags.push(reb);
                }
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            collection,
            col_names.join(","),
            val_frags.join(",")
        );
        Ok((sql, params))
    }

    /// Build a multi-row INSERT with dialect-aware placeholders.
    /// `columns` are the final column names in order.
    /// `rows` is a slice of rows, each row a vector of InsertValue with the same length as columns.
    pub fn preview_insert_bulk(
        &self,
        collection: &str,
        columns: &[String],
        rows: &[Vec<InsertValue>],
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        if columns.is_empty() { return Err(anyhow::anyhow!("insert columns cannot be empty")); }
        if rows.is_empty() { return Err(anyhow::anyhow!("insert rows cannot be empty")); }
        // Validate uniform row length
        for (i, r) in rows.iter().enumerate() {
            if r.len() != columns.len() {
                return Err(anyhow::anyhow!(
                    "row {} has {} values but {} columns provided",
                    i, r.len(), columns.len()
                ));
            }
        }
        let mut params: Vec<DbParam> = Vec::new();
        let mut idx = 0usize; // for postgres placeholder numbering across all rows
        let mut values_groups: Vec<String> = Vec::with_capacity(rows.len());

        for r in rows.iter() {
            let mut frags: Vec<String> = Vec::with_capacity(r.len());
            for val in r.iter() {
                match val {
                    InsertValue::Param(p) => {
                        idx += 1;
                        params.push(p.clone());
                        frags.push(self.next_placeholder(idx));
                    }
                    InsertValue::Raw(sql) => {
                        frags.push(sql.clone());
                    }
                    InsertValue::RawWithParams { sql, params: p } => {
                        let reb = self.rebind_fragment(sql, p, &mut idx, &mut params);
                        frags.push(reb);
                    }
                }
            }
            values_groups.push(format!("({})", frags.join(",")));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            collection,
            columns.join(","),
            values_groups.join(", ")
        );
        Ok((sql, params))
    }

    fn rebind_fragment(
        &self,
        sql: &str,
        frag_params: &[DbParam],
        idx: &mut usize,
        params: &mut Vec<DbParam>,
    ) -> String {
        if frag_params.is_empty() {
            return sql.to_string();
        }
        let parts: Vec<&str> = sql.split('?').collect();
        let needed = parts.len().saturating_sub(1);
        // Safety: assume needed == frag_params.len(); if not, we bind as many as min
        let bind_count = std::cmp::min(needed, frag_params.len());
        let mut out = String::new();
        for i in 0..bind_count {
            out.push_str(parts[i]);
            *idx += 1;
            out.push_str(&self.next_placeholder(*idx));
            params.push(frag_params[i].clone());
        }
        // append remaining chunk(s)
        out.push_str(parts.get(bind_count).copied().unwrap_or(""));
        // If there are extra chunks beyond bind_count+1 (due to mismatch), append them raw
        for chunk in parts.into_iter().skip(bind_count + 1) {
            out.push_str(chunk);
        }
        out
    }

    fn build_call_procedure(
        &self,
        name: &str,
        param_len: usize,
        params: Vec<DbParam>,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        let proc_name = name.trim();
        if proc_name.is_empty() {
            return Err(anyhow::anyhow!("procedure name is empty"));
        }

        // Build placeholders according to dialect
        let placeholders = match self.db_type.as_str() {
            "postgres" => {
                // $1, $2, ...
                (1..=param_len).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ")
            }
            _ => {
                // ?, ?, ...
                if param_len == 0 { String::new() } else { vec!["?"; param_len].join(", ") }
            }
        };

        let sql = match self.db_type.as_str() {
            // MySQL / MariaDB
            "mysql" => format!("CALL {}({})", proc_name, placeholders),
            // PostgreSQL (procedures supported >= 11). If using functions, configure name accordingly.
            "postgres" => format!("CALL {}({})", proc_name, placeholders),
            // SQL Server uses EXEC
            "mssql" => {
                if placeholders.is_empty() {
                    format!("EXEC {}", proc_name)
                } else {
                    format!("EXEC {} {}", proc_name, placeholders)
                }
            }
            // SQLite (and others) do not support stored procedures
            other => return Err(anyhow::anyhow!("Stored procedures are not supported for backend: {}", other)),
        };

        Ok((sql, params))
    }

    pub fn preview_call_procedure(
        &self,
        name: &str,
        param_len: usize,
        params: Vec<DbParam>,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        self.build_call_procedure(name, param_len, params)
    }

    /// Compose an INSERT .. SELECT with dialect-aware upsert/merge support.
    /// - For MySQL/MariaDB: ON DUPLICATE KEY UPDATE col=VALUES(col), ...
    /// - For Postgres: ON CONFLICT (keys) DO UPDATE SET col=EXCLUDED.col, ...
    /// - For MSSQL: MERGE INTO ... USING (SELECT ...) AS s(..) ON (...) WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...
    /// - Others: fallback to plain INSERT .. SELECT (no upsert)
    pub fn preview_insert_select_upsert(
        &self,
        target: &str,
        insert_columns: &[String],
        select_sql: &str,
        conflict_columns: &[String],
        update_extra_assignments: &[String],
        params: Vec<DbParam>,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        let cols_joined = insert_columns.join(", ");
        let has_keys = !conflict_columns.is_empty();
        let mut sql: String;
        match self.db_type.as_str() {
            "mysql" => {
                sql = format!("INSERT INTO {} ({}) {}", target, cols_joined, select_sql);
                if has_keys || !update_extra_assignments.is_empty() {
                    // Build assignments
                    let mut assigns: Vec<String> = Vec::new();
                    for k in conflict_columns {
                        assigns.push(format!("{}=VALUES({})", k, k));
                    }
                    assigns.extend(update_extra_assignments.iter().cloned());
                    if !assigns.is_empty() {
                        sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", assigns.join(", ")));
                    }
                }
                Ok((sql, params))
            }
            "postgres" => {
                sql = format!("INSERT INTO {} ({}) {}", target, cols_joined, select_sql);
                if has_keys {
                    let mut assigns: Vec<String> = Vec::new();
                    for k in conflict_columns {
                        assigns.push(format!("{} = EXCLUDED.{}", k, k));
                    }
                    assigns.extend(update_extra_assignments.iter().cloned());
                    if !assigns.is_empty() {
                        sql.push_str(&format!(" ON CONFLICT ({}) DO UPDATE SET {}", conflict_columns.join(", "), assigns.join(", ")));
                    }
                }
                Ok((sql, params))
            }
            "mssql" => {
                if has_keys {
                    // Build MERGE statement
                    let src_cols = insert_columns.join(", ");
                    let on_clause = conflict_columns
                        .iter()
                        .map(|k| format!("t.{} = s.{}", k, k))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    // Update sets: keys (t.k = s.k) then extra assignments
                    let mut update_sets: Vec<String> = Vec::new();
                    for k in conflict_columns {
                        update_sets.push(format!("t.{} = s.{}", k, k));
                    }
                    update_sets.extend(update_extra_assignments.iter().cloned());
                    let insert_values = insert_columns
                        .iter()
                        .map(|c| format!("s.{}", c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql = format!(
                        "MERGE INTO {target} AS t USING ({select}) AS s ({src_cols}) ON {on} \
                         WHEN MATCHED THEN UPDATE SET {upd} \
                         WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});",
                        target = target,
                        select = select_sql,
                        src_cols = src_cols,
                        on = on_clause,
                        upd = update_sets.join(", "),
                        cols = cols_joined,
                        vals = insert_values,
                    );
                    Ok((sql, params))
                } else {
                    // No keys -> plain insert-select
                    sql = format!("INSERT INTO {} ({}) {}", target, cols_joined, select_sql);
                    Ok((sql, params))
                }
            }
            _ => {
                // Fallback: plain insert-select (no upsert)
                sql = format!("INSERT INTO {} ({}) {}", target, cols_joined, select_sql);
                Ok((sql, params))
            }
        }
    }

    fn build_update(
        &self,
        collection: &str,
        filter: Option<&Filter>,
        patch: &Value,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        let obj = patch
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("update expects object"))?;
        let mut params = Vec::<DbParam>::new();
        let mut idx = 0usize;
        let sets: Vec<_> = obj
            .iter()
            .map(|(k, v)| {
                idx += 1;
                params.push(json_to_param(v));
                format!("{} = {}", k, self.next_placeholder(idx))
            })
            .collect();
        let mut sql = format!("UPDATE {} SET {}", collection, sets.join(","));
        if let Some(f) = filter {
            let clause = self.compile_filter(f, &mut params, &mut idx);
            sql.push_str(" WHERE ");
            sql.push_str(&clause);
        }
        Ok((sql, params))
    }

    pub fn preview_update(
        &self,
        collection: &str,
        filter: Option<&Filter>,
        patch: &Value,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        self.build_update(collection, filter, patch)
    }

    /// Build an UPDATE statement from explicit column-value pairs that may include expressions.
    pub fn preview_update_with(
        &self,
        collection: &str,
        filter: Option<&Filter>,
        fields: &[(String, InsertValue)],
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        if fields.is_empty() {
            return Err(anyhow::anyhow!("update fields cannot be empty"));
        }
        let mut params: Vec<DbParam> = Vec::new();
        let mut idx = 0usize;

        let sets: Vec<String> = fields
            .iter()
            .map(|(k, v)| match v {
                InsertValue::Param(p) => {
                    idx += 1;
                    params.push(p.clone());
                    format!("{} = {}", k, self.next_placeholder(idx))
                }
                InsertValue::Raw(sql) => format!("{} = {}", k, sql),
                InsertValue::RawWithParams { sql, params: p } => {
                    let reb = self.rebind_fragment(sql, p, &mut idx, &mut params);
                    format!("{} = {}", k, reb)
                }
            })
            .collect();

        let mut sql = format!("UPDATE {} SET {}", collection, sets.join(","));
        if let Some(f) = filter {
            let clause = self.compile_filter(f, &mut params, &mut idx);
            if !clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&clause);
            }
        }
        Ok((sql, params))
    }

    fn build_delete(
        &self,
        collection: &str,
        filter: Option<&Filter>,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        let mut params = Vec::<DbParam>::new();
        let mut idx = 0usize;
        let mut sql = format!("DELETE FROM {}", collection);
        if let Some(f) = filter {
            let clause = self.compile_filter(f, &mut params, &mut idx);
            sql.push_str(" WHERE ");
            sql.push_str(&clause);
        }
        Ok((sql, params))
    }

    pub fn preview_delete(
        &self,
        collection: &str,
        filter: Option<&Filter>,
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        self.build_delete(collection, filter)
    }
}

fn to_param(v: &Val) -> DbParam {
    match v {
        Val::I64(x) => DbParam::I64(*x),
        Val::F64(x) => DbParam::F64(*x),
        Val::Bool(b) => DbParam::Bool(*b),
        Val::Str(s) => DbParam::Str(s.clone()),
        Val::Null => DbParam::Null,
    }
}

fn json_to_param(v: &Value) -> DbParam {
    match v {
        Value::Null => DbParam::Null,
        Value::Bool(b) => DbParam::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DbParam::I64(i)
            } else if let Some(f) = n.as_f64() {
                DbParam::F64(f)
            } else {
                DbParam::Str(n.to_string())
            }
        }
        Value::String(s) => DbParam::Str(s.clone()),
        _ => DbParam::Str(v.to_string()),
    }
}

#[async_trait]
impl DataStore for SqlStore {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            transactions: true,
            like: true,
            sql_formula: true,
            joins: true,
        }
    }

    async fn query(&self, q: &Query) -> anyhow::Result<Vec<Value>> {
        let (sql, params) = self.compile_query(q);
        self.inner.query_with_params(&sql, params).await
    }

    async fn insert(&self, collection: &str, doc: Value) -> anyhow::Result<Value> {
        let (sql, params) = self.build_insert(collection, &doc)?;
        let _ = self.inner.query_with_params(&sql, params).await?;
        Ok(Value::Null) // optionally return inserted id
    }

    async fn update(
        &self,
        collection: &str,
        filter: Option<Filter>,
        patch: Value,
    ) -> anyhow::Result<u64> {
        let (sql, params) = self.build_update(collection, filter.as_ref(), &patch)?;
        let _ = self.inner.query_with_params(&sql, params).await?;
        Ok(1)
    }

    async fn delete(&self, collection: &str, filter: Option<Filter>) -> anyhow::Result<u64> {
        let (sql, params) = self.build_delete(collection, filter.as_ref())?;
        let _ = self.inner.query_with_params(&sql, params).await?;
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;

    struct MockRepo;

    #[async_trait]
    impl DbRepository for MockRepo {
        async fn query(&self, _sql: &str) -> anyhow::Result<Vec<Value>, anyhow::Error> {
            Ok(vec![])
        }
        async fn query_with_params(
            &self,
            sql: &str,
            params: Vec<DbParam>,
        ) -> anyhow::Result<Vec<Value>, anyhow::Error> {
            // Echo back for assertions
            Ok(vec![json!({ "sql": sql, "params": format!("{:?}", params) })])
        }
        
        async fn begin_transaction(&self) -> anyhow::Result<Box<dyn crate::database::state::DbTransaction>, anyhow::Error> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn compiles_basic_select_with_filter_and_sort() {
        let repo: Arc<dyn DbRepository> = Arc::new(MockRepo);
        let store = SqlStore::new(repo, "mysql".to_string());
        let q = crate::storage::ast::Query::from("flx_users")
            .select(["id", "email"]) 
            .r#where(crate::storage::ast::Filter::Eq(
                "status".to_string(),
                crate::storage::ast::Val::Str("active".to_string()),
            ))
            .order_by("id", true)
            .limit(10)
            .offset(5);

        let rows = store.query(&q).await.unwrap();
        let first = rows.first().unwrap();
        let sql = first.get("sql").and_then(|v| v.as_str()).unwrap();
        assert!(sql.contains("SELECT id,email FROM flx_users"));
        assert!(sql.contains("WHERE status = ?"));
        assert!(sql.contains("ORDER BY id ASC"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 5"));
    }

    #[tokio::test]
    async fn compiles_in_and_is_null() {
        let repo: Arc<dyn DbRepository> = Arc::new(MockRepo);
        let store = SqlStore::new(repo, "postgres".to_string());
        use crate::storage::ast::{Filter as F, Val as V};
        let q = crate::storage::ast::Query::from("orders").r#where(F::And(vec![
            F::In("status".into(), vec![V::Str("new".into()), V::Str("paid".into())]),
            F::IsNull("deleted_at".into()),
        ]));
        let rows = store.query(&q).await.unwrap();
        let first = rows.first().unwrap();
        let sql = first.get("sql").and_then(|v| v.as_str()).unwrap();
        assert!(sql.contains("status IN ($1,$2)"));
        assert!(sql.contains("deleted_at IS NULL"));
    }
}
