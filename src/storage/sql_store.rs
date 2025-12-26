use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::database::state::{DbParam, DbRepository};
use crate::storage::ast::{Filter, Query, Val, JoinKind, Expr, Agg, AggFunc};
use crate::storage::traits::{BackendCapabilities, DataStore, TxStore};
use crate::storage::ddl::*;

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

    pub fn dialect(&self) -> &str { self.db_type.as_str() }

    /// Compile the query into (SQL, params) for debugging or advanced use-cases.
    pub fn preview_sql(&self, q: &Query) -> (String, Vec<DbParam>) {
        compile_query_with_dialect(&self.db_type, q)
    }

    fn compile_default_expr(&self, d: &DefaultExpr) -> String {
        match d {
            DefaultExpr::CurrentTimestamp => match self.db_type.as_str() {
                "sqlite" => "CURRENT_TIMESTAMP".into(),
                "mssql" => "GETDATE()".into(),
                _ => "CURRENT_TIMESTAMP".into(),
            },
            DefaultExpr::Now => match self.db_type.as_str() {
                "mssql" => "GETDATE()".into(),
                "sqlite" => "CURRENT_TIMESTAMP".into(),
                _ => "NOW()".into(),
            },
            DefaultExpr::Raw(s) => s.clone(),
        }
    }

    fn fk_action_sql(&self, a: &crate::storage::ddl::ForeignAction) -> &'static str {
        match a {
            crate::storage::ddl::ForeignAction::Cascade => "CASCADE",
            crate::storage::ddl::ForeignAction::SetNull => "SET NULL",
            crate::storage::ddl::ForeignAction::Restrict => "RESTRICT",
            crate::storage::ddl::ForeignAction::NoAction => "NO ACTION",
        }
    }

    fn compile_create_table(&self, ct: &CreateTable) -> String {
        // Fallback to combined statements using the separate builder
        let (table_sql, index_sqls) = self.compile_create_table_separate(ct);
        let mut sql = table_sql;
        for ix in index_sqls {
            sql.push('\n');
            sql.push_str(&ix);
        }
        sql
    }

    fn compile_create_table_separate(&self, ct: &CreateTable) -> (String, Vec<String>) {
        // Column lines
        let mut col_lines: Vec<String> = Vec::with_capacity(ct.columns.len());
        for c in &ct.columns {
            let mut line = format!("{} ", c.name);
            match &c.col_type {
                ColumnType::Raw(t) => line.push_str(t),
            }
            if let Some(coll) = &c.collate
                && !coll.trim().is_empty() {
                    line.push_str(" COLLATE ");
                    line.push_str(coll.trim());
                }
            if !c.nullable { line.push_str(" NOT NULL"); }
            if let Some(def) = &c.default {
                line.push_str(" DEFAULT ");
                line.push_str(&self.compile_default_expr(def));
            }
            if c.auto_increment {
                // best-effort: most dialects encode autoinc in type; for MySQL we may append AUTO_INCREMENT
                if self.db_type == "mysql" {
                    line.push_str(" AUTO_INCREMENT");
                }
            }
            if c.primary_key_inline {
                line.push_str(" PRIMARY KEY");
                if self.db_type == "sqlite" && matches!(c.col_type, ColumnType::Raw(ref t) if t.eq_ignore_ascii_case("INTEGER")) {
                    // If caller wants AUTOINCREMENT they should specify it in col_type raw
                }
            }
            col_lines.push(line);
        }

        // Table constraints
        let mut indexes_to_emit: Vec<(Option<String>, Vec<String>, bool)> = vec![];
        // For MSSQL, emit FKs as separate ALTER TABLE statements guarded by existence checks
    #[allow(clippy::type_complexity)]
    let mut fks_to_emit_mssql: Vec<(Option<String>, Vec<String>, String, Vec<String>, Option<crate::storage::ddl::ForeignAction>, Option<crate::storage::ddl::ForeignAction>)> = vec![];
        for cons in &ct.constraints {
            match cons {
                TableConstraint::PrimaryKey { columns } => {
                    col_lines.push(format!("PRIMARY KEY ({})", columns.join(", ")));
                }
                TableConstraint::Unique { name, columns } => {
                    if let Some(nm) = name { col_lines.push(format!("CONSTRAINT {} UNIQUE ({})", nm, columns.join(", "))); }
                    else { col_lines.push(format!("UNIQUE ({})", columns.join(", "))); }
                }
                TableConstraint::Index { name, columns, unique } => {
                    // Emit as separate CREATE INDEX after CREATE TABLE for portability
                    indexes_to_emit.push((name.clone(), columns.clone(), *unique));
                }
                TableConstraint::ForeignKey { name, columns, ref_table, ref_columns, on_delete, on_update } => {
                    if self.db_type == "mssql" {
                        fks_to_emit_mssql.push((name.clone(), columns.clone(), ref_table.clone(), ref_columns.clone(), on_delete.clone(), on_update.clone()));
                    } else {
                        let mut fk = String::new();
                        if let Some(nm) = name { fk.push_str(&format!("CONSTRAINT {} ", nm)); }
                        fk.push_str(&format!(
                            "FOREIGN KEY ({}) REFERENCES {}({})",
                            columns.join(", "),
                            ref_table,
                            ref_columns.join(", ")
                        ));
                        if let Some(act) = on_delete {
                            fk.push_str(" ON DELETE ");
                            fk.push_str(self.fk_action_sql(act));
                        }
                        if let Some(act) = on_update {
                            fk.push_str(" ON UPDATE ");
                            fk.push_str(self.fk_action_sql(act));
                        }
                        col_lines.push(fk);
                    }
                }
            }
        }

        let if_not_exists = match self.db_type.as_str() {
            "mysql" | "postgres" | "sqlite" => if ct.if_not_exists { " IF NOT EXISTS" } else { "" },
            _ => "", // MSSQL handled with IF NOT EXISTS wrapper
        };
        let mut sql = String::new();
        if self.db_type == "mssql" && ct.if_not_exists {
            sql.push_str(&format!(
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{}' AND xtype='U')\nBEGIN\n",
                ct.name
            ));
        }
        sql.push_str(&format!(
            "CREATE TABLE{} {} (\n    {}\n);",
            if_not_exists,
            ct.name,
            col_lines.join(",\n    ")
        ));
        if self.db_type == "mssql" && ct.if_not_exists {
            sql.push_str("\nEND");
        }
        // Collect statements after table creation (FKs first for MSSQL, then indexes)
        let mut index_sqls: Vec<String> = vec![];
        if self.db_type == "mssql" {
            for (name_opt, cols, ref_tbl, ref_cols, on_del, on_upd) in fks_to_emit_mssql.into_iter() {
                let fk_name = name_opt.unwrap_or_else(|| format!("fk_{}_{}_{}", ct.name, cols.join("_"), ref_tbl));
                let mut stmt = format!(
                    "IF OBJECT_ID(N'{}', N'U') IS NOT NULL AND OBJECT_ID(N'{}', N'U') IS NOT NULL\nBEGIN\n    ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({})",
                    ct.name, ref_tbl, ct.name, fk_name, cols.join(", "), ref_tbl, ref_cols.join(", ")
                );
                if let Some(act) = on_del { stmt.push_str(&format!(" ON DELETE {}", self.fk_action_sql(&act))); }
                if let Some(act) = on_upd { stmt.push_str(&format!(" ON UPDATE {}", self.fk_action_sql(&act))); }
                stmt.push_str(";\nEND");
                index_sqls.push(stmt);
            }
        }
        for (name_opt, cols, unique) in indexes_to_emit.into_iter() {
            let ix_name = name_opt.unwrap_or_else(|| format!("idx_{}_{}", ct.name, cols.join("_")));
            let uniq = if unique { "UNIQUE " } else { "" };
            let stmt = match self.db_type.as_str() {
                "postgres" | "sqlite" => format!(
                    "CREATE {uniq}INDEX IF NOT EXISTS {ix} ON {tbl} ({cols});",
                    uniq=uniq,
                    ix=ix_name,
                    tbl=ct.name,
                    cols=cols.join(", ")
                ),
                "mssql" => format!(
                    "IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = '{ix}' AND object_id = OBJECT_ID('{tbl}'))\nBEGIN\n    CREATE {uniq}INDEX {ix} ON {tbl} ({cols});\nEND",
                    uniq=uniq,
                    ix=ix_name,
                    tbl=ct.name,
                    cols=cols.join(", ")
                ),
                _ => format!(
                    "CREATE {uniq}INDEX {ix} ON {tbl} ({cols});",
                    uniq=uniq,
                    ix=ix_name,
                    tbl=ct.name,
                    cols=cols.join(", ")
                ),
            };
            index_sqls.push(stmt);
        }
        (sql, index_sqls)
    }

    pub fn preview_ddl(&self, d: &Ddl) -> String {
        match d {
            Ddl::CreateTable(ct) => self.compile_create_table(ct),
        }
    }

    pub fn preview_ddl_separate(&self, d: &Ddl) -> (String, Vec<String>) {
        match d {
            Ddl::CreateTable(ct) => self.compile_create_table_separate(ct),
        }
    }

    fn build_insert(&self, collection: &str, doc: &Value) -> anyhow::Result<(String, Vec<DbParam>)> {
        build_insert_with_dialect(&self.db_type, collection, doc)
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
                    val_frags.push(next_placeholder_for(&self.db_type, idx));
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
                        frags.push(next_placeholder_for(&self.db_type, idx));
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

    /// Build an INSERT statement with a dialect-aware RETURNING of specified columns.
    /// - Postgres/SQLite: appends `RETURNING col1, col2`
    /// - MSSQL: injects `OUTPUT INSERTED.col1, INSERTED.col2`
    /// - Others: returns plain INSERT (no returning support)
    pub fn preview_insert_with_returning(
        &self,
        collection: &str,
        fields: &[(String, InsertValue)],
        returning_cols: &[&str],
    ) -> anyhow::Result<(String, Vec<DbParam>)> {
        if fields.is_empty() { return Err(anyhow::anyhow!("insert fields cannot be empty")); }

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
                    val_frags.push(next_placeholder_for(&self.db_type, idx));
                }
                InsertValue::Raw(sql) => { val_frags.push(sql.clone()); }
                InsertValue::RawWithParams { sql, params: p } => {
                    let reb = self.rebind_fragment(sql, p, &mut idx, &mut params);
                    val_frags.push(reb);
                }
            }
        }

        let base_cols = col_names.join(",");
        let base_vals = val_frags.join(",");

        let sql = match self.db_type.as_str() {
            // Inject OUTPUT between INSERT and VALUES
            "mssql" => {
                let outs = if returning_cols.is_empty() {
                    String::new()
                } else {
                    let list = returning_cols.iter().map(|c| format!("INSERTED.{}", c)).collect::<Vec<_>>().join(", ");
                    format!(" OUTPUT {}", list)
                };
                format!("INSERT INTO {} ({}){} VALUES ({})", collection, base_cols, outs, base_vals)
            }
            // Append RETURNING at the end
            "postgres" | "sqlite" => {
                if returning_cols.is_empty() {
                    format!("INSERT INTO {} ({}) VALUES ({})", collection, base_cols, base_vals)
                } else {
                    format!(
                        "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
                        collection,
                        base_cols,
                        base_vals,
                        returning_cols.join(", ")
                    )
                }
            }
            _ => {
                // Fallback: no returning support
                format!("INSERT INTO {} ({}) VALUES ({})", collection, base_cols, base_vals)
            }
        };

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
            out.push_str(&next_placeholder_for(&self.db_type, *idx));
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

    #[allow(dead_code)]
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
                format!("{} = {}", k, next_placeholder_for(&self.db_type, idx))
            })
            .collect();
        let mut sql = format!("UPDATE {} SET {}", collection, sets.join(","));
        if let Some(f) = filter {
            let clause = compile_filter_for(&self.db_type, f, &mut params, &mut idx);
            sql.push_str(" WHERE ");
            sql.push_str(&clause);
        }
        Ok((sql, params))
    }

    #[allow(dead_code)]
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
                    format!("{} = {}", k, next_placeholder_for(&self.db_type, idx))
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
            let clause = compile_filter_for(&self.db_type, f, &mut params, &mut idx);
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
            let clause = compile_filter_for(&self.db_type, f, &mut params, &mut idx);
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
        let (sql, params) = compile_query_with_dialect(&self.db_type, q);
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

    async fn begin_tx(&self) -> anyhow::Result<Box<dyn TxStore>> {
        let tx = self.inner.begin_transaction().await?;
        Ok(Box::new(SqlTxStore { 
            db_type: self.db_type.clone(),
            tx,
        }))
    }
}

struct SqlTxStore {
    db_type: String,
    tx: Box<dyn crate::database::state::DbTransaction>,
}

#[async_trait::async_trait]
impl TxStore for SqlTxStore {
    async fn query(&mut self, q: &Query) -> anyhow::Result<Vec<serde_json::Value>> {
        let (sql, params) = compile_query_with_dialect(&self.db_type, q);
    self.tx.query_with_params(&sql, params).await
    }

    async fn insert(&mut self, collection: &str, doc: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let (sql, params) = build_insert_with_dialect(&self.db_type, collection, &doc)?;
        let _ = self.tx.query_with_params(&sql, params).await?;
        Ok(serde_json::Value::Null)
    }

    async fn update(&mut self, collection: &str, filter: Option<Filter>, patch: serde_json::Value) -> anyhow::Result<u64> {
        let (sql, params) = build_update_with_dialect(&self.db_type, collection, filter.as_ref(), &patch)?;
        let _ = self.tx.query_with_params(&sql, params).await?;
        Ok(1)
    }

    async fn delete(&mut self, collection: &str, filter: Option<Filter>) -> anyhow::Result<u64> {
        let (sql, params) = build_delete_with_dialect(&self.db_type, collection, filter.as_ref())?;
        let _ = self.tx.query_with_params(&sql, params).await?;
        Ok(1)
    }

    async fn raw_sql(&mut self, sql: &str, params: Vec<DbParam>) -> anyhow::Result<Vec<serde_json::Value>> {
    self.tx.query_with_params(sql, params).await
    }

    async fn commit(self: Box<Self>) -> anyhow::Result<()> {
    self.tx.commit().await
    }

    async fn rollback(self: Box<Self>) -> anyhow::Result<()> {
    self.tx.rollback().await
    }
}

// === Dialect-parametrized helpers (allow reuse without self/Arc lifetimes) ===
fn next_placeholder_for(db_type: &str, i: usize) -> String {
    match db_type {
        "postgres" => format!("${}", i),
        _ => "?".to_string(),
    }
}

fn compile_filter_for(db_type: &str, f: &Filter, params: &mut Vec<DbParam>, idx: &mut usize) -> String {
    match f {
        Filter::Eq(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} = {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::Ne(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} <> {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::Gt(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} > {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::Gte(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} >= {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::Lt(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} < {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::Lte(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} <= {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::Like(col, pat) => { *idx += 1; params.push(DbParam::Str(pat.clone())); format!("{} LIKE {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::ILike(col, pat) => {
            *idx += 1; params.push(DbParam::Str(pat.clone()));
            if db_type == "postgres" { format!("{} ILIKE {}", col, next_placeholder_for(db_type, *idx)) }
            else { format!("LOWER({}) LIKE LOWER({})", col, next_placeholder_for(db_type, *idx)) }
        }
        Filter::NotLike(col, pat) => { *idx += 1; params.push(DbParam::Str(pat.clone())); format!("{} NOT LIKE {}", col, next_placeholder_for(db_type, *idx)) }
        Filter::IsNull(col) => format!("{} IS NULL", col),
        Filter::IsNotNull(col) => format!("{} IS NOT NULL", col),
        Filter::In(col, xs) => {
            if xs.is_empty() { return "1=0".into(); }
            let mut phs = Vec::with_capacity(xs.len());
            for v in xs { *idx += 1; params.push(to_param(v)); phs.push(next_placeholder_for(db_type, *idx)); }
            format!("{} IN ({})", col, phs.join(","))
        }
        Filter::NotIn(col, xs) => {
            if xs.is_empty() { return "1=1".into(); }
            let mut phs = Vec::with_capacity(xs.len());
            for v in xs { *idx += 1; params.push(to_param(v)); phs.push(next_placeholder_for(db_type, *idx)); }
            format!("{} NOT IN ({})", col, phs.join(","))
        }
        Filter::Between(col, a, b) => {
            *idx += 1; params.push(to_param(a)); let p1 = next_placeholder_for(db_type, *idx);
            *idx += 1; params.push(to_param(b)); let p2 = next_placeholder_for(db_type, *idx);
            format!("{} BETWEEN {} AND {}", col, p1, p2)
        }
        Filter::And(fs) => {
            let inner = fs.iter().map(|g| compile_filter_for(db_type, g, params, idx)).collect::<Vec<_>>().join(" AND ");
            if inner.is_empty() { inner } else { format!("({})", inner) }
        }
        Filter::Or(fs) => {
            let inner = fs.iter().map(|g| compile_filter_for(db_type, g, params, idx)).collect::<Vec<_>>().join(" OR ");
            if inner.is_empty() { inner } else { format!("({})", inner) }
        }
    }
}

// Compile boolean Expr trees (for JOIN ON and HAVING) into SQL with bound params
fn compile_expr_for(db_type: &str, e: &Expr, params: &mut Vec<DbParam>, idx: &mut usize) -> String {
    match e {
        // Column vs literal value
        Expr::Eq(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} = {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::Ne(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} <> {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::Gt(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} > {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::Gte(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} >= {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::Lt(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} < {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::Lte(col, v) => { *idx += 1; params.push(to_param(v)); format!("{} <= {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::Like(col, pat) => { *idx += 1; params.push(DbParam::Str(pat.clone())); format!("{} LIKE {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::ILike(col, pat) => {
            *idx += 1; params.push(DbParam::Str(pat.clone()));
            if db_type == "postgres" { format!("{} ILIKE {}", col, next_placeholder_for(db_type, *idx)) }
            else { format!("LOWER({}) LIKE LOWER({})", col, next_placeholder_for(db_type, *idx)) }
        }
        Expr::NotLike(col, pat) => { *idx += 1; params.push(DbParam::Str(pat.clone())); format!("{} NOT LIKE {}", col, next_placeholder_for(db_type, *idx)) }
        Expr::In(col, xs) => {
            if xs.is_empty() { return "1=0".into(); }
            let mut phs = Vec::with_capacity(xs.len());
            for v in xs { *idx += 1; params.push(to_param(v)); phs.push(next_placeholder_for(db_type, *idx)); }
            format!("{} IN ({})", col, phs.join(","))
        }
        Expr::NotIn(col, xs) => {
            if xs.is_empty() { return "1=1".into(); }
            let mut phs = Vec::with_capacity(xs.len());
            for v in xs { *idx += 1; params.push(to_param(v)); phs.push(next_placeholder_for(db_type, *idx)); }
            format!("{} NOT IN ({})", col, phs.join(","))
        }
        Expr::Between(col, a, b) => {
            *idx += 1; params.push(to_param(a)); let p1 = next_placeholder_for(db_type, *idx);
            *idx += 1; params.push(to_param(b)); let p2 = next_placeholder_for(db_type, *idx);
            format!("{} BETWEEN {} AND {}", col, p1, p2)
        }
        // Column vs column
        Expr::ColEq(a, b) => format!("{} = {}", a, b),
        Expr::ColNe(a, b) => format!("{} <> {}", a, b),
        Expr::ColGt(a, b) => format!("{} > {}", a, b),
        Expr::ColGte(a, b) => format!("{} >= {}", a, b),
        Expr::ColLt(a, b) => format!("{} < {}", a, b),
        Expr::ColLte(a, b) => format!("{} <= {}", a, b),
        // Composition
        Expr::And(xs) => {
            let inner = xs.iter().map(|x| compile_expr_for(db_type, x, params, idx)).collect::<Vec<_>>().join(" AND ");
            if inner.is_empty() { inner } else { format!("({})", inner) }
        }
        Expr::Or(xs) => {
            let inner = xs.iter().map(|x| compile_expr_for(db_type, x, params, idx)).collect::<Vec<_>>().join(" OR ");
            if inner.is_empty() { inner } else { format!("({})", inner) }
        }
        // Escape hatch
        Expr::Raw(s) => s.clone(),
    }
}

fn render_agg_sql(a: &Agg) -> String {
    match &a.func {
        AggFunc::CountAll => format!("COUNT(*) AS {}", a.alias),
        AggFunc::Count(f) => format!("COUNT({}) AS {}", f, a.alias),
        AggFunc::Sum(f) => format!("SUM({}) AS {}", f, a.alias),
        AggFunc::Avg(f) => format!("AVG({}) AS {}", f, a.alias),
        AggFunc::Min(f) => format!("MIN({}) AS {}", f, a.alias),
        AggFunc::Max(f) => format!("MAX({}) AS {}", f, a.alias),
    }
}

fn compile_query_with_dialect(db_type: &str, q: &Query) -> (String, Vec<DbParam>) {
    let mut sql = String::new();
    let mut params = Vec::<DbParam>::new();
    let mut idx = 0usize;

    // Build SELECT list: honor explicit projection if provided; otherwise, derive from group_by/aggs or fallback to *
    let select_list = if !q.projection.is_empty() {
        q.projection.join(",")
    } else if !q.aggs.is_empty() || !q.group_by.is_empty() {
        let mut cols: Vec<String> = Vec::new();
        if !q.group_by.is_empty() {
            cols.extend(q.group_by.clone());
        }
        if !q.aggs.is_empty() {
            cols.extend(q.aggs.iter().map(render_agg_sql));
        }
        if cols.is_empty() { "*".to_string() } else { cols.join(",") }
    } else {
        "*".to_string()
    };
    sql.push_str(&format!("SELECT {} FROM {}", select_list, q.collection));

    if !q.joins.is_empty() {
        for j in &q.joins {
            let on_clause = if let Some(ref ex) = j.on_expr {
                compile_expr_for(db_type, ex, &mut params, &mut idx)
            } else {
                j.on.clone()
            };
            match j.kind {
                JoinKind::Inner => sql.push_str(&format!(" INNER JOIN {} ON {}", j.table, on_clause)),
                JoinKind::Left => sql.push_str(&format!(" LEFT JOIN {} ON {}", j.table, on_clause)),
            }
        }
    }

    if let Some(f) = &q.filter {
        let clause = compile_filter_for(db_type, f, &mut params, &mut idx);
        if !clause.is_empty() { sql.push_str(" WHERE "); sql.push_str(&clause); }
    }

    if !q.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(&q.group_by.join(", "));
    }
    // HAVING: compiled exprs only
    if !q.having_exprs.is_empty() {
        let compiled = q
            .having_exprs
            .iter()
            .map(|e| compile_expr_for(db_type, e, &mut params, &mut idx))
            .collect::<Vec<_>>()
            .join(" AND ");
        if !compiled.is_empty() {
            sql.push_str(" HAVING ");
            sql.push_str(&compiled);
        }
    }
    if !q.sort.is_empty() {
        let ord = q.sort.iter().map(|s| format!("{} {}", s.field, if s.asc { "ASC" } else { "DESC" })).collect::<Vec<_>>().join(",");
        sql.push_str(" ORDER BY ");
        sql.push_str(&ord);
    }
    match db_type {
        "mssql" => {
            if q.limit.is_some() || q.offset.is_some() {
                if q.sort.is_empty() { sql.push_str(" ORDER BY 1"); }
                let off = q.offset.unwrap_or(0);
                let lim = q.limit.unwrap_or(100);
                sql.push_str(&format!(" OFFSET {} ROWS FETCH NEXT {} ROWS ONLY", off, lim));
            }
        }
        _ => {
            if let Some(l) = q.limit { sql.push_str(&format!(" LIMIT {}", l)); }
            if let Some(o) = q.offset { sql.push_str(&format!(" OFFSET {}", o)); }
        }
    }
    (sql, params)
}

fn build_insert_with_dialect(db_type: &str, collection: &str, doc: &Value) -> anyhow::Result<(String, Vec<DbParam>)> {
    let obj = doc.as_object().ok_or_else(|| anyhow::anyhow!("insert expects object"))?;
    let cols: Vec<_> = obj.keys().cloned().collect();
    let mut params = Vec::<DbParam>::new();
    let mut idx = 0usize;
    let placeholders: Vec<_> = obj.values().map(|v| { idx += 1; params.push(json_to_param(v)); next_placeholder_for(db_type, idx) }).collect();
    let sql = format!("INSERT INTO {} ({}) VALUES ({})", collection, cols.join(","), placeholders.join(","));
    Ok((sql, params))
}

fn build_update_with_dialect(db_type: &str, collection: &str, filter: Option<&Filter>, patch: &Value) -> anyhow::Result<(String, Vec<DbParam>)> {
    let obj = patch.as_object().ok_or_else(|| anyhow::anyhow!("update expects object"))?;
    let mut params = Vec::<DbParam>::new();
    let mut idx = 0usize;
    let sets: Vec<_> = obj.iter().map(|(k, v)| { idx += 1; params.push(json_to_param(v)); format!("{} = {}", k, next_placeholder_for(db_type, idx)) }).collect();
    let mut sql = format!("UPDATE {} SET {}", collection, sets.join(","));
    if let Some(f) = filter {
        let clause = compile_filter_for(db_type, f, &mut params, &mut idx);
        sql.push_str(" WHERE "); sql.push_str(&clause);
    }
    Ok((sql, params))
}

#[allow(dead_code)]
fn build_delete_with_dialect(db_type: &str, collection: &str, filter: Option<&Filter>) -> anyhow::Result<(String, Vec<DbParam>)> {
    let mut params = Vec::<DbParam>::new();
    let mut idx = 0usize;
    let mut sql = format!("DELETE FROM {}", collection);
    if let Some(f) = filter {
        let clause = compile_filter_for(db_type, f, &mut params, &mut idx);
        sql.push_str(" WHERE "); sql.push_str(&clause);
    }
    Ok((sql, params))
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

    #[tokio::test]
    async fn compiles_join_on_expr_and_having_exprs() {
        let repo: Arc<dyn DbRepository> = Arc::new(MockRepo);
        let store = SqlStore::new(repo, "postgres".to_string());
        use crate::storage::ast::{Expr as E, Val as V};
        // SELECT o.id FROM orders o INNER JOIN customers c ON o.customer_id = c.id GROUP BY o.customer_id HAVING SUM(o.total) > $1
        let q = crate::storage::ast::Query::from("orders o")
            .select(["o.id"]) 
            .join_inner_expr("customers c", E::ColEq("o.customer_id".into(), "c.id".into()))
            .group_by(["o.customer_id"]) 
            .having_expr([E::Gt("SUM(o.total)".into(), V::F64(1000.0))]);
        let rows = store.query(&q).await.unwrap();
        let first = rows.first().unwrap();
        let sql = first.get("sql").and_then(|v| v.as_str()).unwrap();
        assert!(sql.contains("INNER JOIN customers c ON o.customer_id = c.id"));
        assert!(sql.contains("GROUP BY o.customer_id"));
        assert!(sql.contains("HAVING SUM(o.total) > $1"));
    }

    #[tokio::test]
    async fn compiles_aggs_with_group_by_into_select() {
        let repo: Arc<dyn DbRepository> = Arc::new(MockRepo);
        let store = SqlStore::new(repo, "postgres".to_string());
        use crate::storage::ast::{Query as Q};
        let q = Q::from("orders o")
            .group_by(["o.customer_id"]) 
            .agg_count_all("order_count")
            .agg_sum("amount_sum", "o.total");
        let rows = store.query(&q).await.unwrap();
        let first = rows.first().unwrap();
        let sql = first.get("sql").and_then(|v| v.as_str()).unwrap();
        assert!(sql.contains("SELECT o.customer_id,COUNT(*) AS order_count,SUM(o.total) AS amount_sum FROM orders o"));
        assert!(sql.contains("GROUP BY o.customer_id"));
    }

    #[tokio::test]
    async fn compiles_global_aggs_into_select_without_group_by() {
        let repo: Arc<dyn DbRepository> = Arc::new(MockRepo);
        let store = SqlStore::new(repo, "mysql".to_string());
        use crate::storage::ast::Query as Q;
        let q = Q::from("orders")
            .agg_avg("avg_total", "total")
            .agg_max("max_total", "total");
        let rows = store.query(&q).await.unwrap();
        let first = rows.first().unwrap();
        let sql = first.get("sql").and_then(|v| v.as_str()).unwrap();
        assert!(sql.contains("SELECT AVG(total) AS avg_total,MAX(total) AS max_total FROM orders"));
        // No GROUP BY expected
        assert!(!sql.contains("GROUP BY"));
    }
}
