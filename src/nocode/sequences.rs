//! Atomic running numbers (`flx_sequences`).
//!
//! Custom document ids (`function: "INV/%Y/000ID"`) and trigger `{seq:…}`
//! tokens used to be derived from `SELECT MAX(id) … LIKE 'prefix%'` inside the
//! transaction: a full scan on every insert and, because a plain SELECT takes
//! no lock, two concurrent inserts could compute the same number.
//!
//! Now every `(scope)` has one row here and the increment is a single
//! `UPDATE … SET last_value = last_value + 1 WHERE scope = ?`. The row lock the
//! database takes for that UPDATE serialises concurrent transactions on the
//! same scope (PostgreSQL, MySQL/InnoDB, MSSQL; SQLite has one writer), so the
//! numbers are gap-free per committed transaction and never duplicated.
//! The first use of a scope seeds the row from the legacy maximum so existing
//! numbering continues seamlessly.

use anyhow::{anyhow, Result};
use serde_json::json;

use crate::database::state::DbParam;
use crate::model::{Column, PrimaryKey, TableSchema};
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV};
use crate::storage::traits::TxStore;

pub const TABLE: &str = "flx_sequences";

pub fn table_schema() -> TableSchema {
    let mut t = TableSchema { table: TABLE.into(), ..Default::default() };
    t.primary_key = PrimaryKey { columns: vec!["scope".into()] };
    t.columns = vec![
        Column { name: "scope".into(), type_data: "varchar(255)".into(), nullable: false, ..Default::default() },
        Column { name: "last_value".into(), type_data: "bigint".into(), nullable: false, ..Default::default() },
        Column { name: "updated_at".into(), type_data: "varchar(40)".into(), nullable: true, ..Default::default() },
    ];
    t.auto_generate = true;
    t
}

/// Scope key for a custom-id prefix of a table.
pub fn id_scope(table: &str, prefix: &str) -> String {
    format!("{}:id:{}", table, prefix)
}

/// Scope key for a trigger `{seq:}` pattern on `table.col`.
pub fn seq_scope(table: &str, col: &str, prefix: &str) -> String {
    format!("{}:{}:{}", table, col, prefix)
}

/// Increment an existing scope and return the new value; `None` if the scope
/// has never been used (caller seeds it with [`seed_and_take`]).
pub async fn try_increment(tx: &mut (dyn TxStore + '_), scope: &str) -> Result<Option<i64>> {
    let affected = tx
        .raw_execute(
            &format!("UPDATE {} SET last_value = last_value + 1 WHERE scope = ?", TABLE),
            vec![DbParam::Str(scope.to_string())],
        )
        .await?;
    if affected == 0 {
        return Ok(None);
    }
    let rows = tx
        .query(&QQ::from(TABLE).select(["last_value"]).r#where(QF::Eq("scope".into(), QV::Str(scope.to_string()))).limit(1))
        .await?;
    let v = rows
        .first()
        .and_then(|r| r.get("last_value"))
        .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .ok_or_else(|| anyhow!("sequence {} vanished after increment", scope))?;
    Ok(Some(v))
}

fn savepoint_sql(dialect: &str) -> (&'static str, &'static str) {
    match dialect {
        "mssql" => ("SAVE TRANSACTION flx_seq", "ROLLBACK TRANSACTION flx_seq"),
        _ => ("SAVEPOINT flx_seq", "ROLLBACK TO SAVEPOINT flx_seq"),
    }
}

/// First use of a scope: create the row at `seed_last + 1` and return that
/// value. If another transaction created the row first, the INSERT fails on
/// the primary key; the failure is isolated in a savepoint (an error would
/// otherwise abort the whole PostgreSQL transaction) and we fall back to a
/// normal increment.
pub async fn seed_and_take(tx: &mut (dyn TxStore + '_), dialect: &str, scope: &str, seed_last: i64) -> Result<i64> {
    let (save, rollback) = savepoint_sql(dialect);
    let _ = tx.raw_execute(save, vec![]).await;
    let doc = json!({
        "scope": scope,
        "last_value": seed_last + 1,
        "updated_at": chrono::Utc::now().to_rfc3339(),
    });
    match tx.insert(TABLE, doc).await {
        Ok(_) => Ok(seed_last + 1),
        Err(_) => {
            let _ = tx.raw_execute(rollback, vec![]).await;
            try_increment(tx, scope).await?.ok_or_else(|| anyhow!("sequence {} could not be initialised", scope))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_and_schema() {
        assert_eq!(id_scope("sales_order", "SO/2026"), "sales_order:id:SO/2026");
        assert_eq!(seq_scope("gl", "voucher_no", "GL/"), "gl:voucher_no:GL/");
        let t = table_schema();
        assert_eq!(t.primary_key.columns, vec!["scope".to_string()]);
        assert!(!t.columns[0].auto_increment);
        assert_eq!(savepoint_sql("mssql").0, "SAVE TRANSACTION flx_seq");
        assert_eq!(savepoint_sql("postgres").1, "ROLLBACK TO SAVEPOINT flx_seq");
    }
}
