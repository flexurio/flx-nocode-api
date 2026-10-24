//! Atomic running numbers (`flx_sequences`).
//!
//! Custom document ids (`function: "INV/%Y/000ID"`) and trigger `{seq:…}`
//! tokens used to be derived from `SELECT MAX(id) … LIKE 'prefix%'` inside the
//! transaction: a full scan on every insert and, because a plain SELECT takes
//! no lock, two concurrent inserts could compute the same number.
//!
//! Now every `(scope)` has one row here and the increment is a single
//! `UPDATE … SET seq_value = seq_value + 1 WHERE scope = ?`. The row lock the
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
/// Counter column. Not `last_value`: that is a reserved word in MySQL 8
/// (window function), so `CREATE TABLE` failed there and every custom-id
/// insert errored. Older installs are renamed by [`migrate_legacy_column`].
pub const VALUE_COL: &str = "seq_value";
const LEGACY_VALUE_COL: &str = "last_value";

pub fn table_schema() -> TableSchema {
    let mut t = TableSchema { table: TABLE.into(), ..Default::default() };
    t.primary_key = PrimaryKey { columns: vec!["scope".into()] };
    t.columns = vec![
        Column { name: "scope".into(), type_data: "varchar(255)".into(), nullable: false, ..Default::default() },
        Column { name: VALUE_COL.into(), type_data: "bigint".into(), nullable: false, ..Default::default() },
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
            &format!("UPDATE {t} SET {c} = {c} + 1 WHERE scope = ?", t = TABLE, c = VALUE_COL),
            vec![DbParam::Str(scope.to_string())],
        )
        .await?;
    if affected == 0 {
        return Ok(None);
    }
    let rows = tx
        .query(&QQ::from(TABLE).select([VALUE_COL]).r#where(QF::Eq("scope".into(), QV::Str(scope.to_string()))).limit(1))
        .await?;
    let v = rows
        .first()
        .and_then(|r| r.get(VALUE_COL))
        .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .ok_or_else(|| anyhow!("sequence {} vanished after increment", scope))?;
    Ok(Some(v))
}

/// Rename the pre-0.5.1 `last_value` column to [`VALUE_COL`] on databases
/// where the table could be created (PostgreSQL/SQLite/MSSQL). Best effort:
/// fails harmlessly when the table does not exist or is already migrated.
pub async fn migrate_legacy_column(store: &dyn crate::storage::traits::DataStore, dialect: &str) {
    let sql = match dialect {
        "mssql" => format!("EXEC sp_rename '{t}.{o}', '{n}', 'COLUMN'", t = TABLE, o = LEGACY_VALUE_COL, n = VALUE_COL),
        "mysql" => return, // the table never got created there
        _ => format!("ALTER TABLE {t} RENAME COLUMN {o} TO {n}", t = TABLE, o = LEGACY_VALUE_COL, n = VALUE_COL),
    };
    let Ok(mut tx) = store.begin_tx().await else { return };
    match tx.raw_execute(&sql, vec![]).await {
        Ok(_) => {
            let _ = tx.commit().await;
        }
        Err(_) => {
            let _ = tx.rollback().await;
        }
    }
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
        VALUE_COL: seed_last + 1,
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
