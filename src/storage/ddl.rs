use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DefaultExpr {
    CurrentTimestamp,
    Now,
    Raw(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColumnType {
    /// Use a raw type string (e.g., "BIGINT", "BIGSERIAL", "NVARCHAR(MAX)") chosen by caller per dialect
    Raw(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
    pub default: Option<DefaultExpr>,
    /// Dialect-specific autoincrement. For cross-db, prefer setting concrete types in `Raw` and keep this false.
    pub auto_increment: bool,
    /// Mark this column as part of primary key when needed (for SQLite INTEGER PRIMARY KEY AUTOINCREMENT special case, the builder should encode via col_type).
    pub primary_key_inline: bool,
    /// Optional column collation (e.g. UTF8_GENERAL_CI, "en_US.utf8").
    pub collate: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey { columns: Vec<String> },
    Unique { name: Option<String>, columns: Vec<String> },
    Index { name: Option<String>, columns: Vec<String>, unique: bool },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<ForeignAction>,
        on_update: Option<ForeignAction>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ForeignAction {
    Cascade,
    SetNull,
    Restrict,
    NoAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Ddl {
    CreateTable(CreateTable),
}
