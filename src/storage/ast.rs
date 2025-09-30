use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Val {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    Null,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Join {
    pub kind: JoinKind,
    pub table: String,
    pub on: String, // safe, comes from static config with optional sanitized substitutions
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Filter {
    Eq(String, Val),
    Ne(String, Val),
    Gt(String, Val),
    Gte(String, Val),
    Lt(String, Val),
    Lte(String, Val),
    Like(String, String),
    ILike(String, String),
    NotLike(String, String),
    In(String, Vec<Val>),
    NotIn(String, Vec<Val>),
    IsNull(String),
    IsNotNull(String),
    Between(String, Val, Val),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sort {
    pub field: String,
    pub asc: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Query {
    pub collection: String,          // table/collection
    pub projection: Vec<String>,     // select fields
    pub filter: Option<Filter>,
    pub sort: Vec<Sort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub joins: Vec<Join>,
    pub group_by: Vec<String>,
    pub having_raw: Vec<String>, // raw-safe (from config), joined with ", " similar to legacy
    pub distinct: bool,
}

impl Query {
    pub fn from<S: Into<String>>(collection: S) -> Self {
        Self { collection: collection.into(), ..Default::default() }
    }
    pub fn select<I, S2>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S2>,
        S2: Into<String>,
    {
        self.projection = fields.into_iter().map(Into::into).collect();
        self
    }
    pub fn r#where(mut self, f: Filter) -> Self {
        self.filter = Some(f);
        self
    }
    pub fn order_by<S: Into<String>>(mut self, field: S, asc: bool) -> Self {
        self.sort.push(Sort { field: field.into(), asc });
        self
    }
    pub fn limit(mut self, n: u32) -> Self {
        self.limit = Some(n);
        self
    }
    pub fn offset(mut self, n: u32) -> Self {
        self.offset = Some(n);
        self
    }

    pub fn join_inner<S1: Into<String>, S2: Into<String>>(mut self, table: S1, on: S2) -> Self {
        self.joins.push(Join { kind: JoinKind::Inner, table: table.into(), on: on.into() });
        self
    }
    pub fn join_left<S1: Into<String>, S2: Into<String>>(mut self, table: S1, on: S2) -> Self {
        self.joins.push(Join { kind: JoinKind::Left, table: table.into(), on: on.into() });
        self
    }
    pub fn group_by<I, S2>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S2>,
        S2: Into<String>,
    {
        self.group_by = cols.into_iter().map(Into::into).collect();
        self
    }
    pub fn having_raw<I, S2>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = S2>,
        S2: Into<String>,
    {
        self.having_raw = exprs.into_iter().map(Into::into).collect();
        self
    }

    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }
}
