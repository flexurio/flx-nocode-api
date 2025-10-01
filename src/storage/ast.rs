use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Val {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    Null,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinKind { Inner, Left }

/// Boolean expression tree usable for JOIN ON and HAVING clauses
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Expr {
    // Column vs literal value
    Eq(String, Val), Ne(String, Val), Gt(String, Val), Gte(String, Val), Lt(String, Val), Lte(String, Val),
    Like(String, String), ILike(String, String), NotLike(String, String),
    In(String, Vec<Val>), NotIn(String, Vec<Val>), Between(String, Val, Val),
    // Column vs column
    ColEq(String, String), ColNe(String, String), ColGt(String, String), ColGte(String, String), ColLt(String, String), ColLte(String, String),
    // Composition
    And(Vec<Expr>), Or(Vec<Expr>),
    // Escape hatch
    Raw(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Join {
    pub kind: JoinKind,
    pub table: String,
    pub on: String,           // legacy raw path
    pub on_expr: Option<Expr> // new builder path
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Filter {
    Eq(String, Val), Ne(String, Val), Gt(String, Val), Gte(String, Val), Lt(String, Val), Lte(String, Val),
    Like(String, String), ILike(String, String), NotLike(String, String),
    In(String, Vec<Val>), NotIn(String, Vec<Val>),
    IsNull(String), IsNotNull(String),
    Between(String, Val, Val),
    And(Vec<Filter>), Or(Vec<Filter>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sort { pub field: String, pub asc: bool }

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
    pub having_exprs: Vec<Expr>,     // new expr path
    pub aggs: Vec<Agg>,              // aggregate functions with aliases
}

impl Query {
    pub fn from<S: Into<String>>(collection: S) -> Self { Self { collection: collection.into(), ..Default::default() } }
    pub fn select<I, S2>(mut self, fields: I) -> Self where I: IntoIterator<Item = S2>, S2: Into<String> { self.projection = fields.into_iter().map(Into::into).collect(); self }
    pub fn r#where(mut self, f: Filter) -> Self { self.filter = Some(f); self }
    pub fn order_by<S: Into<String>>(mut self, field: S, asc: bool) -> Self { self.sort.push(Sort { field: field.into(), asc }); self }
    pub fn limit(mut self, n: u32) -> Self { self.limit = Some(n); self }
    pub fn offset(mut self, n: u32) -> Self { self.offset = Some(n); self }
    pub fn join_inner_expr<S1: Into<String>>(mut self, table: S1, on: Expr) -> Self {
        self.joins.push(Join { kind: JoinKind::Inner, table: table.into(), on: String::new(), on_expr: Some(on) }); self
    }
    pub fn join_left_expr<S1: Into<String>>(mut self, table: S1, on: Expr) -> Self {
        self.joins.push(Join { kind: JoinKind::Left, table: table.into(), on: String::new(), on_expr: Some(on) }); self
    }
    pub fn group_by<I, S2>(mut self, cols: I) -> Self where I: IntoIterator<Item = S2>, S2: Into<String> { self.group_by = cols.into_iter().map(Into::into).collect(); self }
    pub fn having_expr<I>(mut self, exprs: I) -> Self where I: IntoIterator<Item = Expr> { self.having_exprs = exprs.into_iter().collect(); self }
    // Aggregation builders
    pub fn agg_count_all<S: Into<String>>(mut self, alias: S) -> Self {
        self.aggs.push(Agg { alias: alias.into(), func: AggFunc::CountAll }); self
    }
    pub fn agg_count<S1: Into<String>, S2: Into<String>>(mut self, alias: S1, field: S2) -> Self {
        self.aggs.push(Agg { alias: alias.into(), func: AggFunc::Count(field.into()) }); self
    }
    pub fn agg_sum<S1: Into<String>, S2: Into<String>>(mut self, alias: S1, field: S2) -> Self {
        self.aggs.push(Agg { alias: alias.into(), func: AggFunc::Sum(field.into()) }); self
    }
    pub fn agg_avg<S1: Into<String>, S2: Into<String>>(mut self, alias: S1, field: S2) -> Self {
        self.aggs.push(Agg { alias: alias.into(), func: AggFunc::Avg(field.into()) }); self
    }
    pub fn agg_min<S1: Into<String>, S2: Into<String>>(mut self, alias: S1, field: S2) -> Self {
        self.aggs.push(Agg { alias: alias.into(), func: AggFunc::Min(field.into()) }); self
    }
    pub fn agg_max<S1: Into<String>, S2: Into<String>>(mut self, alias: S1, field: S2) -> Self {
        self.aggs.push(Agg { alias: alias.into(), func: AggFunc::Max(field.into()) }); self
    }
}

// ---------------------------
// Storage-agnostic Logical Plan
// ---------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggFunc {
    CountAll,
    Count(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Agg {
    pub alias: String,
    pub func: AggFunc,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogicalPlan {
    // Read pipeline
    Scan { collection: String },
    Join {
        input: Box<LogicalPlan>,
        kind: JoinKind,
        table: String,
        on_expr: Option<Expr>,
        on_raw: Option<String>,
    },
    Filter { input: Box<LogicalPlan>, predicate: Filter },
    Project { input: Box<LogicalPlan>, fields: Vec<String> },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggs: Vec<Agg>,
        having: Vec<Expr>,
    },
    Sort { input: Box<LogicalPlan>, by: Vec<Sort> },
    Limit { input: Box<LogicalPlan>, offset: Option<u32>, limit: Option<u32> },

    // Write ops (optional for NoSQL/SQL adapters)
    Insert { collection: String, documents: Vec<JsonValue> },
    Update { collection: String, filter: Option<Filter>, patch: JsonValue },
    Delete { collection: String, filter: Option<Filter> },
}

impl Query {
    /// Lower the high-level Query into a storage-agnostic LogicalPlan.
    /// This preserves the order of operations commonly used by executors.
    pub fn to_logical_plan(&self) -> LogicalPlan {
        // Start with a Scan over the base collection
        let mut plan = LogicalPlan::Scan { collection: self.collection.clone() };

        // Apply JOINs in order (left-associated)
        for j in &self.joins {
            plan = LogicalPlan::Join {
                input: Box::new(plan),
                kind: j.kind.clone(),
                table: j.table.clone(),
                on_expr: j.on_expr.clone(),
                on_raw: if j.on_expr.is_none() && !j.on.is_empty() { Some(j.on.clone()) } else { None },
            };
        }

        // WHERE / Filter
        if let Some(pred) = &self.filter {
            plan = LogicalPlan::Filter { input: Box::new(plan), predicate: pred.clone() };
        }

        // GROUP BY / HAVING / AGGs
        if !self.group_by.is_empty() || !self.having_exprs.is_empty() || !self.aggs.is_empty() {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: self.group_by.clone(),
                aggs: self.aggs.clone(),
                having: self.having_exprs.clone(),
            };
        }

        // SELECT projection
        if !self.projection.is_empty() {
            plan = LogicalPlan::Project { input: Box::new(plan), fields: self.projection.clone() };
        }

        // ORDER BY
        if !self.sort.is_empty() {
            plan = LogicalPlan::Sort { input: Box::new(plan), by: self.sort.clone() };
        }

        // OFFSET/LIMIT
        if self.offset.is_some() || self.limit.is_some() {
            plan = LogicalPlan::Limit { input: Box::new(plan), offset: self.offset, limit: self.limit };
        }

        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowers_basic_query_pipeline() {
        let q = Query::from("users")
            .select(["id", "name"]) 
            .r#where(Filter::Eq("active".into(), Val::Bool(true)))
            .order_by("name", true)
            .limit(10)
            .offset(20);

        let plan = q.to_logical_plan();

        // Expect: Limit -> Sort -> Project -> Filter -> Scan
        match plan {
            LogicalPlan::Limit { input: l1, offset, limit } => {
                assert_eq!(offset, Some(20));
                assert_eq!(limit, Some(10));
                match *l1 {
                    LogicalPlan::Sort { input: l2, by } => {
                        assert_eq!(by.len(), 1);
                        assert_eq!(by[0].field, "name");
                        assert!(by[0].asc);
                        match *l2 {
                            LogicalPlan::Project { input: l3, fields } => {
                                assert_eq!(fields, vec!["id".to_string(), "name".to_string()]);
                                match *l3 {
                                    LogicalPlan::Filter { input: l4, predicate } => {
                                        match predicate { Filter::Eq(f, Val::Bool(v)) => { assert_eq!(f, "active"); assert!(v); }, _ => panic!("unexpected predicate") }
                                        match *l4 { LogicalPlan::Scan { collection } => assert_eq!(collection, "users"), _ => panic!("expected scan") }
                                    }
                                    _ => panic!("expected filter"),
                                }
                            }
                            _ => panic!("expected project"),
                        }
                    }
                    _ => panic!("expected sort"),
                }
            }
            _ => panic!("expected limit"),
        }
    }
}
