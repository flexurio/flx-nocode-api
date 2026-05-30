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
    pub where_raw: Vec<String>,      // raw, trusted WHERE conditions (from config); AND-combined with `filter`
    pub sort: Vec<Sort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub joins: Vec<Join>,
    pub group_by: Vec<String>,
    pub having_exprs: Vec<Expr>,     // new expr path
    pub aggs: Vec<Agg>,              // aggregate functions with aliases
}

#[allow(dead_code)]
impl Query {
    pub fn from<S: Into<String>>(collection: S) -> Self { Self { collection: collection.into(), ..Default::default() } }
    pub fn select<I, S2>(mut self, fields: I) -> Self where I: IntoIterator<Item = S2>, S2: Into<String> { self.projection = fields.into_iter().map(Into::into).collect(); self }
    pub fn r#where(mut self, f: Filter) -> Self { self.filter = Some(f); self }
    /// Append a raw, trusted WHERE condition (AND-combined). Source must be config,
    /// never user input — it is emitted verbatim into the SQL.
    pub fn where_raw<S: Into<String>>(mut self, cond: S) -> Self { self.where_raw.push(cond.into()); self }
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
    #[allow(dead_code)]
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

    // --- Query builder ---

    #[test]
    fn test_query_from_sets_collection() {
        let q = Query::from("orders");
        assert_eq!(q.collection, "orders");
        assert!(q.projection.is_empty());
        assert!(q.filter.is_none());
        assert!(q.sort.is_empty());
        assert!(q.limit.is_none());
        assert!(q.offset.is_none());
    }

    #[test]
    fn test_query_select_sets_projection() {
        let q = Query::from("t").select(["id", "name", "email"]);
        assert_eq!(q.projection, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_query_limit_and_offset() {
        let q = Query::from("t").limit(50).offset(100);
        assert_eq!(q.limit, Some(50));
        assert_eq!(q.offset, Some(100));
    }

    #[test]
    fn test_query_order_by_asc_and_desc() {
        let q = Query::from("t")
            .order_by("created_at", false)
            .order_by("id", true);
        assert_eq!(q.sort.len(), 2);
        assert_eq!(q.sort[0].field, "created_at");
        assert!(!q.sort[0].asc);
        assert_eq!(q.sort[1].field, "id");
        assert!(q.sort[1].asc);
    }

    #[test]
    fn test_query_where_filter() {
        let q = Query::from("t").r#where(Filter::Gt("age".into(), Val::I64(18)));
        assert!(q.filter.is_some());
        match q.filter.unwrap() {
            Filter::Gt(col, Val::I64(v)) => {
                assert_eq!(col, "age");
                assert_eq!(v, 18);
            }
            _ => panic!("Expected Filter::Gt"),
        }
    }

    #[test]
    fn test_query_group_by() {
        let q = Query::from("t").group_by(["department", "status"]);
        assert_eq!(q.group_by, vec!["department", "status"]);
    }

    #[test]
    fn test_query_join_inner_expr() {
        let q = Query::from("users u")
            .join_inner_expr("roles r", Expr::ColEq("u.role_id".into(), "r.id".into()));
        assert_eq!(q.joins.len(), 1);
        assert!(matches!(q.joins[0].kind, JoinKind::Inner));
        assert_eq!(q.joins[0].table, "roles r");
    }

    #[test]
    fn test_query_join_left_expr() {
        let q = Query::from("orders o")
            .join_left_expr("customers c", Expr::ColEq("o.customer_id".into(), "c.id".into()));
        assert_eq!(q.joins.len(), 1);
        assert!(matches!(q.joins[0].kind, JoinKind::Left));
    }

    #[test]
    fn test_query_agg_count_all() {
        let q = Query::from("t").agg_count_all("total");
        assert_eq!(q.aggs.len(), 1);
        assert_eq!(q.aggs[0].alias, "total");
        assert!(matches!(q.aggs[0].func, AggFunc::CountAll));
    }

    #[test]
    fn test_query_agg_sum() {
        let q = Query::from("t").agg_sum("revenue", "amount");
        assert!(matches!(&q.aggs[0].func, AggFunc::Sum(f) if f == "amount"));
    }

    #[test]
    fn test_query_agg_avg() {
        let q = Query::from("t").agg_avg("avg_score", "score");
        assert!(matches!(&q.aggs[0].func, AggFunc::Avg(f) if f == "score"));
    }

    #[test]
    fn test_query_agg_min_max() {
        let q = Query::from("t")
            .agg_min("min_val", "value")
            .agg_max("max_val", "value");
        assert_eq!(q.aggs.len(), 2);
        assert!(matches!(&q.aggs[0].func, AggFunc::Min(_)));
        assert!(matches!(&q.aggs[1].func, AggFunc::Max(_)));
    }

    #[test]
    fn test_query_having_expr() {
        let q = Query::from("t")
            .group_by(["category"])
            .having_expr([Expr::Gte("COUNT(*)".into(), Val::I64(5))]);
        assert_eq!(q.having_exprs.len(), 1);
    }

    // --- Filter ---

    #[test]
    fn test_filter_variants_are_cloneable() {
        let filters = vec![
            Filter::Eq("a".into(), Val::I64(1)),
            Filter::Ne("b".into(), Val::Str("x".into())),
            Filter::Gt("c".into(), Val::F64(1.5)),
            Filter::Gte("d".into(), Val::I64(0)),
            Filter::Lt("e".into(), Val::I64(100)),
            Filter::Lte("f".into(), Val::Bool(false)),
            Filter::Like("g".into(), "%foo%".into()),
            Filter::IsNull("h".into()),
            Filter::IsNotNull("i".into()),
            Filter::Between("j".into(), Val::I64(1), Val::I64(10)),
            Filter::In("k".into(), vec![Val::I64(1), Val::I64(2)]),
            Filter::NotIn("l".into(), vec![Val::Str("x".into())]),
        ];
        for f in filters {
            let _ = f.clone(); // Should not panic
        }
    }

    #[test]
    fn test_filter_and_or_composition() {
        let and_filter = Filter::And(vec![
            Filter::Eq("a".into(), Val::I64(1)),
            Filter::Eq("b".into(), Val::I64(2)),
        ]);
        let or_filter = Filter::Or(vec![
            Filter::IsNull("x".into()),
            Filter::IsNotNull("y".into()),
        ]);
        let _combined = Filter::And(vec![and_filter, or_filter]);
    }

    // --- Val ---

    #[test]
    fn test_val_variants_debug() {
        let vals = vec![
            Val::I64(42),
            Val::F64(3.14),
            Val::Bool(true),
            Val::Str("hello".into()),
            Val::Null,
        ];
        for v in vals {
            let _ = format!("{:?}", v);
        }
    }

    // --- Scan-only logical plan ---

    #[test]
    fn test_scan_only_logical_plan() {
        let q = Query::from("products");
        let plan = q.to_logical_plan();
        match plan {
            LogicalPlan::Scan { collection } => assert_eq!(collection, "products"),
            _ => panic!("Expected a bare Scan plan, got {:?}", plan),
        }
    }

    #[test]
    fn test_logical_plan_with_aggregate() {
        let q = Query::from("sales")
            .group_by(["region"])
            .agg_sum("total", "amount");
        let plan = q.to_logical_plan();
        match plan {
            LogicalPlan::Aggregate { group_by, aggs, .. } => {
                assert_eq!(group_by, vec!["region"]);
                assert_eq!(aggs.len(), 1);
            }
            _ => panic!("Expected Aggregate plan"),
        }
    }
}
