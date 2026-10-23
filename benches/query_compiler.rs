//! Micro-benchmarks for the core asset: the AST query compiler and the GET
//! planner. Guards against regressions in the hottest CPU path of every read.
//!
//! Run: `cargo bench --bench query_compiler`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;
use std::sync::Arc;

use flx_nocode_api::database::state::{DbParam, DbRepository, DbTransaction};
use flx_nocode_api::model::{Column, Index, PrimaryKey, TableSchema};
use flx_nocode_api::nocode::repositories::data_read_repo::build_read_query;
use flx_nocode_api::storage::ast::{Expr, Filter, Query, Val};
use flx_nocode_api::storage::sql_store::SqlStore;

struct NoopRepo;

#[async_trait::async_trait]
impl DbRepository for NoopRepo {
    async fn query(&self, _sql: &str) -> anyhow::Result<Vec<serde_json::Value>> {
        Ok(vec![])
    }
    async fn query_with_params(&self, _sql: &str, _p: Vec<DbParam>) -> anyhow::Result<Vec<serde_json::Value>> {
        Ok(vec![])
    }
    async fn begin_transaction(&self) -> anyhow::Result<Box<dyn DbTransaction>> {
        anyhow::bail!("noop")
    }
}

#[allow(clippy::field_reassign_with_default)]
fn schema() -> TableSchema {
    let mut t = TableSchema::default();
    t.table = "orders".into();
    t.primary_key = PrimaryKey { columns: vec!["id".into()] };
    t.columns = vec![
        Column { name: "id".into(), type_data: "integer".into(), ..Default::default() },
        Column { name: "code".into(), type_data: "varchar(50)".into(), ..Default::default() },
        Column { name: "qty".into(), type_data: "int".into(), ..Default::default() },
        Column { name: "status".into(), type_data: "varchar(20)".into(), ..Default::default() },
        Column { name: "created_at".into(), type_data: "datetime".into(), ..Default::default() },
    ];
    t.get.columns = vec!["orders.id".into(), "orders.code".into(), "orders.qty".into(), "orders.status".into(), "orders.created_at".into()];
    t.get.parameters = vec!["code.eq".into(), "qty.gte".into(), "status.in".into(), "created_at.between".into(), "meta->*.eq".into()];
    t.indexes = vec![Index { name: "ix".into(), columns: vec!["code".into(), "status".into(), "created_at".into()], unique: false }];
    t
}

fn complex_query() -> Query {
    Query::from("orders o")
        .select(["o.id", "o.code", "o.qty", "c.name AS customer"])
        .join_inner_expr("customers c", Expr::ColEq("o.customer_id".into(), "c.id".into()))
        .r#where(Filter::And(vec![
            Filter::Eq("o.status".into(), Val::Str("open".into())),
            Filter::In("o.code".into(), (0..8).map(|i| Val::Str(format!("C{}", i))).collect()),
            Filter::Between("o.created_at".into(), Val::Str("2026-01-01".into()), Val::Str("2026-12-31".into())),
            Filter::Or(vec![Filter::Gte("o.qty".into(), Val::I64(10)), Filter::IsNull("o.deleted_at".into())]),
        ]))
        .order_by("o.created_at", false)
        .order_by("o.id", true)
        .limit(200)
        .offset(400)
}

fn bench_compile(c: &mut Criterion) {
    let repo: Arc<dyn DbRepository> = Arc::new(NoopRepo);
    let q = complex_query();
    for dialect in ["mysql", "postgres", "mssql"] {
        let store = SqlStore::new(repo.clone(), dialect.to_string());
        c.bench_function(&format!("compile_query/{}", dialect), |b| {
            b.iter(|| black_box(store.preview_sql(black_box(&q))))
        });
    }
}

fn bench_plan(c: &mut Criterion) {
    let t = schema();
    let simple: serde_json::Map<String, serde_json::Value> =
        [("limit", json!("50")), ("page", json!("2")), ("code.eq", json!("A1"))]
            .into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    let rich: serde_json::Map<String, serde_json::Value> = [
        ("limit", json!("200")),
        ("sort", json!("-created_at,code")),
        ("search", json!("acme")),
        ("count", json!("true")),
        ("qty.gte", json!("5")),
        ("status.in", json!("open,paid,shipped")),
        ("meta->color.eq", json!("red")),
        ("filter", json!({"or":[{"code.eq":"X"},{"and":[{"qty.gte":1},{"created_at.between":"2026-01-01,2026-12-31"}]}]})),
    ].into_iter().map(|(k, v)| (k.to_string(), v)).collect();

    c.bench_function("plan_read/simple", |b| b.iter(|| black_box(build_read_query(&t, black_box(&simple), "postgres"))));
    c.bench_function("plan_read/rich_filter_search_count", |b| b.iter(|| black_box(build_read_query(&t, black_box(&rich), "postgres"))));
}

criterion_group!(benches, bench_compile, bench_plan);
criterion_main!(benches);
