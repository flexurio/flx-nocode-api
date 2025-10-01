#![cfg(feature = "mongodb")]

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::storage::ast::{Filter, LogicalPlan, Sort, Val, Expr, Agg, AggFunc, JoinKind};
use crate::storage::traits::{BackendCapabilities, DataStore, TxStore};

use anyhow::{anyhow, Result};

use mongodb::bson::{doc, Bson, Document};
use mongodb::{Client, Collection, Database, options::ClientOptions};

// Helpers for parsing table aliases and stripping dotted qualifiers
fn extract_table_and_alias(rel: &str) -> (String, Option<String>) {
    let parts = rel.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() { return (rel.trim().to_string(), None); }
    if parts.len() >= 2 { (parts[0].to_string(), Some(parts[parts.len()-1].to_string())) } else { (parts[0].to_string(), None) }
}

fn leftmost_scan_collection(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan { collection } => Some(collection.as_str()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Join { input, .. } => leftmost_scan_collection(input),
        _ => None,
    }
}

fn strip_alias_prefix(path: &str, prefixes: &[String]) -> String {
    for p in prefixes {
        if p.is_empty() { continue; }
        let pref = format!("{}.", p);
        if path.starts_with(&pref) { return path[pref.len()..].to_string(); }
    }
    path.to_string()
}

fn parse_on_raw_eq(raw: &str) -> Option<(String, String)> {
    let s = raw.replace("==", "=");
    let (l, r) = s.split_once('=')?;
    let trim = |t: &str| t.trim().trim_start_matches('(').trim_end_matches(')').to_string();
    Some((trim(l), trim(r)))
}

fn val_to_bson(v: &Val) -> Bson {
    match v {
        Val::I64(n) => Bson::Int64(*n),
        Val::F64(n) => Bson::Double(*n),
        Val::Bool(b) => Bson::Boolean(*b),
        Val::Str(s) => Bson::String(s.clone()),
        Val::Null => Bson::Null,
    }
}

fn filter_to_bson(f: &Filter) -> Document {
    match f {
        Filter::Eq(k, v) => doc! { k: val_to_bson(v) },
        Filter::Ne(k, v) => doc! { k: { "$ne": val_to_bson(v) } },
        Filter::Gt(k, v) => doc! { k: { "$gt": val_to_bson(v) } },
        Filter::Gte(k, v) => doc! { k: { "$gte": val_to_bson(v) } },
        Filter::Lt(k, v) => doc! { k: { "$lt": val_to_bson(v) } },
        Filter::Lte(k, v) => doc! { k: { "$lte": val_to_bson(v) } },
        Filter::Like(k, pat) | Filter::ILike(k, pat) | Filter::NotLike(k, pat) => {
            // Convert SQL LIKE to regex: % -> .*, _ -> ., escape others
            let mut re = String::with_capacity(pat.len() * 2);
            for ch in pat.chars() {
                match ch {
                    '%' => re.push_str(".*"),
                    '_' => re.push('.'),
                    _ => re.push_str(&regex::escape(&ch.to_string())),
                }
            }
            let options = if matches!(f, Filter::ILike(_, _)) { Some("i") } else { None };
            let expr = if matches!(f, Filter::NotLike(_, _)) {
                doc! { "$not": { "$regex": re, "$options": options.unwrap_or("") } }
            } else {
                doc! { "$regex": re, "$options": options.unwrap_or("") }
            };
            doc! { k: expr }
        }
        Filter::In(k, vals) => doc! { k: { "$in": vals.iter().map(val_to_bson).collect::<Vec<_>>() } },
        Filter::NotIn(k, vals) => doc! { k: { "$nin": vals.iter().map(val_to_bson).collect::<Vec<_>>() } },
        Filter::IsNull(k) => doc! { k: Bson::Null },
        Filter::IsNotNull(k) => doc! { k: { "$ne": Bson::Null } },
        Filter::Between(k, a, b) => doc! { k: { "$gte": val_to_bson(a), "$lte": val_to_bson(b) } },
        Filter::And(fs) => {
            let parts: Vec<Document> = fs.iter().map(filter_to_bson).collect();
            doc! { "$and": parts }
        }
        Filter::Or(fs) => {
            let parts: Vec<Document> = fs.iter().map(filter_to_bson).collect();
            doc! { "$or": parts }
        }
    }
}

fn sort_to_bson(sort: &[Sort]) -> Document {
    let mut d = Document::new();
    for s in sort {
        d.insert(&s.field, if s.asc { 1 } else { -1 });
    }
    d
}

/// Build an aggregation pipeline from a LogicalPlan. This function is pure and returns the target
/// collection name and pipeline stages. Not all nodes are supported yet (JOIN, Aggregate minimal).
fn plan_to_pipeline(plan: &LogicalPlan) -> Result<(String, Vec<Document>)> {
    match plan {
        LogicalPlan::Scan { collection } => Ok((collection.clone(), vec![])),
        LogicalPlan::Filter { input, predicate } => {
            let (coll, mut stages) = plan_to_pipeline(input)?;
            stages.push(doc! { "$match": filter_to_bson(predicate) });
            Ok((coll, stages))
        }
        LogicalPlan::Project { input, fields } => {
            let (coll, mut stages) = plan_to_pipeline(input)?;
            let mut proj = Document::new();
            for f in fields { proj.insert(f, 1); }
            stages.push(doc! { "$project": proj });
            Ok((coll, stages))
        }
        LogicalPlan::Sort { input, by } => {
            let (coll, mut stages) = plan_to_pipeline(input)?;
            stages.push(doc! { "$sort": sort_to_bson(by) });
            Ok((coll, stages))
        }
        LogicalPlan::Limit { input, offset, limit } => {
            let (coll, mut stages) = plan_to_pipeline(input)?;
            if let Some(skip) = offset { stages.push(doc!{ "$skip": (*skip as i64) }); }
            if let Some(take) = limit { stages.push(doc!{ "$limit": (*take as i64) }); }
            Ok((coll, stages))
        }
        LogicalPlan::Aggregate { input, group_by, aggs, having } => {
            let (coll, mut stages) = plan_to_pipeline(input)?;
            let mut group_stage = Document::new();
            if !group_by.is_empty() {
                let mut id_doc = Document::new();
                for f in group_by { id_doc.insert(f, format!("${}", f)); }
                group_stage.insert("_id", id_doc);
            } else {
                group_stage.insert("_id", Bson::Null);
            }
            for Agg { alias, func } in aggs {
                match func {
                    AggFunc::CountAll => { group_stage.insert(alias, doc!{ "$sum": 1 }); },
                    AggFunc::Count(field) => {
                        group_stage.insert(alias, doc!{ "$sum": { "$cond": [ { "$ne": [ format!("${}", field), Bson::Null ] }, 1, 0 ] } });
                    }
                    AggFunc::Sum(field) => { group_stage.insert(alias, doc!{ "$sum": format!("${}", field) }); }
                    AggFunc::Avg(field) => { group_stage.insert(alias, doc!{ "$avg": format!("${}", field) }); }
                    AggFunc::Min(field) => { group_stage.insert(alias, doc!{ "$min": format!("${}", field) }); }
                    AggFunc::Max(field) => { group_stage.insert(alias, doc!{ "$max": format!("${}", field) }); }
                }
            }
            stages.push(doc!{ "$group": group_stage });
            if !having.is_empty() {
                // having currently supports boolean expr over columns; crude mapping by reusing filter keys
                let filters: Vec<Filter> = having.iter().filter_map(|e| match e {
                    Expr::Eq(k, v) => Some(Filter::Eq(k.clone(), v.clone())),
                    Expr::Ne(k, v) => Some(Filter::Ne(k.clone(), v.clone())),
                    Expr::Gt(k, v) => Some(Filter::Gt(k.clone(), v.clone())),
                    Expr::Gte(k, v) => Some(Filter::Gte(k.clone(), v.clone())),
                    Expr::Lt(k, v) => Some(Filter::Lt(k.clone(), v.clone())),
                    Expr::Lte(k, v) => Some(Filter::Lte(k.clone(), v.clone())),
                    Expr::And(xs) => Some(Filter::And(xs.iter().filter_map(|x| if let Expr::Eq(k,v)=x { Some(Filter::Eq(k.clone(), v.clone())) } else { None }).collect())),
                    Expr::Or(xs) => Some(Filter::Or(xs.iter().filter_map(|x| if let Expr::Eq(k,v)=x { Some(Filter::Eq(k.clone(), v.clone())) } else { None }).collect())),
                    _ => None,
                }).collect();
                if !filters.is_empty() {
                    let f = if filters.len() == 1 { filters[0].clone() } else { Filter::And(filters) };
                    stages.push(doc! { "$match": filter_to_bson(&f) });
                }
            }
            Ok((coll, stages))
        }
        LogicalPlan::Join { input, kind, table, on_expr, on_raw, .. } => {
            // Minimal LEFT join mapping to $lookup; INNER can be approximated with $lookup + $unwind + $match
            let (coll, mut stages) = plan_to_pipeline(input)?;
            // Resolve base and join aliases to support dotted qualifiers like o.customer_id = c.id
            let (base_table, base_alias) = leftmost_scan_collection(input)
                .map(|c| extract_table_and_alias(c))
                .unwrap_or_else(|| (String::new(), None));
            let (join_table, join_alias) = extract_table_and_alias(table);
            let base_prefixes: Vec<String> = vec![base_alias.clone().unwrap_or_default(), base_table.clone()]
                .into_iter().filter(|s| !s.is_empty()).collect();
            let join_prefixes: Vec<String> = vec![join_alias.clone().unwrap_or_default(), join_table.clone()]
                .into_iter().filter(|s| !s.is_empty()).collect();

            let (a_raw, b_raw) = match on_expr {
                Some(Expr::ColEq(a, b)) => (a.clone(), b.clone()),
                _ => {
                    if let Some(raw) = on_raw {
                        if let Some((l, r)) = parse_on_raw_eq(raw) { (l, r) } else { return Err(anyhow!("JOIN on_raw unsupported format; expected 'local = foreign'")); }
                    } else {
                        return Err(anyhow!("Mongo JOIN expects on_expr = ColEq(local, foreign) or parseable on_raw"));
                    }
                }
            };
            let a_base = base_prefixes.iter().any(|p| a_raw.starts_with(&format!("{}.", p)));
            let b_base = base_prefixes.iter().any(|p| b_raw.starts_with(&format!("{}.", p)));
            let (local_raw, foreign_raw) = if a_base || (!b_base) { (a_raw, b_raw) } else { (b_raw, a_raw) };
            let local_field = strip_alias_prefix(&local_raw, &base_prefixes);
            let foreign_field = strip_alias_prefix(&foreign_raw, &join_prefixes);
            let as_field = format!("join_{}", table);
            stages.push(doc!{
                "$lookup": {
                    "from": table,
                    "localField": local_field,
                    "foreignField": foreign_field,
                    "as": &as_field
                }
            });
            if matches!(kind, JoinKind::Inner) {
                stages.push(doc!{ "$unwind": { "path": format!("${}", as_field), "preserveNullAndEmptyArrays": false } });
            }
            Ok((coll, stages))
        }
        LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. } => {
            Err(anyhow!("Use insert/update/delete methods instead of execute_plan for writes"))
        }
    }
}

pub struct MongoStore {
    db: Database,
}

impl MongoStore {
    pub async fn connect(uri: &str, db_name: &str) -> Result<Self> {
        let options = ClientOptions::parse(uri).await?;
        let client = Client::with_options(options)?;
        let db = client.database(db_name);
        Ok(Self { db })
    }

    fn coll(&self, name: &str) -> Collection<Document> { self.db.collection::<Document>(name) }
}

#[async_trait]
impl DataStore for MongoStore {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities { transactions: false, joins: true, like: true, sql_formula: false }
    }

    async fn query(&self, q: &crate::storage::ast::Query) -> Result<Vec<JsonValue>> {
        let plan = q.to_logical_plan();
        self.execute_plan(&plan).await
    }

    async fn insert(&self, collection: &str, docv: JsonValue) -> Result<JsonValue> {
        let coll = self.coll(collection);
        let bson_doc: Document = mongodb::bson::to_bson(&docv)?.as_document().cloned().unwrap_or_else(Document::new);
    let res = coll.insert_one(bson_doc).await?;
        Ok(serde_json::json!({ "inserted_id": res.inserted_id }))
    }

    async fn update(&self, collection: &str, filter: Option<Filter>, patch: JsonValue) -> Result<u64> {
        let coll = self.coll(collection);
        let filt = filter.map(|f| filter_to_bson(&f)).unwrap_or_else(Document::new);
        let update_doc: Document = mongodb::bson::to_document(&serde_json::json!({ "$set": patch }))?;
    let res = coll.update_many(filt, update_doc).await?;
        Ok(res.modified_count as u64)
    }

    async fn delete(&self, collection: &str, filter: Option<Filter>) -> Result<u64> {
        let coll = self.coll(collection);
        let filt = filter.map(|f| filter_to_bson(&f)).unwrap_or_else(Document::new);
    let res = coll.delete_many(filt).await?;
        Ok(res.deleted_count as u64)
    }

    async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<JsonValue>> {
        let (collection, pipeline) = plan_to_pipeline(plan)?;
        let coll = self.coll(&collection);
    let mut cursor = coll.aggregate(pipeline).await?;
        let mut out = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc) = cursor.next().await { let d = doc?; out.push(serde_json::to_value(d)?); }
        Ok(out)
    }
}

pub struct MongoTxStore {
    store: MongoStore,
}

#[async_trait]
impl TxStore for MongoTxStore {
    async fn query(&mut self, q: &crate::storage::ast::Query) -> Result<Vec<JsonValue>> { self.store.query(q).await }
    async fn insert(&mut self, collection: &str, doc: JsonValue) -> Result<JsonValue> { self.store.insert(collection, doc).await }
    async fn update(&mut self, collection: &str, filter: Option<Filter>, patch: JsonValue) -> Result<u64> { self.store.update(collection, filter, patch).await }
    async fn delete(&mut self, collection: &str, filter: Option<Filter>) -> Result<u64> { self.store.delete(collection, filter).await }
    async fn execute_plan(&mut self, plan: &LogicalPlan) -> Result<Vec<JsonValue>> { self.store.execute_plan(plan).await }
    async fn commit(self: Box<Self>) -> Result<()> { Ok(()) }
    async fn rollback(self: Box<Self>) -> Result<()> { Ok(()) }
}
