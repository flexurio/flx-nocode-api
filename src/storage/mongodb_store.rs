// cfg is already applied from storage/mod.rs; avoid duplicated attribute for clippy

use async_trait::async_trait;
use sonic_rs::{Value, JsonValueTrait, JsonContainerTrait, JsonValueMutTrait};

use crate::storage::ast::{Filter, LogicalPlan, Sort, Val, Expr, Agg, AggFunc, JoinKind};
use crate::storage::traits::{BackendCapabilities, DataStore, TxStore};

use anyhow::{anyhow, Result};

use mongodb::bson::{doc, Bson, Document};
use mongodb::bson::oid::ObjectId;
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

fn sanitize_ident(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
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

fn try_oid_from_str(s: &str) -> Option<Bson> {
    if s.len() == 24 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        ObjectId::parse_str(s).ok().map(Bson::ObjectId)
    } else {
        None
    }
}

fn val_to_bson_for(key: &str, v: &Val) -> Bson {
    // If targeting _id path, attempt to coerce string to ObjectId
    let targets_oid = key == "_id" || key.ends_with("._id");
    match v {
        Val::Str(s) if targets_oid => try_oid_from_str(s).unwrap_or_else(|| Bson::String(s.clone())),
        _ => val_to_bson(v),
    }
}

fn filter_to_bson(f: &Filter) -> Document {
    match f {
    Filter::Eq(k, v) => doc! { k: val_to_bson_for(k, v) },
    Filter::Ne(k, v) => doc! { k: { "$ne": val_to_bson_for(k, v) } },
    Filter::Gt(k, v) => doc! { k: { "$gt": val_to_bson_for(k, v) } },
    Filter::Gte(k, v) => doc! { k: { "$gte": val_to_bson_for(k, v) } },
    Filter::Lt(k, v) => doc! { k: { "$lt": val_to_bson_for(k, v) } },
    Filter::Lte(k, v) => doc! { k: { "$lte": val_to_bson_for(k, v) } },
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
    Filter::In(k, vals) => doc! { k: { "$in": vals.iter().map(|v| val_to_bson_for(k, v)).collect::<Vec<_>>() } },
    Filter::NotIn(k, vals) => doc! { k: { "$nin": vals.iter().map(|v| val_to_bson_for(k, v)).collect::<Vec<_>>() } },
        Filter::IsNull(k) => doc! { k: Bson::Null },
        Filter::IsNotNull(k) => doc! { k: { "$ne": Bson::Null } },
    Filter::Between(k, a, b) => doc! { k: { "$gte": val_to_bson_for(k, a), "$lte": val_to_bson_for(k, b) } },
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

// Rewrite filter keys for Mongo: strip base prefixes, map join table prefixes to join_<table>., and id -> _id
fn rewrite_filter_keys(f: &Filter, base_prefixes: &[String], join_tables: &[String]) -> Filter {
    let map_key = |k: &str| -> String {
        // Strip base alias/table prefix
        let stripped = strip_alias_prefix(k, base_prefixes);
        if stripped != k {
            if stripped == "id" { return "_id".to_string(); }
            return stripped;
        }
        // Map join table prefixes to join_<table>
        if let Some((head, tail)) = k.split_once('.') {
            for jt in join_tables {
                let (jt_coll, _jt_alias) = extract_table_and_alias(jt);
                if head == jt || head == jt_coll {
                    let mapped_tail = if tail == "id" { "_id" } else { tail };
                    return format!("join_{}.{}", jt, mapped_tail);
                }
            }
        }
        // Plain key: map id -> _id
        if k == "id" { "_id".to_string() } else { k.to_string() }
    };

    fn walk(f: &Filter, map: &dyn Fn(&str) -> String) -> Filter {
        match f {
            Filter::Eq(k, v) => Filter::Eq(map(k), v.clone()),
            Filter::Ne(k, v) => Filter::Ne(map(k), v.clone()),
            Filter::Gt(k, v) => Filter::Gt(map(k), v.clone()),
            Filter::Gte(k, v) => Filter::Gte(map(k), v.clone()),
            Filter::Lt(k, v) => Filter::Lt(map(k), v.clone()),
            Filter::Lte(k, v) => Filter::Lte(map(k), v.clone()),
            Filter::Like(k, s) => Filter::Like(map(k), s.clone()),
            Filter::ILike(k, s) => Filter::ILike(map(k), s.clone()),
            Filter::NotLike(k, s) => Filter::NotLike(map(k), s.clone()),
            Filter::In(k, vs) => Filter::In(map(k), vs.clone()),
            Filter::NotIn(k, vs) => Filter::NotIn(map(k), vs.clone()),
            Filter::IsNull(k) => Filter::IsNull(map(k)),
            Filter::IsNotNull(k) => Filter::IsNotNull(map(k)),
            Filter::Between(k, a, b) => Filter::Between(map(k), a.clone(), b.clone()),
            Filter::And(fs) => Filter::And(fs.iter().map(|x| walk(x, map)).collect()),
            Filter::Or(fs) => Filter::Or(fs.iter().map(|x| walk(x, map)).collect()),
        }
    }

    walk(f, &map_key)
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
            // Build base/join context similar to Project
            let (base_table, base_alias) = leftmost_scan_collection(input)
                .map(extract_table_and_alias)
                .unwrap_or_else(|| (String::new(), None));
            let base_prefixes: Vec<String> = vec![base_alias.clone().unwrap_or_default(), base_table.clone()]
                .into_iter().filter(|s| !s.is_empty()).collect();
            let mut join_tables: Vec<String> = Vec::new();
            fn collect_join_tables(plan: &LogicalPlan, out: &mut Vec<String>) {
                match plan {
                    LogicalPlan::Join { input, table, .. } => { out.push(table.clone()); collect_join_tables(input, out); }
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Project { input, .. }
                    | LogicalPlan::Sort { input, .. }
                    | LogicalPlan::Limit { input, .. }
                    | LogicalPlan::Aggregate { input, .. } => collect_join_tables(input, out),
                    _ => {}
                }
            }
            collect_join_tables(input, &mut join_tables);
            let pred_rew = rewrite_filter_keys(predicate, &base_prefixes, &join_tables);
            stages.push(doc! { "$match": filter_to_bson(&pred_rew) });
            Ok((coll, stages))
        }
        LogicalPlan::Project { input, fields } => {
            let (coll, mut stages) = plan_to_pipeline(input)?;
            // Derive base prefixes and list of join tables from the input plan
            let (base_table, base_alias) = leftmost_scan_collection(input)
                .map(extract_table_and_alias)
                .unwrap_or_else(|| (String::new(), None));
            let base_prefixes: Vec<String> = vec![base_alias.clone().unwrap_or_default(), base_table.clone()]
                .into_iter().filter(|s| !s.is_empty()).collect();

            // Collect join table identifiers from the input plan so we can map fields like `bank_types.name`
            fn collect_join_tables(plan: &LogicalPlan, out: &mut Vec<String>) {
                match plan {
                    LogicalPlan::Join { input, table, .. } => {
                        out.push(table.clone());
                        collect_join_tables(input, out);
                    }
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Project { input, .. }
                    | LogicalPlan::Sort { input, .. }
                    | LogicalPlan::Limit { input, .. }
                    | LogicalPlan::Aggregate { input, .. } => collect_join_tables(input, out),
                    _ => {}
                }
            }
            let mut join_tables: Vec<String> = Vec::new();
            collect_join_tables(input, &mut join_tables);

            // Helper to map a logical field path to Mongo field path considering base prefixes and joins
            let map_path = |p: &str| -> String {
                let p_trim = p.trim();
                // Try base prefixes first
                let stripped = strip_alias_prefix(p_trim, &base_prefixes);
                if stripped != p_trim {
                    // Special-case common primary key name 'id' -> Mongo '_id'
                    if stripped == "id" { return "_id".to_string(); }
                    return stripped;
                }
                // Try join mapping: if the path starts with a joined table name, rewrite to join_<table>.<rest>
                if let Some((head, tail)) = p_trim.split_once('.') {
                    for jt in &join_tables {
                        // Compare head with the raw `table` string's collection token (before any alias)
                        let (jt_coll, jt_alias) = extract_table_and_alias(jt);
                        if head == jt || head == jt_coll || jt_alias.as_deref() == Some(head) {
                            let mapped_tail = if tail == "id" { "_id" } else { tail };
                            let suffix = sanitize_ident(jt_alias.as_deref().unwrap_or(&jt_coll));
                            return format!("join_{}.{}", suffix, mapped_tail);
                        }
                    }
                }
                // Fallback: return as-is
                p_trim.to_string()
            };

            // Build $project document with proper aliasing
            let mut proj = Document::new();
            let mut suppress_mongo_id = false;
            for f in fields {
                let f_str = f.as_str();
                // Support case-insensitive " as " aliasing
                if let Some((left_l, alias_l)) = f_str.to_lowercase().split_once(" as ") {
                    // Reconstruct actual alias using original string to preserve case
                    let alias = if let Some((_l, a)) = f_str.split_once(" as ") {
                        a.trim().to_string()
                    } else if let Some((_l, a)) = f_str.split_once(" AS ") {
                        a.trim().to_string()
                    } else {
                        alias_l.trim().to_string()
                    };
                    // Left side before AS: use the portion from the original string up to the length of left_l
                    let left = f_str[..left_l.len()].trim();
                    let mongo_path = map_path(left);
                    if alias == "id" {
                        suppress_mongo_id = true;
                        // if alias id refers to _id or any ._id, cast to string
                        if mongo_path == "_id" || mongo_path.ends_with("._id") {
                            proj.insert("id", doc!{ "$toString": format!("${}", mongo_path) });
                        } else {
                            proj.insert("id", format!("${}", mongo_path));
                        }
                    } else {
                        proj.insert(alias, format!("${}", mongo_path));
                    }
                } else if f_str.contains('.') {
                    // Qualified name without alias: strip/translate and alias to last token of original
                    let path_mapped = map_path(f_str);
                    let key = f_str.rsplit('.').next().unwrap_or(f_str).trim();
                    let key_final = if proj.contains_key(key) { format!("fld_{}", key) } else { key.to_string() };
                    if key_final == "id" && (path_mapped == "_id" || path_mapped.ends_with("._id")) {
                        suppress_mongo_id = true;
                        proj.insert("id", doc!{ "$toString": format!("${}", path_mapped) });
                    } else {
                        proj.insert(key_final, format!("${}", path_mapped));
                    }
                } else {
                    // Simple field, include directly; map 'id' -> '_id' but keep key as 'id'
                    if f_str == "id" {
                        suppress_mongo_id = true;
                        proj.insert("id", doc!{ "$toString": "$_id" });
                    } else {
                        proj.insert(f_str, 1);
                    }
                }
            }
            if suppress_mongo_id { proj.insert("_id", 0); }
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
                .map(extract_table_and_alias)
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
            let mut local_field = strip_alias_prefix(&local_raw, &base_prefixes);
            let mut foreign_field = strip_alias_prefix(&foreign_raw, &join_prefixes);
            if local_field == "id" { local_field = "_id".to_string(); }
            if foreign_field == "id" { foreign_field = "_id".to_string(); }
            // Sanitize $lookup as name using alias if present, else collection name
            let as_suffix = sanitize_ident(join_alias.as_deref().unwrap_or(&join_table));
            let as_field = format!("join_{}", as_suffix);
            stages.push(doc!{
                "$lookup": {
                    "from": join_table,
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

    async fn query(&self, q: &crate::storage::ast::Query) -> Result<Vec<Value>> {
        let plan = q.to_logical_plan();
        self.execute_plan(&plan).await
    }

    async fn insert(&self, collection: &str, docv: Value) -> Result<Value> {
        let coll = self.coll(collection);
        // Map JSON 'id' to Mongo '_id' if present
        let mut bson_doc: Document = mongodb::bson::to_bson(&docv)?.as_document().cloned().unwrap_or_else(Document::new);
        if let Some(id_val) = bson_doc.remove("id") {
            // Only set _id if not already present
            if !bson_doc.contains_key("_id") {
                let id_bson = match id_val {
                    Bson::String(ref s) => try_oid_from_str(s).unwrap_or(id_val),
                    other => other,
                };
                bson_doc.insert("_id", id_bson);
            }
        }
        let res = coll.insert_one(bson_doc).await?;
        Ok(sonic_rs::json!({ "inserted_id": res.inserted_id }))
    }

    async fn update(&self, collection: &str, filter: Option<Filter>, patch: Value) -> Result<u64> {
        let coll = self.coll(collection);
        // Build base prefixes and joins for rewrite context (best-effort: only have collection name here)
        let base_prefixes: Vec<String> = vec![collection.to_string()];
        let join_tables: Vec<String> = vec![];
        let filt = filter
            .map(|f| rewrite_filter_keys(&f, &base_prefixes, &join_tables))
            .map(|f| filter_to_bson(&f))
            .unwrap_or_default();
        // Remap 'id' key in patch to '_id' if present, but generally _id shouldn't be updated; keep other fields
        let mut patch_value = patch;
        if let Some(obj) = patch_value.as_object_mut() {
            // For now simply drop any provided 'id' field to avoid attempting to update _id
            let key = "id".to_string();
            let _ = obj.remove(&key);
        }
        let update_doc: Document = mongodb::bson::to_document(&sonic_rs::json!({ "$set": patch_value }))?;
        let res = coll.update_many(filt, update_doc).await?;
        Ok(res.modified_count as u64)
    }

    async fn delete(&self, collection: &str, filter: Option<Filter>) -> Result<u64> {
        let coll = self.coll(collection);
        let filt = filter.map(|f| filter_to_bson(&f)).unwrap_or_default();
    let res = coll.delete_many(filt).await?;
        Ok(res.deleted_count as u64)
    }

    async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<Value>> {
        let (collection, pipeline) = plan_to_pipeline(plan)?;
        let coll = self.coll(&collection);
    let mut cursor = coll.aggregate(pipeline).await?;
        let mut out = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc) = cursor.next().await { let d = doc?; out.push(sonic_rs::to_value(&d)?); }
        Ok(out)
    }
}

#[allow(dead_code)]
pub struct MongoTxStore {
    store: MongoStore,
}

#[async_trait]
impl TxStore for MongoTxStore {
    async fn query(&mut self, q: &crate::storage::ast::Query) -> Result<Vec<Value>> { self.store.query(q).await }
    async fn insert(&mut self, collection: &str, doc: sonic_rs::Value) -> Result<Value> { self.store.insert(collection, doc).await }
    async fn update(&mut self, collection: &str, filter: Option<Filter>, patch: sonic_rs::Value) -> Result<u64> { self.store.update(collection, filter, patch).await }
    async fn delete(&mut self, collection: &str, filter: Option<Filter>) -> Result<u64> { self.store.delete(collection, filter).await }
    async fn execute_plan(&mut self, plan: &LogicalPlan) -> Result<Vec<Value>> { self.store.execute_plan(plan).await }
    async fn commit(self: Box<Self>) -> Result<()> { Ok(()) }
    async fn rollback(self: Box<Self>) -> Result<()> { Ok(()) }
}
