use actix_multipart::Multipart;
use actix_web::{http::header, web, HttpResponse, Responder};
use sonic_rs::{Value, JsonValueTrait, JsonContainerTrait};
use std::collections::HashSet;
use std::sync::Arc;

use crate::audit::{write_audit, AuditEntry};
use crate::auth::{check_access, get_user_info_from_token, Claims};
use crate::helpers::{filter_table_schema, multipart_to_json, split_column_operator};
use crate::log::log_output;
use crate::model::{ReferenceForeignKey, TableSchema, WebResponse};
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV, Expr as QE};
use crate::storage::sql_store::SqlStore;
use crate::AppState;
use chrono::Local;

/// Export data for a route using filters provided via multipart fields.
/// Fields supported (same as nocode GET):
/// - filters according to schema.get.parameters
pub async fn export(
    state: web::Data<AppState>,
    route: Arc<str>,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder {
    let table_schemas = &schemas.0;

    // AuthZ like GET (read)
    let mut claims = Claims::default();
    if !state.route_publics.iter().any(|r| r == route.as_ref()) {
        let req_for_auth = req.clone();
        claims = match get_user_info_from_token(req_for_auth, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: crate::constants::ERR_INVALID_TOKEN.to_string(),
                    total_data: 0,
                    data: Value::default(),
                });
            }
        };
        if !check_access(&claims, &route, "read") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: crate::constants::ERR_UNAUTHORIZED.to_string(),
                total_data: 0,
                data: Value::default(),
            });
        }
    }

    // Parse multipart fields to JSON (re-use secure helper)
    let body_json: Value = match multipart_to_json(multipart).await {
        Ok(v) => v,
        Err(_) => Value::from(sonic_rs::Object::new()),
    };

    // Type and filename
    let mut export_type = body_json
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("csv")
        .to_lowercase();
    if export_type != "xlsx" && export_type != "csv" { export_type = "csv".to_string(); }
    let filename_base = body_json
        .get("filename")
        .and_then(|v| v.as_str())
        .unwrap_or(&route);

    // Schema lookup
    let table_schema: TableSchema = filter_table_schema(table_schemas, route.as_ref());
    if table_schema.table.is_empty() {
        let message_error = format!(
            "Entity {} on folder config/{}.json not found",
            route, route
        );
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::default(),
        });
    }

    // Build AST query using same rules as nocode GET (portable across DBs)
    let i_limit = body_json
        .get("limit")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
        .map(|v| v.clamp(1, 100_000))
        .unwrap_or(10_000);

    let mut is_deleted_at = true;

    // Preprocess parameters map for convenience
    let params_map = body_json.as_object().cloned().unwrap_or_default();

    // Defaults for ordering
    let mut order_col_ast = table_schema.get.order_by.clone().join(", ");
    let mut order_type_ast = "ASC".to_string();

    // Collect paramjoins (sanitized) and flag deleted_at override
    let mut paramjoins_ast: Vec<(String, String)> = Vec::new();
    for p in &table_schema.get.parameters {
        if p.contains("paramjoin") {
            if let Some(v) = params_map.get(p).and_then(|vv| vv.as_str()) {
                let name = p.replace(".eq", "");
                let safe_val: String = v.chars().filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.').collect();
                paramjoins_ast.push((name, safe_val));
            }
        }
        if p.contains("deleted_at") && params_map.get(p).is_some() { is_deleted_at = false; }
    }
    // Sort by name length desc to avoid partial overlaps
    paramjoins_ast.sort_by(|a,b| b.0.len().cmp(&a.0.len()));

    fn substitute_paramjoins(logical: &str, paramjoins: &[(String, String)]) -> String {
        if paramjoins.is_empty() { return logical.to_string(); }
        let bytes = logical.as_bytes();
        let mut out = String::with_capacity(logical.len());
        let mut i = 0;
        'outer: while i < bytes.len() {
            for (name, val) in paramjoins {
                if bytes[i..].starts_with(name.as_bytes()) {
                    out.push_str(val);
                    i += name.len();
                    continue 'outer;
                }
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        out
    }

    // Helper to parse primitive to QV
    fn to_val(s: &str) -> QV {
        if s.eq_ignore_ascii_case("true") { return QV::Bool(true); }
        if s.eq_ignore_ascii_case("false") { return QV::Bool(false); }
        if let Ok(i) = s.parse::<i64>() { return QV::I64(i); }
        if let Ok(f) = s.parse::<f64>() { return QV::F64(f); }
        QV::Str(s.to_string())
    }

    // Build filters only from allowed parameters
    let mut filters: Vec<QF> = Vec::new();
    for param in &table_schema.get.parameters {
        if let Some(value) = params_map.get(param) {
            let value_str = value.as_str().unwrap_or("").to_string();
            match param.as_str() {
                "sort" => {
                    if !value_str.is_empty() {
                        let sanitized: String = value_str
                            .chars()
                            .filter(|c| c.is_alphanumeric() || *c == '.' || *c == ',' || *c == ' ' || *c == '_' || *c == '-')
                            .collect();
                        order_col_ast = sanitized;
                    }
                }
                "ascending" => {
                    order_type_ast = if value_str.eq_ignore_ascii_case("true") { "ASC".into() } else { "DESC".into() };
                }
                "limit" | "page" => { /* handled elsewhere or ignored for export */ }
                p if p.contains("paramjoin") => { /* handled later in join substitution */ }
                "search" => {
                    let v = value_str;
                    if !v.is_empty() {
                        let mut ors: Vec<QF> = Vec::new();
                        for column in table_schema.primary_key.columns.iter() {
                            let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                            ors.push(QF::ILike(col, format!("%{}%", v)));
                        }
                        for index in table_schema.indexes.iter() {
                            for column in index.columns.iter() {
                                let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                                ors.push(QF::ILike(col, format!("%{}%", v)));
                            }
                        }
                        if !ors.is_empty() { filters.push(QF::Or(ors)); }
                    }
                }
                p if p.contains('|') => {
                    let mut ors: Vec<QF> = Vec::new();
                    for part in p.split('|') {
                        let (column, operator, val) = split_column_operator(part, &table_schema.table, &value_str);
                        let f = match operator.as_str() {
                            "=" => QF::Eq(column, to_val(&val)),
                            "<" => QF::Lt(column, to_val(&val)),
                            "<=" => QF::Lte(column, to_val(&val)),
                            ">" => QF::Gt(column, to_val(&val)),
                            ">=" => QF::Gte(column, to_val(&val)),
                            "like" => QF::ILike(column, val),
                            "nin" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                        let vs = arr.into_iter().map(|x| {
                                            if let Some(i) = x.as_i64() { QV::I64(i) }
                                            else if let Some(f) = x.as_f64() { QV::F64(f) }
                                            else if let Some(b) = x.as_bool() { QV::Bool(b) }
                                            else if let Some(s) = x.as_str() { QV::Str(s.to_string()) }
                                            else if x.is_null() { QV::Null }
                                            else { QV::Str(x.to_string()) }
                                        }).collect::<Vec<QV>>();
                                        QF::NotIn(column, vs)
                                    } else { QF::NotIn(column, vec![to_val(&val)]) }
                                } else if value_str.contains(',') {
                                    let vs = value_str.split(',').map(|s| to_val(s.trim())).collect::<Vec<QV>>();
                                    QF::NotIn(column, vs)
                                } else { QF::NotIn(column, vec![to_val(&val)]) }
                            }
                            "between" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                        if arr.len() == 2 {
                                            let to_qv = |v: &Value| {
                                                if let Some(i) = v.as_i64() { QV::I64(i) }
                                                else if let Some(f) = v.as_f64() { QV::F64(f) }
                                                else if let Some(b) = v.as_bool() { QV::Bool(b) }
                                                else if let Some(s) = v.as_str() { QV::Str(s.to_string()) }
                                                else if v.is_null() { QV::Null }
                                                else { QV::Str(v.to_string()) }
                                            };
                                            QF::Between(column, to_qv(&arr[0]), to_qv(&arr[1]))
                                        } else { QF::Eq(column, to_val(&val)) }
                                    } else { QF::Eq(column, to_val(&val)) }
                                } else if value_str.contains(',') {
                                    let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                                    let a = parts.next().unwrap_or_default();
                                    let b = parts.next().unwrap_or_default();
                                    QF::Between(column, to_val(&a), to_val(&b))
                                } else { QF::Eq(column, to_val(&val)) }
                            }
                            "is" => {
                                if val.eq_ignore_ascii_case("NULL") { QF::IsNull(column) }
                                else if val.eq_ignore_ascii_case("NOT NULL") { QF::IsNotNull(column) }
                                else { QF::Eq(column, to_val(&val)) }
                            }
                            _ => QF::Eq(column, to_val(&val)),
                        };
                        ors.push(f);
                    }
                    if !ors.is_empty() { filters.push(QF::Or(ors)); }
                }
                _ => {
                    let (column, operator, val) = split_column_operator(param, &table_schema.table, &value_str);
                    if operator == "is" {
                        if value_str.eq_ignore_ascii_case("NULL") {
                            filters.push(QF::IsNull(column));
                        } else if value_str.eq_ignore_ascii_case("NOT NULL") {
                            filters.push(QF::IsNotNull(column));
                        } else {
                            filters.push(QF::Eq(column, to_val(&val)));
                        }
                    } else {
                        let f = match operator.as_str() {
                            "=" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                        let vs = arr.into_iter().map(|x| {
                                            if let Some(i) = x.as_i64() { QV::I64(i) }
                                            else if let Some(f) = x.as_f64() { QV::F64(f) }
                                            else if let Some(b) = x.as_bool() { QV::Bool(b) }
                                            else if let Some(s) = x.as_str() { QV::Str(s.to_string()) }
                                            else if x.is_null() { QV::Null }
                                            else { QV::Str(x.to_string()) }
                                        }).collect::<Vec<QV>>();
                                        QF::In(column, vs)
                                    } else { QF::Eq(column, to_val(&val)) }
                                } else if value_str.contains(',') {
                                    let vs = value_str
                                        .split(',')
                                        .map(|s| to_val(s.trim()))
                                        .collect::<Vec<QV>>();
                                    QF::In(column, vs)
                                } else {
                                    QF::Eq(column, to_val(&val))
                                }
                            }
                            "<" => QF::Lt(column, to_val(&val)),
                            "<=" => QF::Lte(column, to_val(&val)),
                            ">" => QF::Gt(column, to_val(&val)),
                            ">=" => QF::Gte(column, to_val(&val)),
                            "like" => QF::ILike(column, val),
                            "nin" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                        let vs = arr.into_iter().map(|x| {
                                            if let Some(i) = x.as_i64() { QV::I64(i) }
                                            else if let Some(f) = x.as_f64() { QV::F64(f) }
                                            else if let Some(b) = x.as_bool() { QV::Bool(b) }
                                            else if let Some(s) = x.as_str() { QV::Str(s.to_string()) }
                                            else if x.is_null() { QV::Null }
                                            else { QV::Str(x.to_string()) }
                                        }).collect::<Vec<QV>>();
                                        QF::NotIn(column, vs)
                                    } else { QF::NotIn(column, vec![to_val(&val)]) }
                                } else if value_str.contains(',') {
                                    let vs = value_str.split(',').map(|s| to_val(s.trim())).collect::<Vec<QV>>();
                                    QF::NotIn(column, vs)
                                } else { QF::NotIn(column, vec![to_val(&val)]) }
                            }
                            "between" => {
                                let val_trim = value_str.trim();
                                if val_trim.starts_with('[') && val_trim.ends_with(']') {
                                    if let Ok(arr) = sonic_rs::from_str::<Vec<Value>>(val_trim) {
                                        if arr.len() == 2 {
                                            let to_qv = |v: &Value| {
                                                if let Some(i) = v.as_i64() { QV::I64(i) }
                                                else if let Some(f) = v.as_f64() { QV::F64(f) }
                                                else if let Some(b) = v.as_bool() { QV::Bool(b) }
                                                else if let Some(s) = v.as_str() { QV::Str(s.to_string()) }
                                                else if v.is_null() { QV::Null }
                                                else { QV::Str(v.to_string()) }
                                            };
                                            QF::Between(column, to_qv(&arr[0]), to_qv(&arr[1]))
                                        } else { QF::Eq(column, to_val(&val)) }
                                    } else { QF::Eq(column, to_val(&val)) }
                                } else if value_str.contains(',') {
                                    let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                                    let a = parts.next().unwrap_or_default();
                                    let b = parts.next().unwrap_or_default();
                                    QF::Between(column, to_val(&a), to_val(&b))
                                } else { QF::Eq(column, to_val(&val)) }
                            }
                            _ => QF::Eq(column, to_val(&val)),
                        };
                        filters.push(f);
                    }
                }
            }
        }
    }

    if is_deleted_at {
        filters.push(QF::IsNull(format!("{}.deleted_at", table_schema.table)));
    }

    // Build Query AST
    let select_columns = table_schema.get.columns.clone();
    let mut q = QQ::from(table_schema.table.clone()).select(select_columns);
    if !filters.is_empty() {
        q = q.r#where(QF::And(filters));
    }

    // Order by: allow alias/unqualified names present in projection/index/join columns
    let mut allowed_unqualified: HashSet<String> = HashSet::new();
    for c in table_schema.get.columns.iter() {
        let s = c.trim();
        if let Some((left, _right_alias_lc)) = s.to_lowercase().split_once(" as ") {
            if let Some((_l, alias_actual)) = s.split_once(" as ") {
                allowed_unqualified.insert(alias_actual.trim().to_string());
            } else if let Some((_l, alias_actual)) = s.split_once(" AS ") {
                allowed_unqualified.insert(alias_actual.trim().to_string());
            }
            let left_orig = &s[..left.len()];
            let base = left_orig.rsplit('.').next().unwrap_or(left_orig).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        } else {
            let base = s.rsplit('.').next().unwrap_or(s).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        }
    }
    for idx in table_schema.indexes.iter() {
        for c in idx.columns.iter() {
            let base = c.rsplit('.').next().unwrap_or(c).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        }
    }
    for j in table_schema.get.join_tables.iter() {
        for c in j.columns.iter() {
            let base = c.rsplit('.').next().unwrap_or(c).trim();
            if !base.is_empty() { allowed_unqualified.insert(base.to_string()); }
        }
    }

    let global_asc = order_type_ast.eq_ignore_ascii_case("ASC");
    let mut any_order = false;
    for token in order_col_ast.split(',') {
        let raw = token.trim();
        if raw.is_empty() { continue; }
        let mut col_str = raw;
        let mut asc_opt: Option<bool> = None;
        if let Some(stripped) = raw.strip_prefix('-') {
            col_str = stripped.trim();
            asc_opt = Some(false);
        } else if let Some((name, dir)) = raw.rsplit_once(' ') {
            let d = dir.trim().to_ascii_lowercase();
            if d == "asc" || d == "desc" {
                col_str = name.trim();
                asc_opt = Some(d == "asc");
            }
        }
        let unqualified = col_str.rsplit('.').next().unwrap_or(col_str).trim();
        if !allowed_unqualified.contains(unqualified) { continue; }
        let asc = asc_opt.unwrap_or(global_asc);
        q = q.order_by(col_str.to_string(), asc);
        any_order = true;
    }
    if !any_order {
        for col in table_schema.get.order_by.iter() {
            let col_trim = col.trim();
            if col_trim.is_empty() { continue; }
            let unqualified = col_trim.rsplit('.').next().unwrap_or(col_trim);
            if allowed_unqualified.contains(unqualified) || allowed_unqualified.contains(col_trim) {
                q = q.order_by(col_trim.to_string(), global_asc);
            }
        }
    }

    // JOINs (apply optimized paramjoin substitutions)
    if !table_schema.get.join_tables.is_empty() {
        for j in &table_schema.get.join_tables {
            let logical = substitute_paramjoins(&j.logical, &paramjoins_ast);
            if j.type_join.eq_ignore_ascii_case("left") {
                q = q.join_left_expr(j.table.clone(), QE::Raw(logical));
            } else {
                q = q.join_inner_expr(j.table.clone(), QE::Raw(logical));
            }
        }
    }

    // GROUP BY and HAVING
    if !table_schema.get.column_groups.is_empty() {
        q = q.group_by(table_schema.get.column_groups.clone());
    }
    if !table_schema.get.having.is_empty() {
        let hv = table_schema
            .get
            .having
            .iter()
            .cloned()
            .map(QE::Raw)
            .collect::<Vec<_>>();
        q = q.having_expr(hv);
    }

    // LIMIT only (no pagination by default for export)
    q = q.limit(i_limit as u32);

    // Execute via DataStore (AST). Use SqlStore only for preview logs.
    let ds = SqlStore::new(state.db.clone(), state.db_type.clone());
    if *crate::ISDEBUG {
        let (s_sql_dbg, params_dbg) = ds.preview_sql(&q);
    log_output("QUERY", "EXPORT(AST)", route.as_ref(), s_sql_dbg.clone(), true);
    log_output("PARAMS", "EXPORT(AST)", route.as_ref(), format!("{:?}", params_dbg), true);
    }
    let rows = match state.store.query(&q).await {
        Ok(res) => res,
        Err(e) => {
            return HttpResponse::InternalServerError().json(WebResponse {
                success: false,
                message: format!("Error EXPORT(AST) query: {}", e),
                total_data: 0,
                data: Value::default(),
            })
        }
    };

    // Convert rows to a stable vector of maps for export
    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    if let Some(first) = rows.first() {
        if let Some(obj) = first.as_object() {
            headers = obj.iter().map(|(k, _)| k.to_string()).collect();
        }
    }
    for row in rows.iter() {
        if let Some(obj) = row.as_object() {
            if headers.is_empty() {
                headers = obj.iter().map(|(k, _)| k.to_string()).collect();
            }
            let mut line: Vec<String> = Vec::with_capacity(headers.len());
            for h in headers.iter() {
                if let Some(val) = obj.get(h) {
                    if val.is_null() { line.push(String::new()); continue; }
                    else if let Some(s) = val.as_str() { line.push(s.to_string()); continue; }
                    else if let Some(i) = val.as_i64() { line.push(i.to_string()); continue; }
                    else if let Some(u) = val.as_u64() { line.push(u.to_string()); continue; }
                    else if let Some(f) = val.as_f64() { line.push(f.to_string()); continue; }
                    else if let Some(b) = val.as_bool() { line.push(if b { "1".into() } else { "0".into() }); continue; }
                    else { line.push(val.to_string().trim_matches('"').to_string()); continue; }
                } else { line.push(String::new()); }
            }
            data_rows.push(line);
        }
    }

    let ts = Local::now().format("%Y%m%d-%H%M%S");
    let (content_type, file_ext, bytes) = if export_type == "xlsx" {
        let buf = write_xlsx(&headers, &data_rows).unwrap_or_else(|e| {
            log_output(
                "WARN",
                "EXPORT",
                route.as_ref(),
                format!("Falling back to CSV: {}", e),
                true,
            );
            write_csv(&headers, &data_rows).unwrap_or_default()
        });
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(),
            "xlsx".to_string(),
            buf,
        )
    } else {
        let buf = write_csv(&headers, &data_rows).unwrap_or_default();
        ("text/csv".to_string(), "csv".to_string(), buf)
    };

    // Audit
    write_audit(&AuditEntry {
        at: Local::now().to_rfc3339(),
        actor_id: claims.id,
        action: "EXPORT",
        route: &route,
        id: None,
        ip: req.peer_addr().map(|a| a.ip().to_string()).as_deref(),
    });

    let filename = format!("{}-{}.{}", filename_base, ts, file_ext);
    HttpResponse::Ok()
        .insert_header((
            header::CONTENT_TYPE,
            header::HeaderValue::from_str(&content_type).unwrap(),
        ))
        .insert_header((
            header::CONTENT_DISPOSITION,
            header::HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename))
                .unwrap(),
        ))
        .body(bytes)
}

fn write_csv(headers: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, anyhow::Error> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(Vec::new());
    if !headers.is_empty() {
        wtr.write_record(headers)?;
    }
    for r in rows.iter() {
        wtr.write_record(r)?;
    }
    wtr.flush()?;
    Ok(wtr.into_inner()?)
}

fn write_xlsx(headers: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, anyhow::Error> {
    use rust_xlsxwriter::{Format, Workbook};
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    let mut row_idx: u32 = 0;
    if !headers.is_empty() {
        let bold = Format::new().set_bold();
        for (col, h) in headers.iter().enumerate() {
            worksheet.write_string_with_format(row_idx, col as u16, h, &bold)?;
        }
        row_idx += 1;
    }
    for r in rows.iter() {
        for (c, val) in r.iter().enumerate() {
            worksheet.write_string(row_idx, c as u16, val)?;
        }
        row_idx += 1;
    }
    let buf: Vec<u8> = workbook.save_to_buffer()?;
    Ok(buf)
}
