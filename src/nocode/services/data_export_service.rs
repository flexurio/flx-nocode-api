use actix_multipart::Multipart;
use actix_web::{http::header, web, HttpResponse};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use chrono::Local;

use crate::AppState;
use crate::model::{ParamJoin, TableSchema, WebResponse};
use crate::audit::{AuditEntry, write_audit};
use crate::auth::{check_access, get_user_info_from_token};
use crate::helpers::{multipart_to_json, split_column_operator};
use crate::log::log_output;
use crate::nocode::repositories::data_export_repo;
use crate::storage::ast::{Filter as QF, Query as QQ, Val as QV, Expr as QE}; // Removed Join import if unused
// JoinKind unused

// Helper to parse primitive to QV (Same as get, maybe verify if we can share this?)
// Leaving here to decouple from get service details.
fn to_val(s: &str) -> QV {
    if s.eq_ignore_ascii_case("true") { return QV::Bool(true); }
    if s.eq_ignore_ascii_case("false") { return QV::Bool(false); }
    if let Ok(i) = s.parse::<i64>() { return QV::I64(i); }
    if let Ok(f) = s.parse::<f64>() { return QV::F64(f); }
    QV::Str(s.to_string())
}

#[allow(clippy::collapsible_if)]
pub async fn process_export_request(
    state: &web::Data<AppState>,
    route: &str,
    table_schema: &Arc<TableSchema>,
    multipart: Multipart,
    req: &actix_web::HttpRequest,
) -> HttpResponse {

    // Auth matches GET (read)
    let mut actor_id_opt: Option<String> = None;

    if state.require_auth && !state.route_publics.contains(&route.to_string()) {
        let claims = match get_user_info_from_token(req, state.clone()) {
            Ok(c) => c,
            Err(_) => {
                return HttpResponse::Unauthorized().json(WebResponse {
                    success: false,
                    message: "Invalid token".to_string(),
                    total_data: 0,
                    data: Value::Null,
                });
            }
        };
        if let Err(e) = check_access(&claims, req) {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: format!("Unauthorized: {}", e),
                total_data: 0,
                data: Value::Null,
            });
        }
        actor_id_opt = Some(claims.id);
    }

    // Parse multipart
    let body_json: Value = match multipart_to_json(multipart).await {
        Ok(v) => v,
        Err(_) => Value::Object(serde_json::Map::new()),
    };

    // Export Config
    let mut export_type = body_json
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("csv")
        .to_lowercase();
    if export_type != "xlsx" && export_type != "csv" { export_type = "csv".to_string(); }
    
    let filename_base = body_json
        .get("filename")
        .and_then(|v| v.as_str())
        .unwrap_or(route);

    // Schema Check
    if table_schema.table.is_empty() {
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: format!("Entity {} not found", route),
            total_data: 0,
            data: Value::Null,
        });
    }

    // AST Params
    let i_limit = body_json
        .get("limit")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
        .map(|v| v.clamp(1, 100_000))
        .unwrap_or(10_000);

    let mut is_deleted_at = true;
    let params_map = body_json.as_object().cloned().unwrap_or_default();
    let mut table_schema_get_params = table_schema.get.parameters.clone();

    // Required Params
    {
        let mut missing_required: Vec<String> = Vec::new();
        for param in &mut table_schema_get_params {
            if param.starts_with('*') {
                let param_name = param.trim_start_matches('*');
                if !params_map.contains_key(param_name) || 
                   params_map.get(param_name).map(|v| v.as_str().unwrap_or("").is_empty()).unwrap_or(true) {
                    missing_required.push(param_name.to_string());
                } else {
                    *param = param_name.to_string();
                }
            }
        }
        if !missing_required.is_empty() {
             return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: format!("Required parameters missing: {}", missing_required.join(", ")),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // Build AST Logic (shared with GET, adapted)
    let mut order_col_ast = table_schema.get.order_by.join(", ");
    let mut order_type_ast = "ASC".to_string();
    let mut paramjoins_ast: Vec<ParamJoin> = Vec::new();

    for p in &table_schema.get.parameters {
        let v_opt = if p.contains("paramjoin") {
            params_map.get(p).and_then(|vv| vv.as_str())
        } else { None };
        if let Some(v) = v_opt {
             paramjoins_ast.push(ParamJoin { name: p.replace(".eq", ""), value: v.to_string() });
        }
        if p.contains("deleted_at") && params_map.contains_key(p) {
             is_deleted_at = false;
        }
    }

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
                 "limit" | "page" => {}
                 p if p.contains("paramjoin") => {}
                 "search" => {
                      if !value_str.is_empty() {
                           let mut ors: Vec<QF> = Vec::new();
                           for column in table_schema.primary_key.columns.iter() {
                                let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                                ors.push(QF::ILike(col, format!("%{}%", value_str)));
                           }
                           for index in table_schema.indexes.iter() {
                                for column in index.columns.iter() {
                                     let col = if column.contains('.') { column.clone() } else { format!("{}.{}", table_schema.table, column) };
                                     ors.push(QF::ILike(col, format!("%{}%", value_str)));
                                }
                           }
                           if !ors.is_empty() { filters.push(QF::Or(ors)); }
                      }
                 }
                 p if p.contains('|') => {
                      let mut ors: Vec<QF> = Vec::new();
                      for part in p.split('|') {
                           let (column, operator, val) = split_column_operator(part, &table_schema.table, &value_str);
                           // Logic copied from export.rs
                           let f = build_filter_clause(&column, &operator, &val, &value_str);
                           ors.push(f);
                      }
                      if !ors.is_empty() { filters.push(QF::Or(ors)); }
                 }
                 _ => {
                      let (column, operator, val) = split_column_operator(param, &table_schema.table, &value_str);
                      if operator == "is" {
                           if value_str.eq_ignore_ascii_case("NULL") { filters.push(QF::IsNull(column)); }
                           else if value_str.eq_ignore_ascii_case("NOT NULL") { filters.push(QF::IsNotNull(column)); }
                           else { filters.push(QF::Eq(column, to_val(&val))); }
                      } else {
                           let f = build_filter_clause(&column, &operator, &val, &value_str);
                           filters.push(f);
                      }
                 }
             }
         }
    }

    if is_deleted_at {
         filters.push(QF::IsNull(format!("{}.deleted_at", table_schema.table)));
    }

    // Project, Join, Group, Limit
    let mut q = QQ::from(table_schema.table.clone()).select(table_schema.get.columns.clone());
    if !filters.is_empty() { q = q.r#where(QF::And(filters)); }

    // Order By
    apply_order_by(&mut q, &order_col_ast, &order_type_ast, table_schema);

    // JOINs
    if !table_schema.get.join_tables.is_empty() {
         for j in &table_schema.get.join_tables {
             let mut logical = j.logical.clone();
             for pj in &paramjoins_ast {
                 let safe_val: String = pj.value.chars().filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.').collect();
                 logical = logical.replace(&pj.name, &safe_val);
             }
             if j.type_join.eq_ignore_ascii_case("left") {
                 q = q.join_left_expr(j.table.clone(), QE::Raw(logical));
             } else {
                 q = q.join_inner_expr(j.table.clone(), QE::Raw(logical));
             }
         }
    }

     if !table_schema.get.column_groups.is_empty() {
         q = q.group_by(table_schema.get.column_groups.clone());
    }
    if !table_schema.get.having.is_empty() {
        let hv: Vec<QE> = table_schema.get.having.iter().cloned().map(QE::Raw).collect();
        q = q.having_expr(hv);
    }
    q = q.limit(i_limit as u32);

    // Execute
    let rows = match data_export_repo::execute_export_query(state, &q, route).await {
         Ok(r) => r,
         Err(e) => return HttpResponse::InternalServerError().json(WebResponse { success: false, message: e, total_data: 0, data: Value::Null })
    };

    // Transform rows
    let mut headers: Vec<String> = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();
    if let Some(obj) = rows.first().and_then(|f| f.as_object()) {
         headers = obj.keys().cloned().collect();
    }
     for row in rows.iter() {
        if let Some(obj) = row.as_object() {
            if headers.is_empty() { headers = obj.keys().cloned().collect(); }
            let mut line: Vec<String> = Vec::with_capacity(headers.len());
            for h in headers.iter() {
                let val = obj.get(h).unwrap_or(&Value::Null);
                line.push(match val {
                    Value::Null => String::new(),
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => if *b { "1".into() } else { "0".into() },
                    other => other.to_string().trim_matches('"').to_string(),
                });
            }
            data_rows.push(line);
        }
    }

    let ts = Local::now().format("%Y%m%d-%H%M%S");
     let (content_type, file_ext, bytes) = if export_type == "xlsx" {
        let buf = write_xlsx(&headers, &data_rows).unwrap_or_else(|e| {
             log_output("WARN", "EXPORT", route, format!("Falling back to CSV: {}", e), true);
             write_csv(&headers, &data_rows).unwrap_or_default()
        });
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(), "xlsx".to_string(), buf)
     } else {
         let buf = write_csv(&headers, &data_rows).unwrap_or_default();
         ("text/csv".to_string(), "csv".to_string(), buf)
     };

     // Audit
     write_audit(&AuditEntry {
        at: Local::now().to_rfc3339(),
        actor_id: actor_id_opt.unwrap_or_default(),
        action: "EXPORT",
        route,
        id: None,
        ip: Some(crate::helpers::get_client_ip(req)).as_deref(),
    });

    let filename = format!("{}-{}.{}", filename_base, ts, file_ext);
    let content_type_header = match header::HeaderValue::from_str(&content_type) {
        Ok(v) => v,
        Err(e) => {
            log_output("WARN", "EXPORT", route, format!("Invalid content-type header: {} ({})", content_type, e), true);
            header::HeaderValue::from_static("application/octet-stream")
        }
    };
    let disposition_value = format!("attachment; filename=\"{}\"", filename);
    let content_disp_header = match header::HeaderValue::from_str(&disposition_value) {
        Ok(v) => v,
        Err(e) => {
            log_output("WARN", "EXPORT", route, format!("Invalid content-disposition header: {} ({})", disposition_value, e), true);
            header::HeaderValue::from_static("attachment")
        }
    };

    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, content_type_header))
        .insert_header((header::CONTENT_DISPOSITION, content_disp_header))
        .body(bytes)

}

// Helper function to keep main logic cleaner
fn build_filter_clause(column: &str, operator: &str, val: &str, value_str: &str) -> QF {
      match operator {
        "=" => {
            let val_trim = value_str.trim();
            if val_trim.starts_with('[') && val_trim.ends_with(']') {
                if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                    let vs = arr.into_iter().map(|x| json_val_to_qv(&x)).collect();
                    QF::In(column.to_string(), vs)
                } else { QF::Eq(column.to_string(), to_val(val)) }
            } else if value_str.contains(',') {
                let vs = value_str.split(',').map(|s| to_val(s.trim())).collect();
                QF::In(column.to_string(), vs)
            } else { QF::Eq(column.to_string(), to_val(val)) }
        }
        "<" => QF::Lt(column.to_string(), to_val(val)),
        "<=" => QF::Lte(column.to_string(), to_val(val)),
        ">" => QF::Gt(column.to_string(), to_val(val)),
        ">=" => QF::Gte(column.to_string(), to_val(val)),
        "like" => QF::ILike(column.to_string(), val.to_string()),
        "nin" => {
             let val_trim = value_str.trim();
             if val_trim.starts_with('[') && val_trim.ends_with(']') {
                  if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                      let vs = arr.into_iter().map(|x| json_val_to_qv(&x)).collect();
                      QF::NotIn(column.to_string(), vs)
                  } else { QF::NotIn(column.to_string(), vec![to_val(val)]) }
             } else if value_str.contains(',') {
                  let vs = value_str.split(',').map(|s| to_val(s.trim())).collect();
                  QF::NotIn(column.to_string(), vs)
             } else { QF::NotIn(column.to_string(), vec![to_val(val)]) }
        }
        "between" => {
            let val_trim = value_str.trim();
             if val_trim.starts_with('[') && val_trim.ends_with(']') {
                 if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(val_trim) {
                     if arr.len() == 2 {
                         QF::Between(column.to_string(), json_val_to_qv(&arr[0]), json_val_to_qv(&arr[1]))
                     } else { QF::Eq(column.to_string(), to_val(val)) }
                 } else { QF::Eq(column.to_string(), to_val(val)) }
             } else if value_str.contains(',') {
                 let mut parts = value_str.split(',').map(|s| s.trim().to_string());
                 let a = parts.next().unwrap_or_default();
                 let b = parts.next().unwrap_or_default();
                 QF::Between(column.to_string(), to_val(&a), to_val(&b))
             } else { QF::Eq(column.to_string(), to_val(val)) }
        }
        "is" => {
             if val.eq_ignore_ascii_case("NULL") { QF::IsNull(column.to_string()) }
             else if val.eq_ignore_ascii_case("NOT NULL") { QF::IsNotNull(column.to_string()) }
             else { QF::Eq(column.to_string(), to_val(val)) }
        }
        _ => QF::Eq(column.to_string(), to_val(val)),
    }
}

fn json_val_to_qv(v: &serde_json::Value) -> QV {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { QV::I64(i) }
            else if let Some(f) = n.as_f64() { QV::F64(f) } else { QV::Str(n.to_string()) }
        }
        serde_json::Value::Bool(b) => QV::Bool(*b),
        serde_json::Value::String(s) => QV::Str(s.clone()),
        serde_json::Value::Null => QV::Null,
        other => QV::Str(other.to_string()),
    }
}


fn apply_order_by(q: &mut QQ, order_col_ast: &str, order_type_ast: &str, table_schema: &TableSchema) {
    let mut allowed_unqualified: HashSet<String> = HashSet::new();
    for c in table_schema.get.columns.iter() {
        let s = c.trim();
        if let Some((left, _right_alias_lc)) = s.to_lowercase().split_once(" as ") {
             if let Some((_l, alias_actual)) = s.split_once(" as ").or(s.split_once(" AS ")) {
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
        *q = q.clone().order_by(col_str.to_string(), asc);
        any_order = true;
    }
    if !any_order {
         for col in table_schema.get.order_by.iter() {
             let col_trim = col.trim();
             if col_trim.is_empty() { continue; }
             let unqualified = col_trim.rsplit('.').next().unwrap_or(col_trim);
             if allowed_unqualified.contains(unqualified) || allowed_unqualified.contains(col_trim) {
                 *q = q.clone().order_by(col_trim.to_string(), global_asc);
             }
         }
    }
}

pub fn write_csv(headers: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, anyhow::Error> {
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

pub fn write_xlsx(headers: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, anyhow::Error> {
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
