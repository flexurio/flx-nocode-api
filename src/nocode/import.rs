// Clean sonic_rs import implementation
use actix_multipart::Multipart;
use actix_web::{web::Data, HttpResponse, Responder};
use futures::StreamExt;
use chrono::Local;
use sonic_rs::{json, Value, JsonValueTrait};
use std::io::Cursor;
use std::sync::Arc;
use crate::audit::{write_audit, AuditEntry};
use crate::helpers::get_client_ip;
use crate::rate_limit::RL_WINDOW_MUTATE;
use crate::{
    auth::{check_access, get_user_info_from_token, Claims},
    crypt::{encrypt, is_encrypted_string},
    helpers::filter_table_schema,
    log::log_output,
    model::{ReferenceForeignKey, TableSchema, WebResponse},
    nocode::foreign_key::check_data_foreign_key,
    AppState,
};
use crate::storage::ast::{Query as Q, Filter as F};

type Row = sonic_rs::Object;

pub async fn import(
    state: Data<AppState>,
    route: String,
    schemas: Arc<(Vec<TableSchema>, Vec<ReferenceForeignKey>)>,
    mut multipart: Multipart,
    req: actix_web::HttpRequest,
) -> impl Responder { 
    let table_schemas=&schemas.0; let mut claims=Claims::default(); if !state.route_publics.contains(&route){ match get_user_info_from_token(req.clone(), state.clone()){ Ok(c)=>claims=c, Err(_)=>return HttpResponse::Unauthorized().json(WebResponse{success:false,message:crate::constants::ERR_INVALID_TOKEN.into(),total_data:0,data:Value::default()}) } if !check_access(&claims,&route,"write"){ return HttpResponse::Unauthorized().json(WebResponse{success:false,message:crate::constants::ERR_UNAUTHORIZED.into(),total_data:0,data:Value::default()}); } }
    if let Some(limit) = std::env::var("RATE_LIMIT_MUTATE_PER_SEC").ok().and_then(|v| v.parse::<u32>().ok()) {
        if limit > 0 {
            let key = format!("import:{}:{}", route, get_client_ip(&req));
            if !RL_WINDOW_MUTATE.check_and_increment(&key, limit) {
                return HttpResponse::TooManyRequests().json(WebResponse { success:false,message:"Too many requests".into(),total_data:0,data:Value::default()});
            }
        }
    }
    let table_schema=filter_table_schema(table_schemas,route.clone()).await; if table_schema.table.is_empty(){ return HttpResponse::FailedDependency().json(WebResponse{success:false,message:format!("Entity {} on folder config/{}.json not found",route,route),total_data:0,data:Value::default()}); }
    let max_file_size=std::env::var("UPLOAD_LIMIT_MB").ok().and_then(|v|v.parse::<usize>().ok()).unwrap_or(10)*1024*1024; let mut file_bytes=None; let mut filename=String::new(); let mut declared_mime=None; let mut additional=sonic_rs::Object::new();
    while let Some(item)=multipart.next().await { let mut field=match item{Ok(f)=>f,Err(e)=>return HttpResponse::BadRequest().json(WebResponse{success:false,message:format!("Multipart error: {}",e),total_data:0,data:Value::default()})}; let cd=field.content_disposition().cloned(); let name=cd.as_ref().and_then(|c|c.get_name()).unwrap_or(""); if name=="file" { if let Some(fname)=cd.as_ref().and_then(|c|c.get_filename()){filename=fname.to_string();} declared_mime=field.content_type().map(|t|t.to_string()); let mut buf=Vec::new(); let mut total=0usize; while let Some(chunk)=field.next().await { let data=match chunk{Ok(c)=>c,Err(e)=>return HttpResponse::BadRequest().json(WebResponse{success:false,message:format!("Read chunk error: {}",e),total_data:0,data:Value::default()})}; total+=data.len(); if total>max_file_size { return HttpResponse::PayloadTooLarge().json(WebResponse{success:false,message:"File too large".into(),total_data:0,data:Value::default()}); } buf.extend_from_slice(&data);} file_bytes=Some(buf);} else if !name.is_empty(){ let mut val=String::new(); while let Some(chunk)=field.next().await { let data=match chunk{Ok(c)=>c,Err(e)=>return HttpResponse::BadRequest().json(WebResponse{success:false,message:format!("Read field '{}' error: {}",name,e),total_data:0,data:Value::default()})}; val.push_str(&String::from_utf8_lossy(&data)); } additional.insert(name,json!(val)); } }
    let file_bytes=match file_bytes {Some(b)=>b,None=>return HttpResponse::BadRequest().json(WebResponse{success:false,message:"No file provided (field name 'file')".into(),total_data:0,data:Value::default()})};
    let lower=filename.to_lowercase(); let is_csv=lower.ends_with(".csv"); let is_xlsx=lower.ends_with(".xlsx"); let sniff=infer::get(&file_bytes); let mime=sniff.map(|k|k.mime_type().to_string()); let is_xlsx_mime=matches!(mime.as_deref(),Some("application/zip")) && (is_xlsx || matches!(declared_mime.as_deref(),Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))); let is_csv_mime=matches!(declared_mime.as_deref(),Some("text/csv"))||is_csv; let rows:Vec<Row>= if is_xlsx_mime||is_xlsx { match parse_xlsx(file_bytes){Ok(v)=>v,Err(e)=>return HttpResponse::BadRequest().json(WebResponse{success:false,message:format!("Failed to read XLSX: {}",e),total_data:0,data:Value::default()})} } else if is_csv_mime||is_csv { match parse_csv(file_bytes){Ok(v)=>v,Err(e)=>return HttpResponse::BadRequest().json(WebResponse{success:false,message:format!("Failed to read CSV: {}",e),total_data:0,data:Value::default()})} } else { return HttpResponse::BadRequest().json(WebResponse{success:false,message:"Unsupported file type (expect .csv or .xlsx)".into(),total_data:0,data:Value::default()}); }; if rows.is_empty(){ return HttpResponse::BadRequest().json(WebResponse{success:false,message:"No data rows found".into(),total_data:0,data:Value::default()}); }
    if !additional.is_empty(){ log_output("INFO","IMPORT",&table_schema.table,format!("Additional columns: {:?}",additional),true); }
    let mut cols: Vec<&crate::model::Column> = table_schema
        .columns
        .iter()
        .filter(|c| !c.auto_increment)
        .collect();
    let skip: [&str; 6] = [
        "created_at",
        "updated_at",
        "deleted_at",
        "created_by_id",
        "updated_by_id",
        "deleted_by_id",
    ];
    cols.retain(|c| {
        let name_ref: &str = c.name.as_str();
        !skip.contains(&name_ref)
    });
    let has_id = cols.iter().any(|c| c.name == "id");
    let id_func = table_schema
        .columns
        .iter()
        .find(|c| c.name == "id")
        .map(|c| c.function.clone())
        .filter(|s| !s.is_empty());
    let id_key = String::from("id");
    let all_have_id = rows.iter().all(|r| r.get(&id_key).is_some());
    let include_id = has_id && (id_func.is_some() || all_have_id);
    if !include_id { cols.retain(|c| c.name != "id"); }
    let mut id_ctx:Option<(String,usize,i64)>=None; if include_id { if let Some(f)=id_func.as_ref(){ let (prefix,width)=derive_id_prefix_and_width(f); if state.db_type=="mongodb" { use crate::storage::ast::Query as QQ; let q=QQ::from(table_schema.table.clone()).agg_max("max_id","id").r#where(F::ILike("id".into(),format!("{}%",prefix))).limit(1); let max_id:String=match state.store.query(&q).await { Ok(r) if !r.is_empty()=>r[0].get("max_id").and_then(|v|v.as_str()).unwrap_or("0").to_string(), _=>"0".into() }; let last=max_id.rsplit('/').next().unwrap_or("0"); let next=last.trim_start_matches('0').parse().unwrap_or(0); id_ctx=Some((prefix,width,next)); } else { let q=Q::from(table_schema.table.clone()).select(["COALESCE(MAX(id), 0) as max_id"]).r#where(F::Like("id".into(),format!("%{}%",prefix))); let max_id:String=match state.store.query(&q).await { Ok(r) if !r.is_empty()=>{ let v=r[0].get("max_id"); if let Some(s)=v.and_then(|x|x.as_str()){s.to_string()} else if let Some(n)=v.and_then(|x|x.as_i64()){n.to_string()} else if let Some(fl)=v.and_then(|x|x.as_f64()){fl.to_string()} else {"0".into()} }, _=>"0".into() }; let last=max_id.rsplit('/').next().unwrap_or("0"); let next=last.trim_start_matches('0').parse().unwrap_or(0); id_ctx=Some((prefix,width,next)); } } }
    let created_by_type=table_schema.columns.iter().find(|c|c.name=="created_by_id").map(|c|c.type_data.clone()).unwrap_or("int".into()); let mut inserted=0i32; let now=Local::now().to_rfc3339();
    for (i, row) in rows.iter().enumerate() {
        let mut doc = sonic_rs::Object::new();
        for col in cols.iter() {
            let mut v = if let Some(add) = additional.get(&col.name) {
                value_to_string(add)
            } else {
                row.get(&col.name).map(value_to_string).unwrap_or_default()
            };

            if col.name == "id" && v.is_empty() {
                if let Some((ref prefix, width, ref mut next)) = id_ctx {
                    *next += 1;
                    let num = format!("{:0>len$}", *next, len = width);
                    v = format!("{}/{}", prefix, num);
                } else if !all_have_id {
                    return HttpResponse::BadRequest().json(WebResponse { success:false,message:format!("Row {} missing id and no function configured", inserted as usize + i + 1), total_data:inserted, data:Value::default() });
                }
            }

            if !v.is_empty() {
                for fk in table_schema.foreign_keys.iter() {
                    if fk.column == col.name {
                        let ok = check_data_foreign_key(&state, fk.reference_table.clone(), fk.reference_column.clone(), v.clone()).await;
                        if !ok {
                            return HttpResponse::BadRequest().json(WebResponse { success:false,message:format!("Invalid foreign key '{}' for column '{}' at row {}", v, col.name, inserted as usize + i + 1), total_data:inserted, data:Value::default() });
                        }
                    }
                }
            }

            if col.encrypt && !v.is_empty()
                && !is_encrypted_string(&v) {
                    v = encrypt(state.encrypt_key.clone(), v);
                }

            let json_val = if v.is_empty() && col.nullable {
                Value::default()
            } else if (col.type_data.contains("int")
                || col.type_data.contains("float")
                || col.type_data.contains("double")
                || col.type_data.contains("decimal")
                || col.type_data.contains("money")) && !v.is_empty() {
                if let Ok(n) = v.parse::<i64>() { json!(n) }
                else if let Ok(f) = v.parse::<f64>() { json!(f) }
                else { json!(v) }
            } else {
                json!(v)
            };
            doc.insert(col.name.as_str(), json_val);
        }

        doc.insert("created_at", json!(now.clone()));
        if created_by_type.contains("int") {
            if let Ok(n)=claims.id.parse::<i64>() { doc.insert("created_by_id", json!(n)); } else { doc.insert("created_by_id", json!(claims.id.clone())); }
        } else if created_by_type.contains("float") || created_by_type.contains("double") || created_by_type.contains("decimal") || created_by_type.contains("money") {
            if let Ok(fv)=claims.id.parse::<f64>() { doc.insert("created_by_id", json!(fv)); } else { doc.insert("created_by_id", json!(claims.id.clone())); }
        } else {
            doc.insert("created_by_id", json!(claims.id.clone()));
        }

        if let Err(e) = state.store.insert(&table_schema.table, Value::from(doc)).await {
            return HttpResponse::BadRequest().json(WebResponse { success:false,message:format!("Insert error: {} at row {}", e, inserted as usize + i + 1), total_data:inserted, data:Value::default() });
        }
        inserted += 1;
    }
    write_audit(&AuditEntry{at:Local::now().to_rfc3339(),actor_id:claims.id.clone(),action:"IMPORT",route:&route,id:None,ip:Some(get_client_ip(&req)).as_deref(),});
    HttpResponse::Ok().json(WebResponse{success:true,message:format!("Imported {} rows",inserted),total_data:inserted,data:Value::default()}) }

fn derive_id_prefix_and_width(function:&str)->(String,usize){ let mut prefix=String::new(); let mut width=0usize; for part in function.split('/') { match part { "%Y"=>{prefix.push('/');prefix.push_str(&chrono::Utc::now().format("%Y").to_string());}, "%m"=>{prefix.push('/');prefix.push_str(&chrono::Utc::now().format("%m").to_string());}, "%d"=>{prefix.push('/');prefix.push_str(&chrono::Utc::now().format("%d").to_string());}, p if p.contains("ID")=>{ width=p.replace("ID","").len(); }, other=>{ if !other.is_empty(){ prefix.push('/');prefix.push_str(other);} } } } if !prefix.is_empty(){ prefix.remove(0);} (prefix,width) }
fn value_to_string(v:&Value)->String{
    if v.is_null(){ return String::new(); }
    if let Some(s)=v.as_str(){ return s.to_string(); }
    if let Some(i)=v.as_i64(){ return i.to_string(); }
    if let Some(f)=v.as_f64(){ return f.to_string(); }
    if let Some(b)=v.as_bool(){ return if b {"1".into()} else {"0".into()}; }
    v.to_string().trim_matches('"').to_string()
}
fn parse_csv(bytes:Vec<u8>)->anyhow::Result<Vec<Row>>{ let mut rdr=csv::ReaderBuilder::new().has_headers(true).flexible(true).from_reader(Cursor::new(bytes)); let headers=rdr.headers()?.iter().map(|s|s.trim().to_string()).collect::<Vec<_>>(); let mut rows:Vec<Row>=Vec::new(); for rec in rdr.records(){ let rec=rec?; let mut obj=sonic_rs::Object::new(); for (i,val) in rec.iter().enumerate(){ if let Some(h)=headers.get(i){ if !h.is_empty(){ obj.insert(h.as_str(), json!(val.trim())); } } } if !obj.is_empty(){ rows.push(obj);} } Ok(rows) }
fn parse_xlsx(bytes:Vec<u8>)->anyhow::Result<Vec<Row>>{ use calamine::{Reader,Xlsx}; let mut workbook:Xlsx<Cursor<Vec<u8>>>=Xlsx::new(Cursor::new(bytes))?; let range=workbook.worksheet_range_at(0).ok_or_else(||anyhow::anyhow!("No worksheet"))??; let mut rows:Vec<Row>=Vec::new(); let mut headers:Vec<String>=Vec::new(); for (r_idx,r) in range.rows().enumerate(){ if r_idx==0 { headers=r.iter().map(|c|c.to_string()).map(|s|s.trim().trim_matches('"').to_string()).collect(); continue;} let mut obj=sonic_rs::Object::new(); for (i,cell) in r.iter().enumerate(){ if let Some(h)=headers.get(i){ if !h.is_empty(){ let text=cell.to_string(); if !text.trim().is_empty(){ obj.insert(h.as_str(), json!(text.trim())); } } } } if !obj.is_empty(){ rows.push(obj);} } Ok(rows) }
