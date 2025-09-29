use actix_web::{
    web::{self},
    HttpResponse, Responder,
};
use serde_json::Value;

use crate::{
    auth::{check_access, get_user_info_from_token},
    database::state::DbParam,
    helpers::{filter_table_schema, split_column_operator, get_client_ip},
    log::log_output,
    model::{ParamJoin, TableSchema, WebResponse},
    rate_limit::RL_WINDOW_GET,
    AppState,
};
use std::sync::Arc;

// NCO-GET
pub async fn select(
    state: web::Data<AppState>,
    parameters: web::Query<Value>,
    route: String,
    table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    // Per-IP GET rate limit per second
    let ip_key = get_client_ip(&req);
    let get_limit: u32 = std::env::var("RATE_LIMIT_GET_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    if !RL_WINDOW_GET.check_and_increment(&format!("get:{}:{}", route, ip_key), get_limit) {
        return HttpResponse::TooManyRequests().json(WebResponse {
            success: false,
            message: "Too many requests".to_string(),
            total_data: 0,
            data: Value::Null,
        });
    }
    if !state.route_publics.contains(&route) {
        println!("Route: {}", route);
        
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

        println!("Claims: {:?}", claims);

        if !check_access(&claims, &route, "read") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }

        // Per-user GET rate limit per second
        if !claims.id.is_empty()
            && !RL_WINDOW_GET
                .check_and_increment(&format!("get:{}:user:{}", route, claims.id), get_limit)
        {
            return HttpResponse::TooManyRequests().json(WebResponse {
                success: false,
                message: "Too many requests".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    let table_schema: TableSchema = filter_table_schema(&table_schemas, route.clone()).await;
    let mut where_clause: String = "WHERE ".to_string();
    let mut limit_clause: String = String::new();
    let mut i_limit = 100;
    let mut pagination_clause = String::new();
    let mut i_page = 1;
    let mut order_clause: String = "ORDER BY ".to_string();
    let mut order_column = table_schema.get.order_by.clone().join(", ");
    let mut order_type = "ASC".to_string();
    let mut group_clause: String = "GROUP BY ".to_string();
    let mut having_clause: String = "HAVING ".to_string();
    let mut paramjoins: Vec<ParamJoin> = Vec::new();
    let mut bind_params: Vec<DbParam> = Vec::new();

    log_output(
        "CONFIGURATION",
        "FILTERED PARAMETERS",
        "filter_table_schema",
        serde_json::to_string(&table_schema.get.parameters)
            .unwrap_or_else(|_| "Failed to serialize TableSchema".to_string()),
        true,
    );

    if table_schema.table.is_empty() {
        let message_error = format!(
            "ER01(nocode_get): Entity {} on folder config/{}.json not found",
            route, route
        );
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    let mut is_deleted_at = true;

    log_output(
        "CONFIGURATION",
        "PARAMETERS ON ROUTES",
        "TableSchema",
        table_schema.get.parameters.join(", "),
        true,
    );

    // get parameters value only allowed from table_schemas.get.parameters
    // Pre-convert to object once for efficiency
    let params_obj = parameters.clone().into_inner();
    let params_map = match params_obj.as_object() {
        Some(map) => map,
        None => {
            return HttpResponse::BadRequest().json(WebResponse {
                success: false,
                message: "Invalid parameters format".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    };

    for param in &table_schema.get.parameters {
        for (key, value) in params_map {
            if key.contains("deleted_at") {
                is_deleted_at = false;
            }

            // check if parameters contains key from table_schemas.get.parameters
            if param == key {
                // check if in PARAMS_PAGINATION then add to pagination_data
                if param == "page" {
                    i_page = value
                        .as_str()
                        .and_then(|s| s.parse::<i32>().ok())
                        .filter(|v| *v > 0)
                        .unwrap_or(1);
                } else if param == "sort" {
                    if value != "" {
                        let mut val = value.to_string();
                        val = val
                            .chars()
                            .filter(|c| c.is_alphanumeric() || *c == '.' || *c == ',' || *c == ' ')
                            .collect();
                        order_column = val;
                    }
                } else if param == "ascending" {
                    let v = value.as_str().unwrap_or("");
                    order_type = if v.eq_ignore_ascii_case("true") {
                        "ASC".into()
                    } else {
                        "DESC".into()
                    };
                } else if param == "limit" {
                    i_limit = value
                        .as_str()
                        .and_then(|s| s.parse::<i32>().ok())
                        .map(|v| v.clamp(1, 1000))
                        .unwrap_or(100);
                } else if param == "redis" {
                    // check redis
                    println!("Redis: {}", value);
                } else if param == "search" {
                    let value_str = value.as_str().unwrap_or("").to_string();
                    let mut search_clause = "( ".to_string();

                    for column in table_schema.primary_key.columns.iter() {
                        if column.contains(".") {
                            search_clause.push_str(&format!("{} LIKE ? OR ", column));
                            bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                        } else {
                            search_clause
                                .push_str(&format!("{}.{} LIKE ? OR ", table_schema.table, column));
                            bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                        }
                    }

                    //  get column frim table_schema.index.columns
                    for index in table_schema.indexes.iter() {
                        for column in index.columns.iter() {
                            if column.contains(".") {
                                search_clause.push_str(&format!("{} LIKE ? OR ", column));
                                bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                            } else {
                                search_clause.push_str(&format!(
                                    "{}.{} LIKE ? OR ",
                                    table_schema.table, column
                                ));
                                bind_params.push(DbParam::Str(format!("%{}%", value_str)));
                            }
                        }
                    }

                    search_clause = search_clause[..search_clause.len() - 4].to_string();
                    search_clause.push_str(" )");
                    where_clause.push_str(&format!("{} AND ", search_clause));
                } else if param.contains("|") {
                    where_clause.push_str(" ( ");
                    let param_split: Vec<&str> = param.split("|").collect();

                    // loop every param_split
                    for (idx, param) in param_split.iter().enumerate() {
                        let value_str = value.as_str().unwrap_or("").to_string();
                        let (column, operator, value) =
                            split_column_operator(param, &table_schema.table, &value_str);

                        if idx == 0 {
                            where_clause.push_str(&format!("{} {} ? ", column, operator));
                            bind_params.push(DbParam::Str(value));
                        } else {
                            where_clause.push_str(&format!("OR {} {} ? ", column, operator));
                            bind_params.push(DbParam::Str(value));
                        }
                    }

                    where_clause.push_str(" ) AND ");
                } else if param.contains("paramjoin") {
                    // add to paramjoins
                    paramjoins.push(ParamJoin {
                        name: param.to_string().replace(".eq", ""),
                        value: value.as_str().unwrap_or("").to_string(),
                    });
                } else {
                    let value_str = value.as_str().unwrap_or("").to_string();
                    let (column, operator, value) =
                        split_column_operator(param, &table_schema.table, &value_str);

                    if value_str.eq_ignore_ascii_case("NULL") {
                        // special-case IS NULL / IS NOT NULL out of split_column_operator
                        where_clause.push_str(&format!("{} {} NULL AND ", column, operator));
                    } else if value.parse::<i64>().is_ok() {
                        where_clause.push_str(&format!("{} {} ? AND ", column, operator));
                        bind_params.push(DbParam::I64(value.parse().unwrap_or(0)));
                    } else if value.parse::<f64>().is_ok() {
                        where_clause.push_str(&format!("{} {} ? AND ", column, operator));
                        bind_params.push(DbParam::F64(value.parse().unwrap_or(0.0)));
                    } else {
                        where_clause.push_str(&format!("{} {} ? AND ", column, operator));
                        bind_params.push(DbParam::Str(value));
                    }
                }
            }
        }
    }

    // check group by
    for group in table_schema.get.column_groups.iter() {
        group_clause.push_str(&format!("{}, ", group));
    }
    if group_clause.len() > 10 {
        // jika group_clause lebih dari 10, maka hapus ", "
        group_clause = group_clause[..group_clause.len() - 2].to_string();
    } else {
        group_clause = "".to_string();
    }

    // check having
    for having in table_schema.get.having.iter() {
        // only allow literals or schema-defined expressions; do not inject user values here
        having_clause.push_str(&format!("{}, ", having));
    }
    if having_clause.len() > 7 {
        // jika having_clause lebih dari 10, maka hapus ", "
        having_clause = having_clause[..having_clause.len() - 2].to_string();
    } else {
        having_clause = "".to_string();
    }

    // check order by
    if order_column.is_empty() {
        order_clause = "".to_string();
    } else {
        // Whitelist order columns against schema columns/indexes
        let allowed_cols: Vec<&str> = table_schema
            .get
            .columns
            .iter()
            .map(|s| s.as_str())
            .chain(
                table_schema
                    .indexes
                    .iter()
                    .flat_map(|idx| idx.columns.iter().map(|s| s.as_str())),
            )
            .collect();
        let sanitized: String = order_column
            .split(',')
            .map(|c| c.trim())
            .filter(|c| allowed_cols.contains(c))
            .collect::<Vec<&str>>()
            .join(", ");

        if sanitized.is_empty() {
            order_clause = "".to_string();
        } else {
            order_clause.push_str(&format!("{} {} ", sanitized, order_type));
        }
    }

    // DB-specific pagination
    // For MySQL/Postgres/SQLite -> LIMIT <n> OFFSET <m>
    // For MSSQL -> ORDER BY ... OFFSET <m> ROWS FETCH NEXT <n> ROWS ONLY

    // check page
    let offset = (i_page - 1) * i_limit;
    match state.db_type.as_str() {
        "mssql" => {
            // Ensure ORDER BY exists; MSSQL requires ORDER BY for OFFSET/FETCH
            if order_clause.is_empty() {
                let fallback_col = if !table_schema.primary_key.columns.is_empty() {
                    table_schema.primary_key.columns[0].clone()
                } else {
                    // last resort fallback
                    "id".to_string()
                };
                order_clause = format!("ORDER BY {} ASC ", fallback_col);
            }
            pagination_clause.push_str(&format!(
                "OFFSET {} ROWS FETCH NEXT {} ROWS ONLY ",
                offset, i_limit
            ));
            limit_clause.clear();
        }
        _ => {
            // default SQL dialects
            limit_clause = format!("LIMIT {} ", i_limit);
            pagination_clause.push_str(&format!("OFFSET {} ", offset));
        }
    }

    // jika gak ada deleted_at di where_clause, maka tambahkan deleted_at IS NULL
    if is_deleted_at {
        where_clause.push_str(format!("{}.deleted_at IS NULL AND ", route).as_str());
    }

    // remove last " AND " from where_clause
    if where_clause.len() > 6 {
        where_clause = where_clause[..where_clause.len() - 5].to_string();
    } else {
        where_clause = "".to_string();
    }

    let select_columns = table_schema.get.columns.join(", ");

    let joins: Vec<String> = table_schema
        .get
        .join_tables
        .iter()
        .map(|join| {
            // loop every paramjoins and replace join.logical string parameter with value
            let mut logical = join.logical.clone();
            for paramjoin in paramjoins.iter() {
                // Safe replacement limited to identifiers/known macros only; do not inject values containing SQL
                let safe_val: String = paramjoin
                    .value
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                    .collect();
                logical = logical.replace(&paramjoin.name, &safe_val);
            }
            format!(
                "{} JOIN {} ON {}",
                join.type_join.to_uppercase(),
                join.table,
                logical
            )
        })
        .collect();

    let join_clause = if joins.is_empty() {
        "".to_string()
    } else {
        format!(" {}", joins.join(" "))
    };
    let s_sql = format!(
        "SELECT {} FROM {} {} {} {} {} {} {} {} ",
        select_columns,
        table_schema.table,
        join_clause,
        where_clause,
        group_clause,
        having_clause,
        order_clause,
        limit_clause,
        pagination_clause
    );

    log_output("QUERY", "GET", route.as_str(), s_sql.clone(), true);
    log_output(
        "PARAMS",
        "POST",
        route.as_str(),
        format!("{:?}", bind_params),
        true,
    );

    // Build total count query:
    // - Without GROUP BY: simple COUNT(*) over filtered set
    // - With GROUP BY: count number of groups using a subquery, include HAVING
    let s_sql_total = if group_clause.is_empty() && having_clause.is_empty() {
        format!(
            "SELECT COUNT(*) as total_data FROM {} {} {}",
            table_schema.table, join_clause, where_clause
        )
    } else {
        format!(
            "SELECT COUNT(*) as total_data FROM (SELECT 1 FROM {} {} {} {} {}) AS _cnt",
            table_schema.table, join_clause, where_clause, group_clause, having_clause
        )
    };

    // get total data from
    let total_data: i32 = state
        .db
        .get_total_rows_with_params(&s_sql_total, bind_params.clone())
        .await
        .unwrap_or(0);
    let query_result = state.db.query_with_params(&s_sql, bind_params).await;
    match query_result {
        Ok(res) => {
            let result = WebResponse {
                success: true,
                message: "Data found".to_string(),
                total_data,
                data: Value::Array(res),
            };

            HttpResponse::Ok().json(result)
        }
        Err(e) => {
            let res = WebResponse {
                success: false,
                message: format!("Error NCO-GET: {}", e),
                total_data: 0,
                data: Value::Null,
            };
            HttpResponse::InternalServerError().json(res)
        }
    }
}
