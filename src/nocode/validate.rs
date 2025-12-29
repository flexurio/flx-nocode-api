use actix_web::{web::Data, HttpResponse, Responder};
use serde_json::{json, Value};

use crate::{
    auth::{check_access, get_user_info_from_token},
    helpers::filter_table_schema,
    model::{
        Column, OperationGet, Index, JoinTable, OperationDelete, OperationPost, OperationPut, Patch,
        PrimaryKey, Redis, TableSchema, Trace, WebResponse,
    },
    AppState,
};
use std::sync::Arc;

// NCO-VALIDATE
pub async fn check_table_design(
    state: Data<AppState>,
    route: String,
    mut table_schemas: Arc<Vec<TableSchema>>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if !state.route_publics.contains(&route) {
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

        if !check_access(&claims, &route, "execute") {
            return HttpResponse::Unauthorized().json(WebResponse {
                success: false,
                message: "Unauthorized".to_string(),
                total_data: 0,
                data: Value::Null,
            });
        }
    }

    // get table schema from table_schemas where table = route
    let table_schema = filter_table_schema(&table_schemas, route.clone()).await;
    if table_schema.table.is_empty() {
        let message_error = format!("Entity {} on folder config/{}.json not found", route, route);
        return HttpResponse::FailedDependency().json(WebResponse {
            success: false,
            message: message_error,
            total_data: 0,
            data: Value::Null,
        });
    }

    table_schemas = Arc::new(vec![validate_table_design(table_schema.clone())]);

    HttpResponse::Ok().json(WebResponse {
        success: true,
        message: "Table validated".to_string(),
        total_data: 1,
        data: json!((*table_schemas).clone()),
    })
}

pub fn validate_table_design(design: TableSchema) -> TableSchema {
    let mut schema_check = TableSchema {
        table: String::new(),
        auto_generate: design.auto_generate,
        primary_key: PrimaryKey {
            columns: Vec::new(),
        },
        columns: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        redis: Redis {
            keys: Vec::new(),
            ttl: 0,
        },
        get: OperationGet {
            enable_method: false,
            columns: Vec::new(),
            parameters: Vec::new(),
            join_tables: Vec::new(),
            column_groups: Vec::new(),
            having: Vec::new(),
            order_by: Vec::new(),
        },
        post: OperationPost {
            enable_method: false,
            validate_data: String::new(),
            pre_process: String::new(),
            columns: Vec::new(),
            post_process: String::new(),
        },
        put: OperationPut {
            enable_method: false,
            validate_data: String::new(),
            pre_process: String::new(),
            columns: Vec::new(),
            post_process: String::new(),
        },
        del: OperationDelete {
            enable_method: false,
            pre_process: String::new(),
            columns: Vec::new(),
            type_delete: "soft".to_string(),
            post_process: String::new(),
        },
        patch: Patch {
            enable_method: false,
            pre_process_sp: String::new(),
            parameters: Vec::new(),
            return_mode: String::new(),
        },
        trace: Trace {
            enable_method: false,
            insert_into: String::new(),
            column_inserts: Vec::new(),
            column_selects: Vec::new(),
            parameters: Vec::new(),
            join_tables: Vec::new(),
            column_groups: Vec::new(),
            column_conflicts: Vec::new(),
        },
    };

    // Check if table exists
    schema_check.table = if design.table.is_empty() {
        "NOT OK - root.table does not exist".to_string()
    } else {
        "OK".to_string()
    };

    // Check primary key
    if design.primary_key.columns.is_empty() {
        schema_check.primary_key = PrimaryKey {
            columns: vec!["NOT OK - root.primary_key.columns does not exist".to_string()],
        };
    } else {
        schema_check.primary_key = PrimaryKey {
            columns: vec!["OK".to_string()],
        };

        for pk_col in &design.primary_key.columns {
            if !design.columns.iter().any(|col| col.name == *pk_col) {
                schema_check.primary_key.columns = vec![format!(
                    "NOT OK - primary key column '{}' does not exist in columns",
                    pk_col
                )];
            }
        }
    }

    // Check columns
    schema_check.columns = if design.columns.is_empty() {
        vec![Column {
            name: "NOT OK - root.columns.name do not exist".to_string(),
            type_data: "NOT OK - root.columns.type do not exist".to_string(),
            auto_increment: false,
            nullable: false,
            function: "NOT OK - root.columns.function do not exist".to_string(),
            encrypt: false,
            collate: "NOT OK - root.columns.collate do not exist".to_string(),
        }]
    } else {
        vec![Column {
            name: "OK".to_string(),
            type_data: "OK".to_string(),
            auto_increment: false,
            nullable: false,
            function: "OK".to_string(),
            encrypt: false,
            collate: "OK".to_string(),
        }]
    };

    // Check indexes
    schema_check.indexes = if design.indexes.is_empty() {
        vec![Index {
            name: "NOT OK - root.indexes do not exist".to_string(),
            columns: vec!["NOT OK - root.indexes.columns do not exist".to_string()],
            unique: false,
        }]
    } else {
        vec![Index {
            name: "OK".to_string(),
            columns: vec!["OK".to_string()],
            unique: true,
        }]
    };

    for index in &design.indexes {
        for index_col in &index.columns {
            if !design.columns.iter().any(|col| col.name == *index_col) {
                schema_check.indexes = vec![Index {
                    name: format!(
                        "NOT OK - index column '{}' does not exist in columns",
                        index_col
                    ),
                    columns: vec![format!(
                        "NOT OK - index column '{}' does not exist in columns",
                        index_col
                    )],
                    unique: false,
                }];
            }

            if design.primary_key.columns.contains(index_col) {
                schema_check.indexes = vec![Index {
                    name: format!(
                        "NOT OK - primary key column '{}' should not be indexed",
                        index_col
                    ),
                    columns: vec![format!(
                        "NOT OK - primary key column '{}' should not be indexed",
                        index_col
                    )],
                    unique: false,
                }];
            }
        }
    }

    // Check GET
    let is_get_columns_exist = !design.get.columns.is_empty();
    schema_check.get.columns = if design.get.columns.is_empty() {
        vec!["NOT OK - root.GET.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    let ok_or_optional = if is_get_columns_exist {
        "NOT OK"
    } else {
        "OPTIONAL"
    };

    // Check GET parameters
    let required_params = ["search", "page", "sort", "ascending", "limit"];
    let has_required_params = required_params
        .iter()
        .all(|p| design.get.parameters.contains(&p.to_string()));

    if !has_required_params {
        schema_check.get.parameters = vec![format!(
            "{} - root.GET.parameters must contain search,page,sort,ascending,limit",
            ok_or_optional
        )];
    } else if design.get.parameters.is_empty() {
        schema_check.get.parameters = vec![format!(
            "{} - root.GET.parameters do not exist",
            ok_or_optional
        )];
    } else {
        let mut column_problems = Vec::new();
        for param in &design.get.parameters {
            if !required_params.contains(&param.as_str()) && !param.contains("deleted_at") {
                let parts: Vec<&str> = param.split('.').collect();
                let (table, param_name) = if parts.len() >= 2 {
                    (parts[0], parts[parts.len() - 2])
                } else {
                    (design.table.as_str(), parts[0])
                };

                let is_col_ok = if table == design.table {
                    design.columns.iter().any(|col| col.name == param_name)
                } else {
                    design
                        .get
                        .join_tables
                        .iter()
                        .filter(|jt| jt.table == table)
                        .any(|jt| jt.columns.contains(&param_name.to_string()))
                };

                if !is_col_ok {
                    column_problems.push(param.clone());
                } else if !design.primary_key.columns.contains(&param_name.to_string()) {
                    let in_index = design
                        .indexes
                        .iter()
                        .any(|idx| idx.columns.contains(&param_name.to_string()));
                    if !in_index {
                        column_problems.push(param.clone());
                    }
                }
            }
        }

        schema_check.get.parameters = if !column_problems.is_empty() {
            vec![format!(
                "{} - root.GET.parameters must exist in columns, indexes, and primary key. Check: {}",
                ok_or_optional,
                column_problems.join(", ")
            )]
        } else {
            vec!["OK".to_string()]
        };
    }

    // Check join tables
    schema_check.get.join_tables = if design.get.join_tables.is_empty() {
        vec![JoinTable {
            table: "OPTIONAL - root.GET.join_tables.table do not exist".to_string(),
            columns: vec!["OPTIONAL - root.GET.join_tables.columns do not exist".to_string()],
            logical: "OPTIONAL - root.GET.join_tables.logical do not exist".to_string(),
            type_join: "OPTIONAL - root.GET.join_tables.type do not exist".to_string(),
        }]
    } else {
        vec![JoinTable {
            table: "OK".to_string(),
            columns: vec!["OK".to_string()],
            logical: "OK".to_string(),
            type_join: "OK".to_string(),
        }]
    };

    // Check group by
    schema_check.get.column_groups = if design.get.column_groups.is_empty() {
        vec!["OPTIONAL - root.GET.group_by do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Check POST
    schema_check.post.columns = if design.post.columns.is_empty() {
        vec!["OPTIONAL - root.POST.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Check PUT
    schema_check.put.columns = if design.put.columns.is_empty() {
        vec!["OPTIONAL - root.PUT.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Check DELETE
    schema_check.del.columns = if design.del.columns.is_empty() {
        vec!["OPTIONAL - root.DELETE.columns do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    // Note: The Rust struct doesn't fully match the Go version for PATCH,
    // so I've adapted it based on what's available in the provided Rust structs
    schema_check.patch.parameters = if design.patch.parameters.is_empty() {
        vec!["OPTIONAL - root.PATCH.parameters do not exist".to_string()]
    } else {
        vec!["OK".to_string()]
    };

    schema_check
}
