use std::sync::Arc;
use serde_json::{Value};

use crate::{helpers::{extract_expressions}, log::log_output};

pub struct QueryConvertor {
    pub datetime_now: String,    
}

pub struct AppState {
    pub db: Arc<dyn DbRepository>,
    pub db_type: String,
    pub secret: String,
    pub encrypt_key: String,
    pub query_convertor: QueryConvertor,
    pub whitelist_ips: Vec<String>,
    pub route_publics: Vec<String>,
}


#[async_trait::async_trait]
pub trait DbRepository: Send + Sync {
    async fn query(&self, sql: &str) -> Result<Vec<Value>, anyhow::Error>;
    async fn get_total_rows(&self, sql: &str) -> Result<i32, anyhow::Error>;
}


pub async fn execute_sql_formula(db: &Arc<dyn DbRepository>, sql: String, body: &serde_json::Value, route: &str) {
    let mut sql = sql.replace("SQL:", "");
    sql = formula_replace(sql, body);
    log_output("QUERY", "POST", route, sql.to_string(), true);
    // Execute the SQL query
    match db.query(&sql).await {
        Ok(_) => {
            println!("AFTER POST SQL query executed successfully");
        },
        Err(err) => {
            println!("Error executing SQL query: {}", err);
        }
    }
}


pub fn concat_column_values(values: Vec<Value>, column_name: &str, separator: &str) -> String {
    let mut result = Vec::new();

    for value in values {
        if let Value::Object(obj) = value {
            if let Some(v) = obj.get(column_name) {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "".to_string(),
                    _ => "".to_string(),
                };
                result.push(s);
            }
        }
    }

    result.join(separator)
}



pub fn sanitize_sql_input(input: String) -> String {
    input
        .replace("'", "`")       // escape single quotes (SQL standard)
        .replace("--", "")        // remove SQL comment syntax
        .replace(";", "")         // prevent query stacking
        .replace("\"", "")        // remove double quotes
        .replace("\\", "")        // prevent backslash escape (esp. in MySQL)
        .replace("/*", "")        // remove block comment start
        .replace("*/", "")        // remove block comment end
        .replace("#", "")         // MySQL comment
        .replace("`", "")         // MySQL identifier escape
        .replace(" OR ", " ")     // remove logic operators
        .replace(" or ", " ")
        .replace(" AND ", " ")
        .replace(" and ", " ")
        .replace("=", "")         // remove equal signs
        .replace("(", "")         // remove open parenthesis
        .replace(")", "")         // remove close parenthesis
        .replace("%", "")         // remove wildcards in LIKE
        .replace("_", "")         // remove underscore wildcard
        .replace("\u{0000}", "")  // remove null byte
}



pub fn convert_to_sql(input: &str) -> String {
    // Pastikan formatnya seperti products[1].price
    let re = match regex::Regex::new(r"^(\w+)\[(\d+)\]\.(\w+)$") {
        Ok(re) => re,
        Err(_) => return "".to_string(),
    };

    if let Some(captures) = re.captures(input) {
        let table = &captures[1];
        let id = &captures[2];
        let field = &captures[3];

        format!("(SELECT {} FROM {} WHERE id = {})", field, table, id)
    } else {
        "".to_string()
    }
}


pub fn formula_replace(mut string_formula: String, body: &serde_json::Value) -> String {
    let mut i = 0;
    let mut maxloop = 0;

    while i == 0 {
        let expressions = extract_expressions(&string_formula);

        for expr in expressions {
            let expr_rplace = format!("{{{}}}", expr);
            if expr.contains("[") {
                let sql = convert_to_sql(&expr);
                string_formula = string_formula.replace(&expr_rplace, &sql);
                if !string_formula.contains("{") {
                    i = 1;
                }
            } else {
                let colreq = expr.replace("request.", "");

                let value = body
                    .get(&colreq)
                    .map(|v| {
                        format!("{}", v)
                            .replace("\"", "")
                            .replace("null", "")
                    })
                    .unwrap_or_default();
                
                string_formula = string_formula.replace(&expr_rplace, &value);
                if !string_formula.contains("{") {
                    i = 1;
                }

            }
        }
        maxloop += 1;
        if maxloop > 100 {
            println!("Max loop reached, breaking out of loop");
            break;
        }
        
    }
    string_formula

}