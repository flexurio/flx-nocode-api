//! Declarative Action Trigger Engine for `flx-nocode-api`.
//!
//! Executes automated cascading multi-table workflows (such as ERP Sales Order fulfillment,
//! stock deductions, AR invoice creation, and GL auto-posting) within the SAME atomic database
//! transaction scope (`TxStore`).
//!
//! If any action or validation fails (e.g. insufficient inventory), the error bubbles up
//! and causes the entire transaction to rollback (`tx.rollback()`).

use std::collections::HashMap;
use chrono::{Duration, Local};
use serde_json::{Map, Value};

use crate::database::state::DbParam;
use crate::log::log_output;
use crate::model::{ActionTrigger, DbType, TableSchema, TriggerAction, TriggerCondition};
use crate::storage::traits::TxStore;

/// Context data available during trigger execution.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TriggerContext<'a> {
    pub parent_table: &'a str,
    pub parent_pk: &'a str,
    pub old_record: &'a Map<String, Value>,
    pub new_record: &'a Map<String, Value>,
    pub request_body: &'a Value,
    pub actor_id: Option<&'a str>,
}

/// Check whether a trigger condition is met based on old and new record states.
pub fn evaluate_condition(
    cond_opt: &Option<TriggerCondition>,
    event_name: &str,
    old_record: &Map<String, Value>,
    new_record: &Map<String, Value>,
) -> bool {
    let Some(cond) = cond_opt else {
        // No condition declared: runs for any event matching the event_name
        return true;
    };

    // If target field is specified
    if !cond.field.is_empty() {
        let old_val = old_record.get(&cond.field);
        let new_val = new_record.get(&cond.field);

        // For update / status_change events, if the field didn't change at all, do NOT trigger.
        if event_name.contains("update") || event_name.contains("status") {
            if old_val == new_val && !old_record.is_empty() {
                return false;
            }
        }

        // Check `from` constraint if specified
        if let Some(from_rule) = &cond.from {
            let matches_from = match from_rule {
                Value::Array(arr) => arr.iter().any(|v| value_matches(v, old_val)),
                single => value_matches(single, old_val),
            };
            if !matches_from {
                return false;
            }
        }

        // Check `to` constraint if specified
        if let Some(to_rule) = &cond.to {
            let matches_to = match to_rule {
                Value::Array(arr) => arr.iter().any(|v| value_matches(v, new_val)),
                single => value_matches(single, new_val),
            };
            if !matches_to {
                return false;
            }
        }
    }

    true
}

/// Flexible value comparison (handles string vs number, case-insensitive strings).
pub fn value_matches(expected: &Value, actual_opt: Option<&Value>) -> bool {
    let Some(actual) = actual_opt else {
        return expected.is_null();
    };

    match (expected, actual) {
        (Value::String(s1), Value::String(s2)) => {
            s1.trim().eq_ignore_ascii_case(s2.trim())
        }
        (Value::Number(n1), Value::Number(n2)) => n1 == n2,
        (Value::String(s), Value::Number(n)) => s.trim() == n.to_string(),
        (Value::Number(n), Value::String(s)) => s.trim() == n.to_string(),
        (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
        (Value::Null, Value::Null) => true,
        _ => expected == actual,
    }
}

/// Runtime state maintained across actions within a single trigger execution.
#[derive(Debug, Default, Clone)]
pub struct TriggerRuntime {
    /// Cached lookup objects indexed by alias or table name (e.g. "product", "customer")
    pub lookups: HashMap<String, Value>,
    /// Accumulated numeric totals across detail line items (e.g. "total_cogs", "total_tax")
    pub accumulated: HashMap<String, f64>,
    /// Generated running sequence numbers indexed by key (e.g. "AR_INVOICE", "GL_JOURNAL")
    pub sequences: HashMap<String, String>,
}

/// Traverse nested JSON object by dot-separated path (e.g. "product.cost_price", "category.code")
pub fn resolve_nested_json(root: &Value, path: &str) -> Option<Value> {
    if path.is_empty() {
        return Some(root.clone());
    }
    // Fast path: direct key in object
    if let Value::Object(map) = root {
        if let Some(val) = map.get(path) {
            return Some(val.clone());
        }
    }
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;
    for part in parts {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            Value::Array(arr) => {
                let idx: usize = part.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

/// Resolve a variable identifier into a numeric value (f64) from item, lookups, accumulators, parent, or request.
pub fn resolve_numeric_var(
    ident: &str,
    ctx: &TriggerContext,
    runtime_opt: Option<&TriggerRuntime>,
    item_opt: Option<&Map<String, Value>>,
) -> Option<f64> {
    let clean = ident.trim().trim_matches('{').trim_matches('}');

    // 1. Check accumulators (prefix "acc." or "accumulated." or raw key)
    if let Some(prop) = clean.strip_prefix("acc.").or_else(|| clean.strip_prefix("accumulated.")) {
        if let Some(runtime) = runtime_opt {
            if let Some(v) = runtime.accumulated.get(prop) {
                return Some(*v);
            }
        }
    }
    if let Some(runtime) = runtime_opt {
        if let Some(v) = runtime.accumulated.get(clean) {
            return Some(*v);
        }
    }

    // 2. Check item line row
    if let Some(prop) = clean.strip_prefix("item.") {
        if let Some(item) = item_opt {
            let item_val = Value::Object(item.clone());
            if let Some(val) = resolve_nested_json(&item_val, prop) {
                if let Some(n) = val.as_f64() {
                    return Some(n);
                }
                if let Some(s) = val.as_str() {
                    if let Ok(n) = s.trim().parse::<f64>() {
                        return Some(n);
                    }
                }
            }
        }
    }
    if let Some(item) = item_opt {
        let item_val = Value::Object(item.clone());
        if let Some(val) = resolve_nested_json(&item_val, clean) {
            if let Some(n) = val.as_f64() {
                return Some(n);
            }
            if let Some(s) = val.as_str() {
                if let Ok(n) = s.trim().parse::<f64>() {
                    return Some(n);
                }
            }
        }
    }

    // 3. Check lookup store
    if let Some(prop) = clean.strip_prefix("lookup.") {
        if let Some(runtime) = runtime_opt {
            let lookup_val = Value::Object(runtime.lookups.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            if let Some(val) = resolve_nested_json(&lookup_val, prop) {
                if let Some(n) = val.as_f64() {
                    return Some(n);
                }
                if let Some(s) = val.as_str() {
                    if let Ok(n) = s.trim().parse::<f64>() {
                        return Some(n);
                    }
                }
            }
        }
    }
    if let Some(runtime) = runtime_opt {
        let lookup_val = Value::Object(runtime.lookups.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        if let Some(val) = resolve_nested_json(&lookup_val, clean) {
            if let Some(n) = val.as_f64() {
                return Some(n);
            }
            if let Some(s) = val.as_str() {
                if let Ok(n) = s.trim().parse::<f64>() {
                    return Some(n);
                }
            }
        }
    }

    // 4. Check parent record (new_record fallback old_record)
    if let Some(prop) = clean.strip_prefix("parent.") {
        let parent_obj = Value::Object(ctx.new_record.clone());
        if let Some(val) = resolve_nested_json(&parent_obj, prop) {
            if let Some(n) = val.as_f64() {
                return Some(n);
            }
            if let Some(s) = val.as_str() {
                if let Ok(n) = s.trim().parse::<f64>() {
                    return Some(n);
                }
            }
        }
        let old_obj = Value::Object(ctx.old_record.clone());
        if let Some(val) = resolve_nested_json(&old_obj, prop) {
            if let Some(n) = val.as_f64() {
                return Some(n);
            }
            if let Some(s) = val.as_str() {
                if let Ok(n) = s.trim().parse::<f64>() {
                    return Some(n);
                }
            }
        }
    }
    let parent_obj = Value::Object(ctx.new_record.clone());
    if let Some(val) = resolve_nested_json(&parent_obj, clean) {
        if let Some(n) = val.as_f64() {
            return Some(n);
        }
        if let Some(s) = val.as_str() {
            if let Ok(n) = s.trim().parse::<f64>() {
                return Some(n);
            }
        }
    }

    // 5. Check request body
    if let Some(prop) = clean.strip_prefix("request.") {
        if let Some(val) = resolve_nested_json(ctx.request_body, prop) {
            if let Some(n) = val.as_f64() {
                return Some(n);
            }
            if let Some(s) = val.as_str() {
                if let Ok(n) = s.trim().parse::<f64>() {
                    return Some(n);
                }
            }
        }
    }

    None
}

#[derive(Debug, PartialEq, Clone)]
enum MathToken {
    Number(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    LParen,
    RParen,
    Comma,
}

fn tokenize_math(expr: &str) -> Result<Vec<MathToken>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;
    let len = chars.len();

    while i < len {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        match c {
            '+' => { tokens.push(MathToken::Plus); i += 1; }
            '-' => { tokens.push(MathToken::Minus); i += 1; }
            '*' => { tokens.push(MathToken::Star); i += 1; }
            '/' => { tokens.push(MathToken::Slash); i += 1; }
            '%' => { tokens.push(MathToken::Percent); i += 1; }
            '(' => { tokens.push(MathToken::LParen); i += 1; }
            ')' => { tokens.push(MathToken::RParen); i += 1; }
            ',' => { tokens.push(MathToken::Comma); i += 1; }
            '0'..='9' | '.' if c != '.' || (i + 1 < len && chars[i + 1].is_ascii_digit()) => {
                let start = i;
                let mut has_dot = c == '.';
                i += 1;
                while i < len {
                    let next = chars[i];
                    if next.is_ascii_digit() {
                        i += 1;
                    } else if next == '.' && !has_dot {
                        has_dot = true;
                        i += 1;
                    } else {
                        break;
                    }
                }
                let num_str: String = chars[start..i].iter().collect();
                let num = num_str.parse::<f64>().map_err(|_| format!("Invalid number: '{}'", num_str))?;
                tokens.push(MathToken::Number(num));
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let start = i;
                i += 1;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                tokens.push(MathToken::Ident(ident));
            }
            _ => {
                return Err(format!("Unexpected character '{}' in math expression '{}'", c, expr));
            }
        }
    }

    Ok(tokens)
}

struct MathParser<'a, F>
where
    F: Fn(&str) -> Option<f64>,
{
    tokens: &'a [MathToken],
    pos: usize,
    resolver: &'a F,
}

impl<'a, F> MathParser<'a, F>
where
    F: Fn(&str) -> Option<f64>,
{
    fn peek(&self) -> Option<&MathToken> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&MathToken> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn parse_expr(&mut self) -> Result<f64, String> {
        let mut left = self.parse_term()?;
        while let Some(tok) = self.peek() {
            match tok {
                MathToken::Plus => {
                    self.advance();
                    left += self.parse_term()?;
                }
                MathToken::Minus => {
                    self.advance();
                    left -= self.parse_term()?;
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_term(&mut self) -> Result<f64, String> {
        let mut left = self.parse_factor()?;
        while let Some(tok) = self.peek() {
            match tok {
                MathToken::Star => {
                    self.advance();
                    left *= self.parse_factor()?;
                }
                MathToken::Slash => {
                    self.advance();
                    let right = self.parse_factor()?;
                    if right.abs() < 1e-12 {
                        return Err("Division by zero in math expression".to_string());
                    }
                    left /= right;
                }
                MathToken::Percent => {
                    self.advance();
                    let right = self.parse_factor()?;
                    if right.abs() < 1e-12 {
                        return Err("Modulo by zero in math expression".to_string());
                    }
                    left %= right;
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_factor(&mut self) -> Result<f64, String> {
        match self.peek() {
            Some(MathToken::Plus) => {
                self.advance();
                self.parse_factor()
            }
            Some(MathToken::Minus) => {
                self.advance();
                Ok(-self.parse_factor()?)
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<f64, String> {
        let tok = self.advance().ok_or_else(|| "Unexpected end of math expression".to_string())?.clone();
        match tok {
            MathToken::Number(n) => Ok(n),
            MathToken::LParen => {
                let inner = self.parse_expr()?;
                match self.advance() {
                    Some(MathToken::RParen) => Ok(inner),
                    _ => Err("Missing closing parenthesis ')' in math expression".to_string()),
                }
            }
            MathToken::Ident(name) => {
                if let Some(MathToken::LParen) = self.peek() {
                    self.advance();
                    let mut args = Vec::new();
                    if let Some(MathToken::RParen) = self.peek() {
                        self.advance();
                    } else {
                        loop {
                            let arg = self.parse_expr()?;
                            args.push(arg);
                            match self.peek() {
                                Some(MathToken::Comma) => {
                                    self.advance();
                                }
                                Some(MathToken::RParen) => {
                                    self.advance();
                                    break;
                                }
                                _ => return Err(format!("Expected ',' or ')' in function call to '{}'", name)),
                            }
                        }
                    }
                    match name.to_lowercase().as_str() {
                        "round" => {
                            if args.is_empty() || args.len() > 2 {
                                return Err("Function 'round' takes 1 or 2 arguments: round(val, [decimals])".to_string());
                            }
                            let val = args[0];
                            let decimals = if args.len() == 2 { args[1] as i32 } else { 0 };
                            let factor = 10.0_f64.powi(decimals);
                            Ok((val * factor).round() / factor)
                        }
                        "floor" => {
                            if args.len() != 1 {
                                return Err("Function 'floor' takes 1 argument".to_string());
                            }
                            Ok(args[0].floor())
                        }
                        "ceil" => {
                            if args.len() != 1 {
                                return Err("Function 'ceil' takes 1 argument".to_string());
                            }
                            Ok(args[0].ceil())
                        }
                        "abs" => {
                            if args.len() != 1 {
                                return Err("Function 'abs' takes 1 argument".to_string());
                            }
                            Ok(args[0].abs())
                        }
                        "min" => {
                            if args.len() != 2 {
                                return Err("Function 'min' takes 2 arguments".to_string());
                            }
                            Ok(args[0].min(args[1]))
                        }
                        "max" => {
                            if args.len() != 2 {
                                return Err("Function 'max' takes 2 arguments".to_string());
                            }
                            Ok(args[0].max(args[1]))
                        }
                        _ => Err(format!("Unknown function '{}' in math expression", name)),
                    }
                } else {
                    (self.resolver)(&name)
                        .ok_or_else(|| format!("Unknown or non-numeric variable '{}' in math expression", name))
                }
            }
            other => Err(format!("Unexpected token '{:?}' in math expression", other)),
        }
    }
}

/// Evaluate arithmetic expression (supporting operators, parentheses, functions, and dynamic variables).
pub fn eval_math_expr<F>(expr: &str, resolver: F) -> Result<f64, String>
where
    F: Fn(&str) -> Option<f64>,
{
    let tokens = tokenize_math(expr)?;
    if tokens.is_empty() {
        return Err("Empty math expression".to_string());
    }
    let mut parser = MathParser {
        tokens: &tokens,
        pos: 0,
        resolver: &resolver,
    };
    let result = parser.parse_expr()?;
    if parser.pos < parser.tokens.len() {
        return Err(format!(
            "Unexpected trailing tokens in math expression after position {}",
            parser.pos
        ));
    }
    Ok(result)
}

fn eval_conditional_token(
    expr: &str,
    ctx: &TriggerContext,
    runtime_opt: Option<&TriggerRuntime>,
    item_opt: Option<&Map<String, Value>>,
) -> Option<String> {
    let (cond_part, branches) = expr.split_once('?')?;
    let (true_val, false_val) = branches.split_once(':')?;

    let is_true = eval_boolean_condition(cond_part.trim(), ctx, runtime_opt, item_opt);
    let chosen = if is_true { true_val.trim() } else { false_val.trim() };
    Some(chosen.trim_matches('\'').trim_matches('"').to_string())
}

fn eval_boolean_condition(
    cond: &str,
    ctx: &TriggerContext,
    runtime_opt: Option<&TriggerRuntime>,
    item_opt: Option<&Map<String, Value>>,
) -> bool {
    for op in &[">=", "<=", "!=", "==", ">", "<"] {
        if let Some((left_raw, right_raw)) = cond.split_once(op) {
            let left_num = resolve_numeric_var(left_raw.trim(), ctx, runtime_opt, item_opt)
                .or_else(|| left_raw.trim().parse::<f64>().ok());
            let right_num = resolve_numeric_var(right_raw.trim(), ctx, runtime_opt, item_opt)
                .or_else(|| right_raw.trim().parse::<f64>().ok());

            if let (Some(l), Some(r)) = (left_num, right_num) {
                return match *op {
                    ">=" => l >= r,
                    "<=" => l <= r,
                    "!=" => (l - r).abs() > 1e-9,
                    "==" => (l - r).abs() <= 1e-9,
                    ">" => l > r,
                    "<" => l < r,
                    _ => false,
                };
            }

            let left_str = interpolate_string_full(left_raw.trim(), ctx, runtime_opt, item_opt);
            let right_str = interpolate_string_full(right_raw.trim(), ctx, runtime_opt, item_opt);
            return match *op {
                "==" => left_str.trim() == right_str.trim(),
                "!=" => left_str.trim() != right_str.trim(),
                _ => false,
            };
        }
    }
    false
}

/// Fully-featured template string interpolation with:
/// - `{parent.<col>}`: field from `new_record` (fallback `old_record`)
/// - `{item.<col>}` / `{item.lookup.<col>}`: field from child row
/// - `{lookup.<alias>.<col>}` / `{<alias>.<col>}`: field from previous lookup action
/// - `{acc.<col>}` / `{accumulated.<col>}`: accumulated totals
/// - `{request.<col>}`: field from request body
/// - `{calc: <math_expression>}`: dynamic math formula evaluation
/// - `{if: <condition> ? <true_val> : <false_val>}`: conditional logic
/// - `{now:YYYY-MM-DD}`, `{now+30d:YYYY-MM-DD}`, `{now()}`
/// - `{<var>|<default>}`: fallback syntax
pub fn interpolate_string_full(
    template: &str,
    ctx: &TriggerContext,
    runtime_opt: Option<&TriggerRuntime>,
    item_opt: Option<&Map<String, Value>>,
) -> String {
    let mut output = String::new();
    let chars: Vec<char> = template.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i] == '{' {
            let start = i + 1;
            let mut depth = 1;
            let mut j = start;
            while j < len && depth > 0 {
                if chars[j] == '{' {
                    depth += 1;
                } else if chars[j] == '}' {
                    depth -= 1;
                }
                j += 1;
            }
            if depth == 0 {
                let raw_token: String = chars[start..j - 1].iter().collect();
                let resolved = resolve_single_token(&raw_token, ctx, runtime_opt, item_opt);
                output.push_str(&resolved);
                i = j;
                continue;
            }
        }
        output.push(chars[i]);
        i += 1;
    }

    output
}

pub fn interpolate_string(
    template: &str,
    ctx: &TriggerContext,
    item_opt: Option<&Map<String, Value>>,
) -> String {
    interpolate_string_full(template, ctx, None, item_opt)
}

fn resolve_single_token(
    raw_token: &str,
    ctx: &TriggerContext,
    runtime_opt: Option<&TriggerRuntime>,
    item_opt: Option<&Map<String, Value>>,
) -> String {
    let (expr, fallback) = match raw_token.split_once('|') {
        Some((e, f)) => (e.trim(), Some(f.trim())),
        None => (raw_token.trim(), None),
    };

    // 1. Date interpolation: e.g. "now:YYYY-MM-DD" or "now+30d:YYYY-MM-DD"
    if expr == "now()" || expr.starts_with("now") || expr.starts_with("date:") {
        return format_date_expression(expr);
    }

    // 2. Math formula: e.g. "{calc: item.qty * product.cost_price}"
    if expr.starts_with("calc:") || expr.starts_with("math:") || expr.starts_with("eval:") {
        if let Some((_, math_expr)) = expr.split_once(':') {
            let clean = math_expr.trim().replace('{', "").replace('}', "");
            match eval_math_expr(&clean, |ident| resolve_numeric_var(ident, ctx, runtime_opt, item_opt)) {
                Ok(val) => {
                    return if val.fract() == 0.0 {
                        format!("{}", val as i64)
                    } else {
                        format!("{:.4}", val)
                            .trim_end_matches('0')
                            .trim_end_matches('.')
                            .to_string()
                    };
                }
                Err(e) => {
                    return fallback.unwrap_or(&e).to_string();
                }
            }
        }
    }

    // 3. Conditional: e.g. "{if: item.qty_delivered >= item.qty ? 'COMPLETED' : 'PARTIAL'}"
    if expr.starts_with("if:") || expr.starts_with("case:") {
        if let Some((_, cond_expr)) = expr.split_once(':') {
            if let Some(res) = eval_conditional_token(cond_expr, ctx, runtime_opt, item_opt) {
                return res;
            }
        }
    }

    // 4. Sequence token: e.g. "{seq:AR_INVOICE}" or "{seq:AR_INVOICE:INV/{YYYY}/{MM}/{0000ID}}"
    if expr.starts_with("seq:") || expr.starts_with("sequence:") {
        if let Some((_, rest)) = expr.split_once(':') {
            let key = match rest.split_once(':') {
                Some((k, _)) => k.trim(),
                None => rest.trim(),
            };
            if let Some(runtime) = runtime_opt {
                if let Some(seq_val) = runtime.sequences.get(key) {
                    return seq_val.clone();
                }
            }
        }
    }

    // 5. Resolving parent / item / acc / lookup / request references
    if let Some(prop) = expr.strip_prefix("parent.") {
        let parent_obj = Value::Object(ctx.new_record.clone());
        if let Some(v) = resolve_nested_json(&parent_obj, prop) {
            return json_val_to_str(&v);
        }
        let old_obj = Value::Object(ctx.old_record.clone());
        if let Some(v) = resolve_nested_json(&old_obj, prop) {
            return json_val_to_str(&v);
        }
    } else if let Some(prop) = expr.strip_prefix("item.") {
        if let Some(item) = item_opt {
            let item_obj = Value::Object(item.clone());
            if let Some(v) = resolve_nested_json(&item_obj, prop) {
                return json_val_to_str(&v);
            }
        }
    } else if let Some(prop) = expr.strip_prefix("acc.").or_else(|| expr.strip_prefix("accumulated.")) {
        if let Some(runtime) = runtime_opt {
            if let Some(v) = runtime.accumulated.get(prop) {
                return if v.fract() == 0.0 {
                    format!("{}", *v as i64)
                } else {
                    format!("{:.4}", v)
                        .trim_end_matches('0')
                        .trim_end_matches('.')
                        .to_string()
                };
            }
        }
    } else if let Some(prop) = expr.strip_prefix("lookup.") {
        if let Some(runtime) = runtime_opt {
            let lookup_obj = Value::Object(runtime.lookups.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            if let Some(v) = resolve_nested_json(&lookup_obj, prop) {
                return json_val_to_str(&v);
            }
        }
    } else if let Some(prop) = expr.strip_prefix("request.") {
        if let Some(v) = resolve_nested_json(ctx.request_body, prop) {
            return json_val_to_str(&v);
        }
    } else {
        // Direct lookups: check item first, then lookups, then accumulators, then parent, then request
        if let Some(item) = item_opt {
            let item_obj = Value::Object(item.clone());
            if let Some(v) = resolve_nested_json(&item_obj, expr) {
                return json_val_to_str(&v);
            }
        }
        if let Some(runtime) = runtime_opt {
            let lookup_obj = Value::Object(runtime.lookups.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            if let Some(v) = resolve_nested_json(&lookup_obj, expr) {
                return json_val_to_str(&v);
            }
            if let Some(v) = runtime.accumulated.get(expr) {
                return if v.fract() == 0.0 {
                    format!("{}", *v as i64)
                } else {
                    format!("{:.4}", v)
                        .trim_end_matches('0')
                        .trim_end_matches('.')
                        .to_string()
                };
            }
        }
        let parent_obj = Value::Object(ctx.new_record.clone());
        if let Some(v) = resolve_nested_json(&parent_obj, expr) {
            return json_val_to_str(&v);
        }
        let old_obj = Value::Object(ctx.old_record.clone());
        if let Some(v) = resolve_nested_json(&old_obj, expr) {
            return json_val_to_str(&v);
        }
        if let Some(v) = resolve_nested_json(ctx.request_body, expr) {
            return json_val_to_str(&v);
        }
    }

    fallback.unwrap_or("").to_string()
}

fn json_val_to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "".to_string(),
        _ => v.to_string(),
    }
}

/// Parse date formulas such as `now:YYYY-MM-DD`, `now+30d:YYYY-MM-DD`, `now()`
fn format_date_expression(expr: &str) -> String {
    let now = Local::now();
    let mut target_date = now;

    if expr.contains('+') {
        if let Some(plus_part) = expr.split('+').nth(1) {
            let num_str: String = plus_part.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(days) = num_str.parse::<i64>() {
                target_date = now + Duration::days(days);
            }
        }
    }

    if expr.contains("YYYY-MM-DD") || expr == "now()" {
        target_date.format("%Y-%m-%d").to_string()
    } else if expr.contains("YYYY/MM") {
        target_date.format("%Y/%m").to_string()
    } else if expr.contains("YYYYMMDD") {
        target_date.format("%Y%m%d").to_string()
    } else {
        target_date.format("%Y-%m-%d").to_string()
    }
}

/// Evaluate arithmetic set expression, supporting formulas and dynamic variables.
fn compute_set_value_full(
    target_col: &str,
    current_val: f64,
    set_expr: &str,
    ctx: &TriggerContext,
    runtime_opt: Option<&TriggerRuntime>,
    item_opt: Option<&Map<String, Value>>,
) -> Result<f64, String> {
    let interpolated = interpolate_string_full(set_expr, ctx, runtime_opt, item_opt);
    let trimmed = interpolated.trim();

    // 1. Math evaluation resolving target column and "current" to current_val
    let eval_res = eval_math_expr(trimmed, |ident| {
        let clean = ident.trim();
        if clean.eq_ignore_ascii_case(target_col)
            || clean.eq_ignore_ascii_case("current")
            || (target_col.is_empty() && trimmed.starts_with(clean))
        {
            Some(current_val)
        } else {
            resolve_numeric_var(clean, ctx, runtime_opt, item_opt)
        }
    });

    if let Ok(val) = eval_res {
        return Ok(val);
    }

    // 2. Backward compatibility fallback: "<col_name> - <val>"
    if let Some((_col, right)) = trimmed.split_once('-') {
        if let Ok(deduct_val) = right.trim().parse::<f64>() {
            return Ok(current_val - deduct_val);
        }
    }

    // 3. Backward compatibility fallback: "<col_name> + <val>"
    if let Some((_col, right)) = trimmed.split_once('+') {
        if let Ok(add_val) = right.trim().parse::<f64>() {
            return Ok(current_val + add_val);
        }
    }

    // 4. Fallback: direct number
    if let Ok(num) = trimmed.parse::<f64>() {
        return Ok(num);
    }

    Err(format!("Unsupported set expression: '{}' ({})", set_expr, eval_res.err().unwrap_or_default()))
}

fn compute_set_value(
    current_val: f64,
    set_expr: &str,
    ctx: &TriggerContext,
    item_opt: Option<&Map<String, Value>>,
) -> Result<f64, String> {
    compute_set_value_full("", current_val, set_expr, ctx, None, item_opt)
}

/// Check if string matches a standard date/datetime format (e.g. `YYYY-MM-DD` or `YYYY/MM/DD`).
pub fn is_date_format(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.len() >= 10 {
        let b = trimmed.as_bytes();
        if b[0..4].iter().all(|c| c.is_ascii_digit())
            && (b[4] == b'-' || b[4] == b'/')
            && b[5..7].iter().all(|c| c.is_ascii_digit())
            && (b[7] == b'-' || b[7] == b'/')
            && b[8..10].iter().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }
    false
}

/// Coerce a resolved string into the most appropriate DbParam type.
/// Prevents PostgreSQL "operator does not exist: integer = character varying" type mismatches.
pub fn coerce_dbparam(s: &str) -> DbParam {
    let trimmed = s.trim();
    if is_date_format(trimmed) {
        return DbParam::Str(trimmed.to_string());
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        DbParam::I64(i)
    } else if let Ok(f) = trimmed.parse::<f64>() {
        DbParam::F64(f)
    } else if trimmed.eq_ignore_ascii_case("true") {
        DbParam::Bool(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        DbParam::Bool(false)
    } else if trimmed.eq_ignore_ascii_case("null") {
        DbParam::Null
    } else {
        DbParam::Str(s.to_string())
    }
}

/// Parse and expand a sequence pattern (e.g. `INV/{YYYY}/{MM}/{0000ID}`) into prefix, width, and suffix.
pub fn parse_sequence_pattern(pattern: &str) -> (String, usize, String) {
    let now = Local::now();
    let yyyy = now.format("%Y").to_string();
    let yy = now.format("%y").to_string();
    let mm = now.format("%m").to_string();
    let dd = now.format("%d").to_string();

    let expanded = pattern
        .replace("{YYYY}", &yyyy)
        .replace("{yyyy}", &yyyy)
        .replace("{YY}", &yy)
        .replace("{yy}", &yy)
        .replace("{MM}", &mm)
        .replace("{mm}", &mm)
        .replace("{DD}", &dd)
        .replace("{dd}", &dd);

    let mut width = 4;
    let mut prefix = expanded.clone();
    let mut suffix = String::new();

    if let Some(start_idx) = expanded.find('{') {
        if let Some(end_idx) = expanded[start_idx..].find('}') {
            let inner = &expanded[start_idx + 1..start_idx + end_idx];
            if inner.ends_with("ID") || inner.ends_with("id") {
                let zeros = inner[..inner.len() - 2].chars().filter(|c| *c == '0').count();
                width = if zeros > 0 { zeros } else { 1 };
                prefix = expanded[..start_idx].to_string();
                suffix = expanded[start_idx + end_idx + 1..].to_string();
                return (prefix, width, suffix);
            }
        }
    }

    if let Some(id_idx) = expanded.find("ID").or_else(|| expanded.find("id")) {
        let before = &expanded[..id_idx];
        let zeros = before.chars().rev().take_while(|c| *c == '0').count();
        if zeros > 0 {
            width = zeros;
            prefix = expanded[..id_idx - zeros].to_string();
            suffix = expanded[id_idx + 2..].to_string();
            return (prefix, width, suffix);
        }
    }

    (prefix, width, suffix)
}

/// Queries the latest sequence number matching `prefix` in `table`.`col` and increments by 1.
async fn query_next_sequence<'a>(
    tx: &'a mut (dyn TxStore + 'a),
    table: &str,
    col: &str,
    prefix: &str,
) -> i64 {
    if table.is_empty() || col.is_empty() {
        return 1;
    }

    let like_pat = format!("{}%", prefix);
    let q_max = crate::storage::ast::Query::from(table.to_string())
        .select([col])
        .r#where(crate::storage::ast::Filter::Like(col.into(), like_pat))
        .order_by(col, false)
        .limit(1);

    let max_val: String = match tx.query(&q_max).await {
        Ok(rows) if !rows.is_empty() => rows[0]
            .get(col)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        _ => String::new(),
    };

    if max_val.is_empty() {
        return 1;
    }

    let num_part: String = max_val
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect();

    if let Ok(n) = num_part.parse::<i64>() {
        n + 1
    } else {
        1
    }
}

/// Scans text for `{seq:KEY:PATTERN}` or `{seq:KEY}` and resolves it atomically into `runtime.sequences`.
async fn ensure_sequences<'a>(
    tx: &'a mut (dyn TxStore + 'a),
    runtime: &'a mut TriggerRuntime,
    text: &str,
    target_table: &str,
    target_col: &str,
) -> Result<(), String> {
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i] == '{' {
            let is_seq = (i + 5 <= len && chars[i..i + 5].iter().collect::<String>() == "{seq:")
                || (i + 10 <= len && chars[i..i + 10].iter().collect::<String>() == "{sequence:");
            if is_seq {
                let start = i + 1;
                let mut depth = 1;
                let mut j = start;
                while j < len && depth > 0 {
                    if chars[j] == '{' {
                        depth += 1;
                    } else if chars[j] == '}' {
                        depth -= 1;
                    }
                    j += 1;
                }
                if depth == 0 {
                    let token_content: String = chars[start..j - 1].iter().collect();
                    if let Some((_, rest)) = token_content.split_once(':') {
                        let (key, pattern_opt) = match rest.split_once(':') {
                            Some((k, p)) => (k.trim(), Some(p.trim())),
                            None => (rest.trim(), None),
                        };

                        if !runtime.sequences.contains_key(key) {
                            let pattern = pattern_opt.unwrap_or("{0000ID}");
                            let (prefix, width, suffix) = parse_sequence_pattern(pattern);

                            let next_num = query_next_sequence(tx, target_table, target_col, &prefix).await;
                            let formatted = format!("{}{:0width$}{}", prefix, next_num, suffix, width = width);
                            runtime.sequences.insert(key.to_string(), formatted);
                        }
                    }
                    i = j;
                    continue;
                }
            }
        }
        i += 1;
    }
    Ok(())
}

/// Execute all matching triggers within the current transaction scope.
pub async fn execute_triggers<'a>(
    db_type: DbType,
    tx: &mut (dyn TxStore + 'a),
    table_schema: &TableSchema,
    ctx: &TriggerContext<'a>,
    event_name: &str,
) -> Result<Vec<String>, String> {
    if table_schema.action_triggers.is_empty() {
        return Ok(vec![]);
    }

    let mut executed_triggers = Vec::new();

    for trigger in &table_schema.action_triggers {
        // Match event name (e.g. "on_update", "on_status_change")
        let trigger_event = trigger.event.to_lowercase();
        let matches_event = trigger_event == event_name
            || (event_name == "on_update" && trigger_event == "on_status_change")
            || trigger_event == "any";

        if !matches_event {
            continue;
        }

        // Evaluate conditions
        if !evaluate_condition(&trigger.condition, event_name, ctx.old_record, ctx.new_record) {
            continue;
        }

        if *crate::ISDEBUG {
            log_output(
                "TRIGGER",
                "START",
                &trigger.name,
                format!("Firing trigger for table '{}' PK '{}'", ctx.parent_table, ctx.parent_pk),
                true,
            );
        }

        let mut runtime = TriggerRuntime::default();

        // Execute sequential actions
        for action in &trigger.actions {
            execute_action(db_type.clone(), tx, action, ctx, &mut runtime, None).await.map_err(|err| {
                format!(
                    "Trigger '{}' action failed: {}",
                    trigger.name, err
                )
            })?;
        }

        executed_triggers.push(trigger.name.clone());

        if *crate::ISDEBUG {
            log_output(
                "TRIGGER",
                "SUCCESS",
                &trigger.name,
                format!("Trigger '{}' completed successfully", trigger.name),
                true,
            );
        }
    }

    Ok(executed_triggers)
}

/// Execute a single trigger action (and recurse if `iterate_detail`).
fn execute_action<'a>(
    db_type: DbType,
    tx: &'a mut (dyn TxStore + 'a),
    action: &'a TriggerAction,
    ctx: &'a TriggerContext<'a>,
    runtime: &'a mut TriggerRuntime,
    item_opt: Option<Map<String, Value>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Map<String, Value>>, String>> + Send + 'a>> {
    Box::pin(async move {
        // Evaluate condition guard on individual action if specified
        if let Some(cond) = &action.condition {
            if !evaluate_condition(&Some(cond.clone()), "on_update", ctx.old_record, ctx.new_record) {
                return Ok(item_opt);
            }
        }

        let action_type = action.action_type.to_lowercase();

        match action_type.as_str() {
            // ── 1. Lookup Related Master Data (Dynamic Costing & Account Determination) ──
            "lookup" | "fetch" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'lookup' requires 'target_table'".to_string());
                }

                let filter_map = action.filter.as_ref().ok_or_else(|| {
                    format!("Action 'lookup' on '{}' requires 'filter'", target_table)
                })?;

                let mut where_clauses = Vec::new();
                let mut filter_params = Vec::new();
                for (col, tmpl_val) in filter_map {
                    let resolved_str = match tmpl_val {
                        Value::String(s) => interpolate_string_full(s, ctx, Some(runtime), item_opt.as_ref()),
                        other => json_val_to_str(other),
                    };
                    where_clauses.push(format!("{} = ?", col));
                    filter_params.push(coerce_dbparam(&resolved_str));
                }
                let where_sql = where_clauses.join(" AND ");

                let select_sql = format!("SELECT * FROM {} WHERE {}", target_table, where_sql);
                let built_select = crate::database::state::rehydrate_placeholders(
                    &select_sql,
                    db_type.as_str(),
                );

                let rows = tx
                    .raw_sql(&built_select, filter_params)
                    .await
                    .map_err(|e| format!("Error querying record in '{}': {}", target_table, e))?;

                let alias = action
                    .alias
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(target_table);

                if rows.is_empty() {
                    if action.optional == Some(true) {
                        return Ok(item_opt);
                    } else {
                        return Err(format!(
                            "Lookup in table '{}' with filter {:?} returned no record",
                            target_table, filter_map
                        ));
                    }
                }

                let found_row = rows[0].clone();
                runtime.lookups.insert(alias.to_string(), found_row.clone());

                if let Some(mut item) = item_opt {
                    item.insert(alias.to_string(), found_row);
                    return Ok(Some(item));
                }

                Ok(None)
            }

            // ── 2. Accumulate Metrics Across Detail Loops ──────────────────────────
            "accumulate" | "sum" => {
                let source_map = action.accumulate.as_ref().or(action.fields.as_ref());
                if let Some(acc_map) = source_map {
                    for (acc_key, formula_val) in acc_map {
                        let formula_str = match formula_val {
                            Value::String(s) => s.as_str(),
                            other => other.as_str().unwrap_or(""),
                        };
                        let mut clean_formula = formula_str.trim().replace('{', "").replace('}', "");
                        if let Some((_, inner)) = clean_formula.split_once(':') {
                            if clean_formula.starts_with("calc:") || clean_formula.starts_with("math:") || clean_formula.starts_with("eval:") {
                                clean_formula = inner.trim().to_string();
                            }
                        }
                        let val = eval_math_expr(&clean_formula, |ident| {
                            resolve_numeric_var(ident, ctx, Some(runtime), item_opt.as_ref())
                        })?;
                        *runtime.accumulated.entry(acc_key.clone()).or_insert(0.0) += val;
                    }
                }
                Ok(item_opt)
            }

            // ── 3. Iterate Detail (Line Items / BOM) ─────────────────────────────
            "iterate_detail" | "loop_detail" | "for_each_detail" => {
                let detail_table = action
                    .detail_table
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&action.target_table);

                if detail_table.is_empty() {
                    return Err("iterate_detail requires 'detail_table' or 'target_table'".to_string());
                }

                let fk_column = action
                    .foreign_key
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("sales_order_id");

                // Query child items belonging to this parent header
                let select_sql = format!(
                    "SELECT * FROM {} WHERE {} = ?",
                    detail_table, fk_column
                );
                let built_sql = crate::database::state::rehydrate_placeholders(
                    &select_sql,
                    db_type.as_str(),
                );

                let fk_param = coerce_dbparam(ctx.parent_pk);
                let items = tx
                    .raw_sql(&built_sql, vec![fk_param])
                    .await
                    .map_err(|e| format!("Querying detail items from '{}' failed: {}", detail_table, e))?;

                // Sort items deterministically by key to eliminate circular wait deadlocks across concurrent transactions
                let mut sorted_items = items;
                sorted_items.sort_by(|a, b| {
                    let get_sort_key = |val: &Value| -> String {
                        if let Value::Object(m) = val {
                            if let Some(id) = m.get("id").or_else(|| m.get("product_id")).or_else(|| m.get("item_id")) {
                                return json_val_to_str(id);
                            }
                        }
                        "".to_string()
                    };
                    get_sort_key(a).cmp(&get_sort_key(b))
                });

                let sub_actions = action.actions.as_deref().unwrap_or(&[]);

                for item in &sorted_items {
                    let mut current_item = item.as_object().cloned().unwrap_or_default();
                    for sub_act in sub_actions {
                        if let Some(updated) = execute_action(db_type.clone(), tx, sub_act, ctx, runtime, Some(current_item.clone())).await? {
                            current_item = updated;
                        }
                    }

                    // Evaluate iteration accumulator if specified on iterate_detail
                    if let Some(accumulate_map) = &action.accumulate {
                        for (acc_key, formula_val) in accumulate_map {
                            let formula_str = match formula_val {
                                Value::String(s) => s.as_str(),
                                other => other.as_str().unwrap_or(""),
                            };
                            let mut clean_formula = formula_str.trim().replace('{', "").replace('}', "");
                            if let Some((_, inner)) = clean_formula.split_once(':') {
                                if clean_formula.starts_with("calc:") || clean_formula.starts_with("math:") || clean_formula.starts_with("eval:") {
                                    clean_formula = inner.trim().to_string();
                                }
                            }
                            let val = eval_math_expr(&clean_formula, |ident| {
                                resolve_numeric_var(ident, ctx, Some(runtime), Some(&current_item))
                            })?;
                            *runtime.accumulated.entry(acc_key.clone()).or_insert(0.0) += val;
                        }
                    }
                }
                Ok(item_opt)
            }

            // ── 4. Update (e.g. Deduct Inventory Lot, Fulfill Delivery Order) ─────
            "update" | "decrement" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'update' requires 'target_table'".to_string());
                }

                let filter_map = action.filter.as_ref().ok_or_else(|| {
                    format!("Action 'update' on '{}' requires 'filter'", target_table)
                })?;
                let set_map = action.set.as_ref().ok_or_else(|| {
                    format!("Action 'update' on '{}' requires 'set'", target_table)
                })?;

                // Pre-generate sequences if referenced in set values
                for (col, val) in set_map {
                    if let Value::String(s) = val {
                        ensure_sequences(tx, runtime, s, target_table, col).await?;
                    }
                }

                // 1. Build filter WHERE clause with type-coerced parameters
                let mut where_clauses = Vec::new();
                let mut filter_params = Vec::new();
                for (col, tmpl_val) in filter_map {
                    let resolved_str = match tmpl_val {
                        Value::String(s) => interpolate_string_full(s, ctx, Some(runtime), item_opt.as_ref()),
                        other => json_val_to_str(other),
                    };
                    where_clauses.push(format!("{} = ?", col));
                    filter_params.push(coerce_dbparam(&resolved_str));
                }
                let where_sql = where_clauses.join(" AND ");

                // 2. Fetch current record to perform atomic validation & calculation
                let is_atomic = action.atomic.unwrap_or(true);
                let (from_target, lock_clause) = match db_type {
                    DbType::Postgres | DbType::Mysql if is_atomic => (target_table.to_string(), " FOR UPDATE"),
                    DbType::Mssql if is_atomic => (format!("{} WITH (UPDLOCK, ROWLOCK)", target_table), ""),
                    _ => (target_table.to_string(), ""),
                };
                let select_sql = format!("SELECT * FROM {} WHERE {}{}", from_target, where_sql, lock_clause);
                let built_select = crate::database::state::rehydrate_placeholders(
                    &select_sql,
                    db_type.as_str(),
                );

                let rows = tx
                    .raw_sql(&built_select, filter_params.clone())
                    .await
                    .map_err(|e| format!("Error querying record in '{}': {}", target_table, e))?;

                if rows.is_empty() {
                    return Err(format!(
                        "Record not found in '{}' matching filter {:?}",
                        target_table, filter_map
                    ));
                }

                let existing_row = rows[0].as_object().ok_or_else(|| {
                    format!("Invalid row format returned from '{}'", target_table)
                })?;

                // 3. Compute new values (supporting numeric math, strings, booleans, dates, nulls)
                let mut update_assignments = Vec::new();
                let mut update_params = Vec::new();

                for (col, set_expr_val) in set_map {
                    update_assignments.push(format!("{} = ?", col));

                    match set_expr_val {
                        Value::Bool(b) => {
                            update_params.push(DbParam::Bool(*b));
                        }
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                update_params.push(DbParam::I64(i));
                            } else if let Some(f) = n.as_f64() {
                                update_params.push(DbParam::F64(f));
                            } else {
                                update_params.push(coerce_dbparam(&n.to_string()));
                            }
                        }
                        Value::Null => {
                            update_params.push(DbParam::Null);
                        }
                        Value::String(expr_str) => {
                            let resolved_str = interpolate_string_full(expr_str, ctx, Some(runtime), item_opt.as_ref());
                            let trimmed = resolved_str.trim();

                            if is_date_format(trimmed) {
                                update_params.push(DbParam::Str(trimmed.to_string()));
                                continue;
                            }

                            let current_col_val = match existing_row.get(col) {
                                Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                                Some(Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
                                _ => 0.0,
                            };

                            let is_explicit_math = expr_str.contains("calc:") || expr_str.contains("math:") || expr_str.contains("eval:");
                            let contains_arithmetic = trimmed.contains('+') || trimmed.contains('-') || trimmed.contains('*') || trimmed.contains('/');
                            let is_existing_numeric = existing_row.get(col).map_or(false, |v| v.is_number());

                            if is_explicit_math || contains_arithmetic || is_existing_numeric {
                                if let Ok(new_calculated) = compute_set_value_full(col, current_col_val, expr_str, ctx, Some(runtime), item_opt.as_ref()) {
                                    // Enforce minimum validation constraint (e.g. preventing negative inventory)
                                    if let Some(validate) = &action.validate {
                                        if let Some(min_map) = &validate.min {
                                            if let Some(min_val_json) = min_map.get(col) {
                                                let min_threshold = min_val_json.as_f64().unwrap_or(0.0);
                                                if new_calculated < min_threshold {
                                                    let default_msg = format!(
                                                        "Validation failed on '{}': resulting {} ({}) is below minimum allowed ({})",
                                                        target_table, col, new_calculated, min_threshold
                                                    );
                                                    let err_msg = validate
                                                        .error_message
                                                        .as_ref()
                                                        .map(|m| interpolate_string_full(m, ctx, Some(runtime), item_opt.as_ref()))
                                                        .unwrap_or(default_msg);
                                                    return Err(err_msg);
                                                }
                                            }
                                        }
                                    }

                                    if new_calculated.fract() == 0.0 {
                                        update_params.push(DbParam::I64(new_calculated as i64));
                                    } else {
                                        update_params.push(DbParam::F64(new_calculated));
                                    }
                                    continue;
                                } else if is_explicit_math {
                                    return Err(format!("Failed evaluating math expression for '{}': {}", col, expr_str));
                                }
                            }

                            // Otherwise, treat as dynamic string / boolean / null / scalar
                            update_params.push(coerce_dbparam(trimmed));
                        }
                        _ => return Err(format!("Set expression for '{}' must be a string, number, or boolean", col)),
                    }
                }

                // 4. Execute the update query
                let mut all_params = update_params;
                all_params.extend(filter_params);

                let update_sql = format!(
                    "UPDATE {} SET {} WHERE {}",
                    target_table,
                    update_assignments.join(", "),
                    where_sql
                );
                let built_update = crate::database::state::rehydrate_placeholders(
                    &update_sql,
                    db_type.as_str(),
                );

                tx.raw_sql(&built_update, all_params)
                    .await
                    .map_err(|e| format!("Failed to update '{}': {}", target_table, e))?;

                Ok(item_opt)
            }

            // ── 5. Insert Record (e.g. Create AR Invoice Draft) ─────────────────
            "insert" | "create_record" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'insert' requires 'target_table'".to_string());
                }

                let fields_map = action.fields.as_ref().ok_or_else(|| {
                    format!("Action 'insert' on '{}' requires 'fields'", target_table)
                })?;

                // Pre-generate sequences if referenced in insert fields
                for (col, val) in fields_map {
                    if let Value::String(s) = val {
                        ensure_sequences(tx, runtime, s, target_table, col).await?;
                    }
                }

                let mut cols = Vec::new();
                let mut placeholders = Vec::new();
                let mut params = Vec::new();

                for (col, val) in fields_map {
                    cols.push(col.clone());
                    placeholders.push("?");

                    let resolved_str = match val {
                        Value::String(s) => interpolate_string_full(s, ctx, Some(runtime), item_opt.as_ref()),
                        other => json_val_to_str(other),
                    };

                    params.push(coerce_dbparam(&resolved_str));
                }

                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    target_table,
                    cols.join(", "),
                    placeholders.join(", ")
                );
                let built_insert = crate::database::state::rehydrate_placeholders(
                    &insert_sql,
                    db_type.as_str(),
                );

                tx.raw_sql(&built_insert, params)
                    .await
                    .map_err(|e| format!("Failed to insert into '{}': {}", target_table, e))?;

                Ok(item_opt)
            }

            // ── 6. Insert Batch (e.g. GL Journal Lines) ─────────────────────────
            "insert_batch" | "create_records" => {
                let target_table = &action.target_table;
                if target_table.is_empty() {
                    return Err("Action 'insert_batch' requires 'target_table'".to_string());
                }

                let rows = action.rows.as_deref().unwrap_or(&[]);

                // Pre-generate sequences if referenced in batch rows
                for row_map in rows {
                    for (col, val) in row_map {
                        if let Value::String(s) = val {
                            ensure_sequences(tx, runtime, s, target_table, col).await?;
                        }
                    }
                }

                // ERP Double-Entry Balancing Invariant check (Debit == Credit)
                if let Some(validate) = &action.validate {
                    if let Some(bal) = &validate.assert_balanced {
                        let debit_field = &bal.debit_field;
                        let credit_field = &bal.credit_field;
                        let tolerance = bal.tolerance.unwrap_or(0.001);

                        let mut total_debit = 0.0;
                        let mut total_credit = 0.0;

                        for row_map in rows {
                            if let Some(val) = row_map.get(debit_field) {
                                let resolved = match val {
                                    Value::String(s) => interpolate_string_full(s, ctx, Some(runtime), item_opt.as_ref()),
                                    other => json_val_to_str(other),
                                };
                                if let Ok(n) = resolved.parse::<f64>() {
                                    total_debit += n;
                                }
                            }
                            if let Some(val) = row_map.get(credit_field) {
                                let resolved = match val {
                                    Value::String(s) => interpolate_string_full(s, ctx, Some(runtime), item_opt.as_ref()),
                                    other => json_val_to_str(other),
                                };
                                if let Ok(n) = resolved.parse::<f64>() {
                                    total_credit += n;
                                }
                            }
                        }

                        let diff = (total_debit - total_credit).abs();
                        if diff > tolerance {
                            let default_msg = format!(
                                "Double-entry balancing validation failed on '{}': Total debit ({:.2}) does not balance with total credit ({:.2}) [difference: {:.2}]",
                                target_table, total_debit, total_credit, diff
                            );
                            let err_msg = validate
                                .error_message
                                .as_ref()
                                .map(|m| interpolate_string_full(m, ctx, Some(runtime), item_opt.as_ref()))
                                .unwrap_or(default_msg);
                            return Err(err_msg);
                        }
                    }
                }

                for row_map in rows {
                    let mut cols = Vec::new();
                    let mut placeholders = Vec::new();
                    let mut params = Vec::new();

                    for (col, val) in row_map {
                        cols.push(col.clone());
                        placeholders.push("?");

                        let resolved_str = match val {
                            Value::String(s) => interpolate_string_full(s, ctx, Some(runtime), item_opt.as_ref()),
                            other => json_val_to_str(other),
                        };

                        params.push(coerce_dbparam(&resolved_str));
                    }

                    let insert_sql = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        target_table,
                        cols.join(", "),
                        placeholders.join(", ")
                    );
                    let built_insert = crate::database::state::rehydrate_placeholders(
                        &insert_sql,
                        db_type.as_str(),
                    );

                    tx.raw_sql(&built_insert, params)
                        .await
                        .map_err(|e| format!("Failed batch insert into '{}': {}", target_table, e))?;
                }

                Ok(item_opt)
            }

            // ── 7. Raw/Parameterized SQL ─────────────────────────────────────────
            "sql" => {
                let stmt = action
                    .statement
                    .as_deref()
                    .ok_or_else(|| "Action 'sql' requires 'statement'".to_string())?;

                let mut params = Vec::new();
                if let Some(param_templates) = &action.params {
                    for tmpl in param_templates {
                        let resolved = interpolate_string_full(tmpl, ctx, Some(runtime), item_opt.as_ref());
                        params.push(coerce_dbparam(&resolved));
                    }
                }

                let built_sql = crate::database::state::rehydrate_placeholders(stmt, db_type.as_str());
                tx.raw_sql(&built_sql, params)
                    .await
                    .map_err(|e| format!("Custom SQL action failed: {}", e))?;

                Ok(item_opt)
            }

            unsupported => Err(format!("Unsupported action type: '{}'", unsupported)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_condition_matches_field_change() {
        let cond = TriggerCondition {
            field: "status".to_string(),
            from: Some(serde_json::json!(["APPROVED", "PENDING"])),
            to: Some(serde_json::json!("SHIPPED")),
            expression: None,
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));

        let mut new_rec = Map::new();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        assert!(evaluate_condition(&Some(cond.clone()), "on_update", &old_rec, &new_rec));

        // When old status is not in allowed `from` list
        old_rec.insert("status".to_string(), serde_json::json!("CANCELLED"));
        assert!(!evaluate_condition(&Some(cond.clone()), "on_update", &old_rec, &new_rec));

        // When status didn't change (idempotency check)
        old_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));
        assert!(!evaluate_condition(&Some(cond), "on_update", &old_rec, &new_rec));
    }

    #[test]
    fn test_interpolate_string_parent_and_item() {
        let mut old_rec = Map::new();
        old_rec.insert("customer_id".to_string(), serde_json::json!(3));
        let mut new_rec = old_rec.clone();
        new_rec.insert("total_net".to_string(), serde_json::json!(250000));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: Some("1"),
        };

        let mut item = Map::new();
        item.insert("product_id".to_string(), serde_json::json!(12));
        item.insert("qty".to_string(), serde_json::json!(5));

        let tmpl = "UPDATE lot SET qty = qty - {item.qty} WHERE product_id = {item.product_id} AND so = {parent.customer_id}";
        let res = interpolate_string(tmpl, &ctx, Some(&item));
        assert_eq!(res, "UPDATE lot SET qty = qty - 5 WHERE product_id = 12 AND so = 3");
    }

    #[test]
    fn test_compute_set_value_deduction() {
        let old_rec = Map::new();
        let new_rec = Map::new();
        let req_body = serde_json::json!({});
        let ctx = TriggerContext {
            parent_table: "test",
            parent_pk: "1",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut item = Map::new();
        item.insert("qty".to_string(), serde_json::json!(5));

        let res = compute_set_value(100.0, "qty - {item.qty}", &ctx, Some(&item)).unwrap();
        assert_eq!(res, 95.0);
    }

    // ── Mock TxStore for end-to-end ERP trigger testing ─────────────────────
    struct MockTxStore {
        pub executed_sqls: Vec<(String, Vec<DbParam>)>,
        pub lot_stock_12: f64,
        pub lot_stock_14: f64,
    }

    #[async_trait::async_trait]
    impl TxStore for MockTxStore {
        async fn query(&mut self, q: &crate::storage::ast::Query) -> anyhow::Result<Vec<Value>> {
            if q.collection == "transaction_account_receivable" {
                return Ok(vec![serde_json::json!({
                    "faktur_no": "INV/2026/09/0042"
                })]);
            }
            Ok(vec![])
        }
        async fn insert(&mut self, _collection: &str, doc: Value) -> anyhow::Result<Value> {
            Ok(doc)
        }
        async fn update(&mut self, _collection: &str, _filter: Option<crate::storage::ast::Filter>, _patch: Value) -> anyhow::Result<u64> {
            Ok(1)
        }
        async fn delete(&mut self, _collection: &str, _filter: Option<crate::storage::ast::Filter>) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn raw_sql(&mut self, sql: &str, params: Vec<DbParam>) -> anyhow::Result<Vec<Value>> {
            self.executed_sqls.push((sql.to_string(), params.clone()));

            // Detail items for Sales Order
            if sql.contains("SELECT * FROM transaction_sales_order_item") {
                for p in &params {
                    let p_str = match p {
                        DbParam::Str(s) => s.clone(),
                        DbParam::I64(n) => n.to_string(),
                        _ => "".to_string(),
                    };
                    if p_str == "10" {
                        return Ok(vec![serde_json::json!({
                            "id": 10,
                            "sales_order_id": 105,
                            "product_id": 12,
                            "qty": 10,
                            "qty_delivered": 0,
                            "fulfillment_status": "PENDING"
                        })]);
                    }
                }
                return Ok(vec![
                    serde_json::json!({
                        "id": 10,
                        "sales_order_id": 105,
                        "product_id": 12,
                        "product_name": "Paracetamol 500mg",
                        "qty": 5
                    }),
                    serde_json::json!({
                        "id": 11,
                        "sales_order_id": 105,
                        "product_id": 14,
                        "product_name": "Amoxicillin 500mg",
                        "qty": 10
                    }),
                ]);
            }

            // Master Product lookup
            if sql.contains("SELECT * FROM master_product") {
                for p in &params {
                    let p_str = match p {
                        DbParam::Str(s) => s.clone(),
                        DbParam::I64(n) => n.to_string(),
                        _ => "".to_string(),
                    };
                    if p_str == "12" {
                        return Ok(vec![serde_json::json!({
                            "id": 12,
                            "name": "Paracetamol 500mg",
                            "cost_price": 12000,
                            "cogs_account_code": "5101",
                            "inventory_account_code": "1103"
                        })]);
                    }
                    if p_str == "14" {
                        return Ok(vec![serde_json::json!({
                            "id": 14,
                            "name": "Amoxicillin 500mg",
                            "cost_price": 15000,
                            "cogs_account_code": "5101",
                            "inventory_account_code": "1103"
                        })]);
                    }
                }
            }

            // Detail items for Delivery Order
            if sql.contains("SELECT * FROM transaction_delivery_order_item") {
                return Ok(vec![
                    serde_json::json!({
                        "id": 1,
                        "delivery_order_id": 501,
                        "sales_order_item_id": 10,
                        "product_id": 12,
                        "qty_shipped": 5,
                        "qty_ordered": 10
                    })
                ]);
            }

            // Current inventory lot stock query
            if sql.contains("SELECT * FROM transaction_product_lot") {
                for p in &params {
                    let p_str = match p {
                        DbParam::Str(s) => s.clone(),
                        DbParam::I64(n) => n.to_string(),
                        _ => "".to_string(),
                    };
                    if p_str == "12" {
                        return Ok(vec![serde_json::json!({
                            "id": 1,
                            "product_id": 12,
                            "qty": self.lot_stock_12,
                        })]);
                    }
                    if p_str == "14" {
                        return Ok(vec![serde_json::json!({
                            "id": 2,
                            "product_id": 14,
                            "qty": self.lot_stock_14,
                        })]);
                    }
                }
            }

            Ok(vec![])
        }
        async fn commit(self: Box<Self>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn rollback(self: Box<Self>) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sales_order_fulfillment_end_to_end() {
        let trigger_json = r#"{
            "name": "sales_order_fulfillment",
            "event": "on_update",
            "condition": {
                "field": "status",
                "from": ["APPROVED", "PENDING"],
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "name": "deduct_inventory_stock",
                    "type": "iterate_detail",
                    "detail_table": "transaction_sales_order_item",
                    "foreign_key": "sales_order_id",
                    "actions": [
                        {
                            "type": "update",
                            "target_table": "transaction_product_lot",
                            "filter": { "product_id": "{item.product_id}" },
                            "set": { "qty": "qty - {item.qty}" },
                            "validate": { "min": { "qty": 0 } }
                        }
                    ]
                },
                {
                    "name": "generate_ar_invoice",
                    "type": "insert",
                    "target_table": "transaction_account_receivable",
                    "fields": {
                        "customer_id": "{parent.customer_id}",
                        "total_receivable": "{parent.total_net}",
                        "status": "UNPAID"
                    }
                },
                {
                    "name": "post_gl_entries",
                    "type": "insert_batch",
                    "target_table": "transaction_general_ledger_line",
                    "rows": [
                        {
                            "account_code": "1103",
                            "account_name": "Accounts Receivable (AR)",
                            "debit": "{parent.total_net}",
                            "credit": 0
                        },
                        {
                            "account_code": "4101",
                            "account_name": "Sales Revenue",
                            "debit": 0,
                            "credit": "{parent.total_net}"
                        }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("id".to_string(), serde_json::json!(105));
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        old_rec.insert("customer_id".to_string(), serde_json::json!(3));
        old_rec.insert("total_net".to_string(), serde_json::json!(250000));

        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: Some("1"),
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 50.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Trigger execution should succeed: {:?}", res.err());
        let executed = res.unwrap();
        assert_eq!(executed, vec!["sales_order_fulfillment"]);

        // Verify SQL queries executed:
        // 1. SELECT items from detail
        assert!(mock_tx.executed_sqls.iter().any(|(s, _)| s.contains("SELECT * FROM transaction_sales_order_item")));

        // 2. Stock deductions: lot 12 deducted from 100 -> 95, lot 14 deducted from 50 -> 40
        let updates: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("UPDATE transaction_product_lot"))
            .collect();
        assert_eq!(updates.len(), 2, "Should execute 2 lot stock updates (1 per item)");

        // Product 12 lot update (95)
        let has_update_12 = updates.iter().any(|(_, params)| {
            params.iter().any(|p| match p { DbParam::I64(n) => *n == 95, DbParam::F64(f) => *f == 95.0, _ => false })
        });
        assert!(has_update_12, "Lot 12 should be updated to 95");

        // Product 14 lot update (40)
        let has_update_14 = updates.iter().any(|(_, params)| {
            params.iter().any(|p| match p { DbParam::I64(n) => *n == 40, DbParam::F64(f) => *f == 40.0, _ => false })
        });
        assert!(has_update_14, "Lot 14 should be updated to 40");

        // 3. AR invoice creation
        let ar_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_account_receivable"))
            .collect();
        assert_eq!(ar_inserts.len(), 1, "Should create 1 AR invoice draft");
        let ar_has_total = ar_inserts[0].1.iter().any(|p| match p { DbParam::I64(n) => *n == 250000, _ => false });
        assert!(ar_has_total, "AR invoice should have total 250000");

        // 4. GL Journal entries
        let gl_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_general_ledger_line"))
            .collect();
        assert_eq!(gl_inserts.len(), 2, "Should post 2 GL journal lines (debit & credit)");
    }

    #[tokio::test]
    async fn test_sales_order_fulfillment_fails_on_insufficient_stock() {
        let trigger_json = r#"{
            "name": "sales_order_fulfillment",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "type": "iterate_detail",
                    "detail_table": "transaction_sales_order_item",
                    "actions": [
                        {
                            "type": "update",
                            "target_table": "transaction_product_lot",
                            "filter": { "product_id": "{item.product_id}" },
                            "set": { "qty": "qty - {item.qty}" },
                            "validate": { "min": { "qty": 0 } }
                        }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        // Insufficient stock: lot 12 has only 2 items in stock, but order item requires 5
        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 2.0,
            lot_stock_14: 50.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_err(), "Trigger execution must fail when stock is insufficient");
        let err_msg = res.unwrap_err();
        assert!(err_msg.contains("Validation failed on 'transaction_product_lot'"), "Error should mention validation failure: {}", err_msg);
    }

    #[actix_web::test]
    async fn test_gl_posting_fails_on_unbalanced_double_entry() {
        let trigger_json = r#"{
            "name": "auto_gl",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "type": "insert_batch",
                    "target_table": "transaction_general_ledger_line",
                    "validate": {
                        "assert_balanced": {
                            "debit_field": "debit",
                            "credit_field": "credit"
                        }
                    },
                    "rows": [
                        { "account_code": "1103", "debit": 250000, "credit": 0 },
                        { "account_code": "4101", "debit": 0, "credit": 200000 }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_err(), "Trigger execution must fail when GL is unbalanced");
        let err = res.unwrap_err();
        assert!(err.contains("Double-entry balancing validation failed"), "Error: {}", err);
    }

    #[actix_web::test]
    async fn test_gl_posting_succeeds_on_balanced_double_entry() {
        let trigger_json = r#"{
            "name": "auto_gl",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "type": "insert_batch",
                    "target_table": "transaction_general_ledger_line",
                    "validate": {
                        "assert_balanced": {
                            "debit_field": "debit",
                            "credit_field": "credit"
                        }
                    },
                    "rows": [
                        { "account_code": "1103", "debit": 250000, "credit": 0 },
                        { "account_code": "4101", "debit": 0, "credit": 250000 }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Trigger execution must succeed when GL is balanced: {:?}", res.err());
    }

    #[test]
    fn test_eval_math_expr_operators_and_functions() {
        let no_vars = |_name: &str| None;

        // Basic arithmetic & precedence
        assert_eq!(eval_math_expr("10 + 20 * 3", no_vars).unwrap(), 70.0);
        assert_eq!(eval_math_expr("(10 + 20) * 3", no_vars).unwrap(), 90.0);
        assert_eq!(eval_math_expr("100 - 50 / 2", no_vars).unwrap(), 75.0);
        assert_eq!(eval_math_expr("10 % 3", no_vars).unwrap(), 1.0);
        assert_eq!(eval_math_expr("-5 + 15", no_vars).unwrap(), 10.0);

        // Math functions
        assert_eq!(eval_math_expr("round(123.456, 2)", no_vars).unwrap(), 123.46);
        assert_eq!(eval_math_expr("round(123.456)", no_vars).unwrap(), 123.0);
        assert_eq!(eval_math_expr("floor(5.99)", no_vars).unwrap(), 5.0);
        assert_eq!(eval_math_expr("ceil(5.01)", no_vars).unwrap(), 6.0);
        assert_eq!(eval_math_expr("abs(-42.5)", no_vars).unwrap(), 42.5);
        assert_eq!(eval_math_expr("min(10, 5)", no_vars).unwrap(), 5.0);
        assert_eq!(eval_math_expr("max(10, 5)", no_vars).unwrap(), 10.0);

        // Variables
        let vars = |name: &str| match name {
            "qty" => Some(5.0),
            "cost_price" => Some(15000.0),
            "discount_pct" => Some(10.0),
            _ => None,
        };
        assert_eq!(eval_math_expr("qty * cost_price", vars).unwrap(), 75000.0);
        assert_eq!(eval_math_expr("qty * cost_price * (1 - discount_pct / 100)", vars).unwrap(), 67500.0);

        // Errors
        assert!(eval_math_expr("10 / 0", no_vars).is_err(), "Division by zero should error");
        assert!(eval_math_expr("unknown_var + 1", no_vars).is_err(), "Unknown variable should error");
    }

    #[test]
    fn test_interpolate_string_with_calc_and_nested_properties() {
        let mut old_rec = Map::new();
        old_rec.insert("customer_id".to_string(), serde_json::json!(3));
        let mut new_rec = old_rec.clone();
        new_rec.insert("subtotal".to_string(), serde_json::json!(200000));

        let req_body = serde_json::json!({});
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut item = Map::new();
        item.insert("qty".to_string(), serde_json::json!(5));
        item.insert("product".to_string(), serde_json::json!({
            "cost_price": 12000,
            "cogs_account_code": "5101"
        }));

        let mut runtime = TriggerRuntime::default();
        runtime.accumulated.insert("total_cogs".to_string(), 60000.0);

        // 1. Nested lookup property
        let res1 = interpolate_string_full("COGS Account: {product.cogs_account_code}", &ctx, Some(&runtime), Some(&item));
        assert_eq!(res1, "COGS Account: 5101");

        // 2. Dynamic formula calculation without braces
        let res2 = interpolate_string_full("Line COGS: {calc: item.qty * product.cost_price}", &ctx, Some(&runtime), Some(&item));
        assert_eq!(res2, "Line COGS: 60000");

        // 3. Dynamic formula with inner braces
        let res3 = interpolate_string_full("Line COGS: {calc: {item.qty} * {product.cost_price}}", &ctx, Some(&runtime), Some(&item));
        assert_eq!(res3, "Line COGS: 60000");

        // 4. Tax calculation on parent
        let res4 = interpolate_string_full("Tax: {calc: parent.subtotal * 0.11}", &ctx, Some(&runtime), Some(&item));
        assert_eq!(res4, "Tax: 22000");

        // 5. Reading accumulator
        let res5 = interpolate_string_full("Total COGS: {acc.total_cogs}", &ctx, Some(&runtime), Some(&item));
        assert_eq!(res5, "Total COGS: 60000");
    }

    #[test]
    fn test_conditional_if_token_evaluation() {
        let old_rec = Map::new();
        let new_rec = Map::new();
        let req_body = serde_json::json!({});
        let ctx = TriggerContext {
            parent_table: "test",
            parent_pk: "1",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut item = Map::new();
        item.insert("qty".to_string(), serde_json::json!(10));
        item.insert("qty_shipped".to_string(), serde_json::json!(6));

        // When qty_shipped < qty
        let res1 = interpolate_string_full(
            "Status: {if: item.qty_shipped >= item.qty ? 'COMPLETED' : 'PARTIALLY_SHIPPED'}",
            &ctx,
            None,
            Some(&item),
        );
        assert_eq!(res1, "Status: PARTIALLY_SHIPPED");

        // When qty_shipped == qty
        item.insert("qty_shipped".to_string(), serde_json::json!(10));
        let res2 = interpolate_string_full(
            "Status: {if: item.qty_shipped >= item.qty ? 'COMPLETED' : 'PARTIALLY_SHIPPED'}",
            &ctx,
            None,
            Some(&item),
        );
        assert_eq!(res2, "Status: COMPLETED");
    }

    #[tokio::test]
    async fn test_dynamic_cogs_lookup_and_gl_posting_end_to_end() {
        let trigger_json = r#"{
            "name": "sales_order_fulfillment_dynamic",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "name": "process_details",
                    "type": "iterate_detail",
                    "detail_table": "transaction_sales_order_item",
                    "foreign_key": "sales_order_id",
                    "accumulate": {
                        "total_cogs": "{calc: item.qty * product.cost_price}"
                    },
                    "actions": [
                        {
                            "name": "lookup_master_product",
                            "type": "lookup",
                            "target_table": "master_product",
                            "as": "product",
                            "filter": { "id": "{item.product_id}" }
                        },
                        {
                            "name": "deduct_stock",
                            "type": "update",
                            "target_table": "transaction_product_lot",
                            "filter": { "product_id": "{item.product_id}" },
                            "set": { "qty": "qty - {item.qty}" },
                            "validate": { "min": { "qty": 0 } }
                        }
                    ]
                },
                {
                    "name": "post_cogs_gl_journal",
                    "type": "insert_batch",
                    "target_table": "transaction_general_ledger_line",
                    "validate": {
                        "assert_balanced": {
                            "debit_field": "debit",
                            "credit_field": "credit"
                        }
                    },
                    "rows": [
                        {
                            "account_code": "5101",
                            "account_name": "Cost of Goods Sold (COGS)",
                            "debit": "{acc.total_cogs}",
                            "credit": 0
                        },
                        {
                            "account_code": "1103",
                            "account_name": "Merchandise Inventory",
                            "debit": 0,
                            "credit": "{acc.total_cogs}"
                        }
                    ]
                },
                {
                    "name": "generate_tax_invoice",
                    "type": "insert",
                    "target_table": "transaction_account_receivable",
                    "fields": {
                        "customer_id": "{parent.customer_id}",
                        "subtotal": "{parent.total_net}",
                        "tax_amount": "{calc: parent.total_net * 0.11}",
                        "grand_total": "{calc: parent.total_net * 1.11}",
                        "status": "UNPAID"
                    }
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("id".to_string(), serde_json::json!(105));
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        old_rec.insert("customer_id".to_string(), serde_json::json!(3));
        old_rec.insert("total_net".to_string(), serde_json::json!(250000));

        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: Some("1"),
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 50.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Dynamic COGS trigger should succeed: {:?}", res.err());

        // Calculation check:
        // Item 1: Product 12, qty 5 * cost_price 12000 = 60000
        // Item 2: Product 14, qty 10 * cost_price 15000 = 150000
        // Expected total_cogs = 210000

        // Verify GL Lines had exact dynamic total_cogs of 210000
        let gl_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_general_ledger_line"))
            .collect();
        assert_eq!(gl_inserts.len(), 2, "Should insert 2 GL journal rows");
        let has_cogs_debit = gl_inserts[0].1.iter().any(|p| match p { DbParam::I64(n) => *n == 210000, _ => false });
        assert!(has_cogs_debit, "COGS GL line must have debit 210000 calculated dynamically");
        let has_inventory_credit = gl_inserts[1].1.iter().any(|p| match p { DbParam::I64(n) => *n == 210000, _ => false });
        assert!(has_inventory_credit, "Inventory GL line must have credit 210000 calculated dynamically");

        // Verify AR Invoice had calculated tax: 250000 * 0.11 = 27500, grand total = 277500
        let ar_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_account_receivable"))
            .collect();
        assert_eq!(ar_inserts.len(), 1, "Should insert 1 AR invoice");
        let has_tax = ar_inserts[0].1.iter().any(|p| match p { DbParam::I64(n) => *n == 27500, _ => false });
        assert!(has_tax, "AR invoice must have tax calculated as 27500");
        let has_grand_total = ar_inserts[0].1.iter().any(|p| match p { DbParam::I64(n) => *n == 277500, _ => false });
        assert!(has_grand_total, "AR invoice must have grand total calculated as 277500");
    }

    #[tokio::test]
    async fn test_two_step_delivery_partial_fulfillment() {
        let trigger_json = r#"{
            "name": "confirm_delivery_order",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "CONFIRMED"
            },
            "actions": [
                {
                    "name": "process_delivery_items",
                    "type": "iterate_detail",
                    "detail_table": "transaction_delivery_order_item",
                    "foreign_key": "delivery_order_id",
                    "actions": [
                        {
                            "name": "update_sales_order_item_progress",
                            "type": "update",
                            "target_table": "transaction_sales_order_item",
                            "filter": { "id": "{item.sales_order_item_id}" },
                            "set": {
                                "qty_delivered": "qty_delivered + {item.qty_shipped}"
                            }
                        }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_delivery_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("id".to_string(), serde_json::json!(501));
        old_rec.insert("status".to_string(), serde_json::json!("DRAFT"));

        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("CONFIRMED"));

        let req_body = serde_json::json!({ "status": "CONFIRMED" });
        let ctx = TriggerContext {
            parent_table: "transaction_delivery_order",
            parent_pk: "501",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };

        let res = execute_triggers(DbType::Sqlite, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Delivery order confirmation trigger should succeed: {:?}", res.err());

        // Verify update to sales order item: qty_delivered was 0, ships 5 -> updated to 5
        let so_updates: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("UPDATE transaction_sales_order_item"))
            .collect();
        assert_eq!(so_updates.len(), 1, "Should update Sales Order item fulfillment");
        let has_qty_5 = so_updates[0].1.iter().any(|p| match p { DbParam::I64(n) => *n == 5, _ => false });
        assert!(has_qty_5, "Sales Order item qty_delivered should be incremented to 5");
    }

    #[test]
    fn test_parse_sequence_pattern_and_date_tokens() {
        let pattern = "INV/{YYYY}/{MM}/{0000ID}";
        let (prefix, width, suffix) = parse_sequence_pattern(pattern);
        let now = Local::now();
        let expected_prefix = format!("INV/{}/{}/", now.format("%Y"), now.format("%m"));
        assert_eq!(prefix, expected_prefix);
        assert_eq!(width, 4);
        assert_eq!(suffix, "");

        let pattern_with_suffix = "JV/{YYYY}/{000ID}-TEST";
        let (prefix2, width2, suffix2) = parse_sequence_pattern(pattern_with_suffix);
        assert_eq!(prefix2, format!("JV/{}/", now.format("%Y")));
        assert_eq!(width2, 3);
        assert_eq!(suffix2, "-TEST");
    }

    #[test]
    fn test_coerce_dbparam_preserves_data_types() {
        // Integer
        match coerce_dbparam("105") {
            DbParam::I64(n) => assert_eq!(n, 105),
            other => panic!("Expected I64, got {:?}", other),
        }

        // Float
        match coerce_dbparam("12500.50") {
            DbParam::F64(f) => assert!((f - 12500.50).abs() < 1e-6),
            other => panic!("Expected F64, got {:?}", other),
        }

        // Date string must NOT be parsed as number or subtraction!
        match coerce_dbparam("2026-09-06") {
            DbParam::Str(s) => assert_eq!(s, "2026-09-06"),
            other => panic!("Expected Str for date, got {:?}", other),
        }

        // Boolean
        match coerce_dbparam("true") {
            DbParam::Bool(b) => assert!(b),
            other => panic!("Expected Bool, got {:?}", other),
        }

        // String text
        match coerce_dbparam("SHIPPED") {
            DbParam::Str(s) => assert_eq!(s, "SHIPPED"),
            other => panic!("Expected Str, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_trigger_update_supports_non_numeric_and_status_and_dates() {
        let trigger_json = r#"{
            "name": "close_sales_order_and_stamp_date",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "COMPLETED"
            },
            "actions": [
                {
                    "name": "update_target_status_and_date",
                    "type": "update",
                    "target_table": "transaction_sales_order_item",
                    "filter": { "sales_order_id": "{parent.id}" },
                    "set": {
                        "fulfillment_status": "FULFILLED",
                        "completed_at": "{now:YYYY-MM-DD}",
                        "is_active": false
                    }
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("id".to_string(), serde_json::json!(105));
        old_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("COMPLETED"));

        let req_body = serde_json::json!({ "status": "COMPLETED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };

        let res = execute_triggers(DbType::Postgres, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Non-numeric update action must succeed without crash: {:?}", res.err());

        // Verify UPDATE query was executed with string, date, and bool params
        let updates: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("UPDATE transaction_sales_order_item"))
            .collect();
        assert_eq!(updates.len(), 1, "Should execute 1 update query");

        let params = &updates[0].1;
        let has_fulfilled = params.iter().any(|p| match p { DbParam::Str(s) => s == "FULFILLED", _ => false });
        assert!(has_fulfilled, "Must bind 'FULFILLED' string parameter");

        let today = Local::now().format("%Y-%m-%d").to_string();
        let has_today = params.iter().any(|p| match p { DbParam::Str(s) => s == &today, _ => false });
        assert!(has_today, "Must bind today's date formatted as string");

        let has_false = params.iter().any(|p| match p { DbParam::Bool(b) => !*b, _ => false });
        assert!(has_false, "Must bind boolean false parameter");
    }

    #[tokio::test]
    async fn test_trigger_running_sequence_generation_and_shared_reference() {
        let trigger_json = r#"{
            "name": "generate_invoice_and_journal_with_sequence",
            "event": "on_update",
            "condition": {
                "field": "status",
                "to": "SHIPPED"
            },
            "actions": [
                {
                    "name": "create_ar_invoice_with_sequence",
                    "type": "insert",
                    "target_table": "transaction_account_receivable",
                    "fields": {
                        "faktur_no": "{seq:AR_INVOICE:INV/{YYYY}/{MM}/{0000ID}}",
                        "total_amount": 250000
                    }
                },
                {
                    "name": "post_matching_gl_journal",
                    "type": "insert_batch",
                    "target_table": "transaction_general_ledger_line",
                    "rows": [
                        {
                            "voucher_no": "{seq:AR_INVOICE}",
                            "debit": 250000,
                            "credit": 0
                        },
                        {
                            "voucher_no": "{seq:AR_INVOICE}",
                            "debit": 0,
                            "credit": 250000
                        }
                    ]
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("id".to_string(), serde_json::json!(105));
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));

        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));

        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        let mut mock_tx = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };

        let res = execute_triggers(DbType::Postgres, &mut mock_tx, &schema, &ctx, "on_update").await;
        assert!(res.is_ok(), "Trigger with sequence generation should succeed: {:?}", res.err());

        // Expected sequence derived from mock latest "INV/2026/09/0042" + 1 -> "INV/2026/09/0043"
        let now = Local::now();
        let expected_seq = format!("INV/{}/{}/0043", now.format("%Y"), now.format("%m"));

        // Verify AR Invoice insert received the generated sequence number
        let ar_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_account_receivable"))
            .collect();
        assert_eq!(ar_inserts.len(), 1);
        let ar_has_seq = ar_inserts[0].1.iter().any(|p| match p { DbParam::Str(s) => s == &expected_seq, _ => false });
        assert!(ar_has_seq, "AR Invoice must have generated sequence '{}'", expected_seq);

        // Verify GL Journal inserts shared the EXACT SAME voucher number
        let gl_inserts: Vec<&(String, Vec<DbParam>)> = mock_tx.executed_sqls.iter()
            .filter(|(s, _)| s.contains("INSERT INTO transaction_general_ledger_line"))
            .collect();
        assert_eq!(gl_inserts.len(), 2);
        for ins in &gl_inserts {
            let gl_has_seq = ins.1.iter().any(|p| match p { DbParam::Str(s) => s == &expected_seq, _ => false });
            assert!(gl_has_seq, "GL line must reuse shared voucher number '{}'", expected_seq);
        }
    }

    #[tokio::test]
    async fn test_mssql_concurrency_lock_hint_syntax() {
        let trigger_json = r#"{
            "name": "mssql_lock_test",
            "event": "on_update",
            "condition": { "field": "status", "to": "SHIPPED" },
            "actions": [
                {
                    "name": "deduct_stock_mssql",
                    "type": "update",
                    "target_table": "transaction_product_lot",
                    "atomic": true,
                    "filter": { "product_id": 12 },
                    "set": { "qty": "qty - 5" }
                }
            ]
        }"#;

        let trigger: ActionTrigger = serde_json::from_str(trigger_json).unwrap();
        let schema = TableSchema {
            table: "transaction_sales_order".to_string(),
            action_triggers: vec![trigger],
            ..Default::default()
        };

        let mut old_rec = Map::new();
        old_rec.insert("status".to_string(), serde_json::json!("APPROVED"));
        let mut new_rec = old_rec.clone();
        new_rec.insert("status".to_string(), serde_json::json!("SHIPPED"));
        let req_body = serde_json::json!({ "status": "SHIPPED" });
        let ctx = TriggerContext {
            parent_table: "transaction_sales_order",
            parent_pk: "105",
            old_record: &old_rec,
            new_record: &new_rec,
            request_body: &req_body,
            actor_id: None,
        };

        // 1. Run with MSSQL
        let mut mock_tx_mssql = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };
        let res_mssql = execute_triggers(DbType::Mssql, &mut mock_tx_mssql, &schema, &ctx, "on_update").await;
        assert!(res_mssql.is_ok());
        let mssql_select = mock_tx_mssql.executed_sqls.iter().find(|(s, _)| s.contains("SELECT * FROM transaction_product_lot")).unwrap();
        assert!(mssql_select.0.contains("WITH (UPDLOCK, ROWLOCK)"), "MSSQL must include table hint WITH (UPDLOCK, ROWLOCK)");

        // 2. Run with Postgres
        let mut mock_tx_pg = MockTxStore {
            executed_sqls: Vec::new(),
            lot_stock_12: 100.0,
            lot_stock_14: 100.0,
        };
        let res_pg = execute_triggers(DbType::Postgres, &mut mock_tx_pg, &schema, &ctx, "on_update").await;
        assert!(res_pg.is_ok());
        let pg_select = mock_tx_pg.executed_sqls.iter().find(|(s, _)| s.contains("SELECT * FROM transaction_product_lot")).unwrap();
        assert!(pg_select.0.ends_with(" FOR UPDATE"), "Postgres must append FOR UPDATE");
    }
}


