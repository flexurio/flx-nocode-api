//! Built-in declarative cron scheduler (`schedules[]`).
//!
//! Runs time-based CPOB / ERP workflows directly inside the FLEXURIO engine
//! without external cron scripts (CAPA overdue, calibration due H-30, document
//! periodic review, supplier certificate expiry, retained-sample disposal, …).
//!
//! Guarantees:
//! - **Exactly one run per cron slot across replicas.** Each schedule owns two
//!   rows in `flx_sequences` (one per schedule, the table never grows):
//!   `sched:slot:<name>` (last executed slot, claimed with an atomic
//!   compare-and-set) and `sched:lease:<name>` (run lease, so cron and manual
//!   runs of the same schedule never overlap on any replica).
//! - **Missed slots are caught up** within `catch_up_secs` (restart, deploy,
//!   long previous run) — once, never twice.
//! - **Same write rules as `PUT`:** `update` actions run through
//!   [`check_transition_guards`] (`state_machine` with role `"system"`,
//!   `locked_when`), skip soft-deleted rows, stamp `updated_at` /
//!   `updated_by_id`, bump `version`, fire `action_triggers` `on_update` and
//!   entity `on_update` webhooks — all inside one transaction per row.
//! - **Idempotent:** a row whose `update` changes nothing (already in the
//!   target state) is skipped, including its later actions (no duplicate
//!   webhook / insert when two runs see the same row).
//! - **Audited:** every changed row, every failed row and every run is written
//!   to the audit trail (`SCHEDULE` / `SCHEDULE_FAILED`).
//! - Hot-reloads with [`crate::registry`]; manual test run via
//!   `POST /admin/schedules/<name>/run`.
//!
//! SQL backends only (PostgreSQL, MySQL, MSSQL, SQLite). On MongoDB the worker
//! is not started and manual runs are rejected.

use std::collections::{BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::audit::{write_audit_with, AuditEntry};
use crate::database::redis::bump_route_version;
use crate::database::state::{rehydrate_placeholders, AppState, DbParam};
use crate::log::log_output;
use crate::model::{DbType, ScheduleAction, ScheduleDef, TableSchema};
use crate::nocode::outbox;
use crate::nocode::repositories::data_update_repo::check_transition_guards;
use crate::nocode::sequences;
use crate::nocode::validate::is_valid_identifier;
use crate::registry::{self, Registry};
use crate::storage::traits::TxStore;

/// Role used for `state_machine` checks and as actor of cron runs.
pub const SYSTEM_ROLE: &str = "system";

const DEFAULT_LEASE_TTL_SECS: u64 = 900;
const DEFAULT_CATCH_UP_SECS: u64 = 3600;
const MAX_CATCH_UP_SECS: u64 = 7 * 86_400;
const DEFAULT_MAX_ROWS: usize = 10_000;

/// Last slot this process has settled (ran or saw claimed) per schedule, so
/// the 15s tick only touches the database once per slot.
static LOCAL_LAST_SLOT: Lazy<DashMap<String, i64>> = Lazy::new(DashMap::new);
/// Schedules with a cron task currently in flight in this process.
static LOCAL_RUNNING: Lazy<DashMap<String, ()>> = Lazy::new(DashMap::new);
/// `(name, slot)` already warned as "lease busy" (avoid log spam every tick).
static BUSY_WARNED: Lazy<DashMap<String, i64>> = Lazy::new(DashMap::new);

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(default)
}

fn lease_ttl(schedule: &ScheduleDef) -> u64 {
    schedule
        .lock_ttl_secs
        .unwrap_or_else(|| env_u64("SCHEDULER_LOCK_TTL_SECS", DEFAULT_LEASE_TTL_SECS))
        .max(60)
}

fn catch_up_window(schedule: &ScheduleDef) -> u64 {
    schedule
        .catch_up_secs
        .unwrap_or_else(|| env_u64("SCHEDULER_CATCH_UP_SECS", DEFAULT_CATCH_UP_SECS))
        .min(MAX_CATCH_UP_SECS)
}

fn max_rows(schedule: &ScheduleDef) -> usize {
    schedule
        .max_rows
        .unwrap_or_else(|| env_u64("SCHEDULER_MAX_ROWS", DEFAULT_MAX_ROWS as u64) as usize)
        .max(1)
}

// ── Timezone ─────────────────────────────────────────────────────────────────

/// Timezone of a schedule. IANA names use the tz database (DST-aware).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScheduleTz {
    Fixed(FixedOffset),
    Named(chrono_tz::Tz),
    Local,
}

impl ScheduleTz {
    /// Wall-clock time of the UTC instant `utc` in this timezone.
    pub fn to_local(&self, utc: DateTime<Utc>) -> DateTime<FixedOffset> {
        match self {
            ScheduleTz::Fixed(off) => utc.with_timezone(off),
            ScheduleTz::Named(tz) => utc.with_timezone(tz).fixed_offset(),
            ScheduleTz::Local => utc.with_timezone(&chrono::Local).fixed_offset(),
        }
    }
}

/// Parse a schedule timezone.
///
/// Accepts IANA names from the tz database (`"Asia/Jakarta"`,
/// `"Europe/Berlin"`, … — DST handled), the Indonesian aliases `WIB` / `WITA`
/// / `WIT`, `"UTC"` / `"GMT"`, `"Local"`, and fixed offsets (`"+07:00"`,
/// `"+0700"`, `"UTC+7"`, `"GMT-05:00"`).
pub fn parse_timezone(tz: &str) -> Result<ScheduleTz, String> {
    let raw = tz.trim();
    let utc = || FixedOffset::east_opt(0).map(ScheduleTz::Fixed).ok_or_else(|| "invalid UTC offset".to_string());
    if raw.is_empty() || raw.eq_ignore_ascii_case("UTC") || raw.eq_ignore_ascii_case("GMT") || raw == "Z" {
        return utc();
    }
    if raw.eq_ignore_ascii_case("local") {
        return Ok(ScheduleTz::Local);
    }
    let alias = match raw.to_ascii_uppercase().as_str() {
        "WIB" => Some("Asia/Jakarta"),
        "WITA" => Some("Asia/Makassar"),
        "WIT" => Some("Asia/Jayapura"),
        _ => None,
    };
    if let Ok(named) = chrono_tz::Tz::from_str(alias.unwrap_or(raw)) {
        return Ok(ScheduleTz::Named(named));
    }

    // Fixed offset: "+07:00", "+0700", "-05:00", "UTC+7", "GMT+07:00"
    let upper = raw.to_ascii_uppercase();
    let stripped = upper
        .strip_prefix("UTC")
        .or_else(|| upper.strip_prefix("GMT"))
        .unwrap_or(&upper)
        .trim()
        .to_string();
    if let Some(sign) = stripped.chars().next().filter(|&c| c == '+' || c == '-') {
        let rest = &stripped[1..];
        let (h_str, m_str) = if let Some((h, m)) = rest.split_once(':') {
            (h, m)
        } else if rest.len() == 4 {
            (&rest[..2], &rest[2..])
        } else {
            (rest, "0")
        };
        let hours: i32 = h_str.parse().map_err(|_| format!("invalid timezone '{}'", raw))?;
        let mins: i32 = m_str.parse().map_err(|_| format!("invalid timezone '{}'", raw))?;
        if hours > 23 || mins > 59 {
            return Err(format!("timezone offset out of range '{}'", raw));
        }
        let total = (hours * 3600 + mins * 60) * if sign == '-' { -1 } else { 1 };
        return FixedOffset::east_opt(total)
            .map(ScheduleTz::Fixed)
            .ok_or_else(|| format!("invalid timezone offset '{}'", raw));
    }

    Err(format!("unsupported timezone '{}' (use an IANA name such as 'Asia/Jakarta' or an offset such as '+07:00')", raw))
}

// ── Cron expression parser ───────────────────────────────────────────────────

/// Parsed 5-field cron schedule (`min hour dom month dow`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpr {
    pub minutes: BTreeSet<u32>,
    pub hours: BTreeSet<u32>,
    pub doms: BTreeSet<u32>,
    pub months: BTreeSet<u32>,
    pub dows: BTreeSet<u32>,
    /// Day-of-month field starts with `*` (unrestricted, Vixie semantics).
    pub dom_wildcard: bool,
    /// Day-of-week field starts with `*` (unrestricted, Vixie semantics).
    pub dow_wildcard: bool,
}

fn truncate_minute(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_second(0).and_then(|d| d.with_nanosecond(0)).unwrap_or(dt)
}

impl CronExpr {
    /// Parse a standard 5-field cron expression or macro (`@hourly`, `@daily`,
    /// `@midnight`, `@weekly`, `@monthly`, `@yearly`, `@annually`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        let expanded = match trimmed.to_ascii_lowercase().as_str() {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            _ => trimmed,
        };
        let fields: Vec<&str> = expanded.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!(
                "cron expression '{}' must have 5 fields (minute hour dom month dow)",
                raw
            ));
        }

        let minutes = parse_cron_field(fields[0], 0, 59, None)?;
        let hours = parse_cron_field(fields[1], 0, 23, None)?;
        let dom_wildcard = fields[2].starts_with('*');
        let doms = parse_cron_field(fields[2], 1, 31, None)?;
        let months = parse_cron_field(fields[3], 1, 12, Some(parse_month_name))?;
        let dow_wildcard = fields[4].starts_with('*');
        let raw_dows = parse_cron_field(fields[4], 0, 7, Some(parse_dow_name))?;
        // Normalize Sunday = 7 → 0
        let dows: BTreeSet<u32> = raw_dows.into_iter().map(|d| if d == 7 { 0 } else { d }).collect();

        Ok(CronExpr { minutes, hours, doms, months, dows, dom_wildcard, dow_wildcard })
    }

    fn day_matches(&self, dt: &DateTime<FixedOffset>) -> bool {
        let dom_match = self.doms.contains(&dt.day());
        let dow_match = self.dows.contains(&dt.weekday().num_days_from_sunday());
        // Vixie cron: when both DOM and DOW are restricted, either one matching
        // triggers the job; otherwise both must match (the wildcard always does).
        if !self.dom_wildcard && !self.dow_wildcard {
            dom_match || dow_match
        } else {
            dom_match && dow_match
        }
    }

    /// True when the wall-clock time `dt` matches this expression.
    pub fn matches(&self, dt: &DateTime<FixedOffset>) -> bool {
        self.minutes.contains(&dt.minute())
            && self.hours.contains(&dt.hour())
            && self.months.contains(&dt.month())
            && self.day_matches(dt)
    }

    /// Next matching minute strictly after `after` in timezone `tz`
    /// (searches up to ~1 year ahead). Steps over real instants, so DST gaps
    /// and repeated hours are handled by the timezone itself.
    pub fn next_after(&self, after: DateTime<Utc>, tz: &ScheduleTz) -> Option<DateTime<FixedOffset>> {
        let mut cur = truncate_minute(after) + chrono::Duration::minutes(1);
        let limit = cur + chrono::Duration::days(368);
        while cur <= limit {
            let local = tz.to_local(cur);
            if !self.months.contains(&local.month()) || !self.day_matches(&local) {
                // Jump towards local midnight, but stop 60 min early so a DST
                // shift can never make us overshoot into the next day.
                let to_midnight = 1440 - (local.hour() * 60 + local.minute()) as i64;
                cur += chrono::Duration::minutes(if to_midnight > 60 { to_midnight - 60 } else { 1 });
                continue;
            }
            if !self.hours.contains(&local.hour()) {
                cur += chrono::Duration::minutes((60 - local.minute() as i64).max(1));
                continue;
            }
            if self.minutes.contains(&local.minute()) {
                return Some(local);
            }
            cur += chrono::Duration::minutes(1);
        }
        None
    }

    /// Most recent matching minute in `[now - window_secs, now]` (UTC instant),
    /// used by the worker to fire the current slot or catch up a missed one.
    pub fn latest_due(&self, now: DateTime<Utc>, tz: &ScheduleTz, window_secs: u64) -> Option<DateTime<Utc>> {
        let earliest = truncate_minute(now - chrono::Duration::seconds(window_secs as i64));
        let mut cur = truncate_minute(now);
        while cur >= earliest {
            if self.matches(&tz.to_local(cur)) {
                return Some(cur);
            }
            cur -= chrono::Duration::minutes(1);
        }
        None
    }
}

/// Numeric slot id of a UTC minute (`YYYYMMDDHHMM`), stored in `flx_sequences`.
pub fn slot_id(due: DateTime<Utc>) -> i64 {
    due.format("%Y%m%d%H%M").to_string().parse().unwrap_or(0)
}

fn parse_month_name(s: &str) -> Option<u32> {
    const M: [&str; 12] = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
    let up = s.to_ascii_uppercase();
    M.iter().position(|m| *m == up).map(|i| i as u32 + 1)
}

fn parse_dow_name(s: &str) -> Option<u32> {
    const D: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
    let up = s.to_ascii_uppercase();
    D.iter().position(|d| *d == up).map(|i| i as u32)
}

fn parse_cron_value(raw: &str, min: u32, max: u32, named: Option<fn(&str) -> Option<u32>>) -> Result<u32, String> {
    let s = raw.trim();
    let v = if let Some(n) = named.and_then(|f| f(s)) {
        n
    } else {
        s.parse::<u32>().map_err(|_| format!("invalid cron value '{}'", s))?
    };
    if v < min || v > max {
        return Err(format!("cron value {} out of range {}..={}", v, min, max));
    }
    Ok(v)
}

fn parse_cron_field(
    field: &str,
    min: u32,
    max: u32,
    named: Option<fn(&str) -> Option<u32>>,
) -> Result<BTreeSet<u32>, String> {
    let mut set = BTreeSet::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(format!("empty segment in cron field '{}'", field));
        }
        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step_val = s
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| format!("invalid step '{}' in cron field '{}'", s, field))?;
                if step_val == 0 {
                    return Err(format!("step cannot be 0 in cron field '{}'", field));
                }
                (r.trim(), step_val)
            }
            None => (part, 1),
        };

        let (start, end) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let s = parse_cron_value(a, min, max, named)?;
            let e = parse_cron_value(b, min, max, named)?;
            if s > e {
                return Err(format!("invalid range {}-{} in cron field '{}'", s, e, field));
            }
            (s, e)
        } else {
            let v = parse_cron_value(range_part, min, max, named)?;
            if part.contains('/') { (v, max) } else { (v, v) }
        };

        let mut cur = start;
        while cur <= end {
            set.insert(cur);
            match cur.checked_add(step) {
                Some(next) => cur = next,
                None => break,
            }
        }
    }
    if set.is_empty() {
        return Err(format!("cron field '{}' matched no values", field));
    }
    Ok(set)
}

// ── SQL SELECT validation ────────────────────────────────────────────────────

/// Extract and validate the read-only `SELECT` SQL from `schedule.select`.
///
/// Strips an optional `"SQL:"` prefix (case-insensitive). Returns `Ok(None)`
/// when `raw` is empty (the schedule runs its actions once with an empty row).
/// Rejects multi-statement queries and non-SELECT statements.
pub fn extract_select_sql(raw: &str) -> Result<Option<String>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let sql = if trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("SQL:") {
        trimmed[4..].trim()
    } else {
        trimmed
    };
    if sql.is_empty() {
        return Err("schedule select SQL is empty".to_string());
    }
    if sql.contains(';') {
        return Err("schedule select SQL must not contain ';' (single SELECT statement only)".to_string());
    }

    let without_strings = strip_sql_string_literals(sql);
    let upper = without_strings.to_ascii_uppercase();
    let first_word = upper.split_whitespace().next().unwrap_or("");
    if first_word != "SELECT" && first_word != "WITH" {
        return Err(format!("schedule select must be a read-only SELECT query, got '{}'", first_word));
    }

    const FORBIDDEN: &[&str] = &[
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
        "CREATE", "REPLACE", "MERGE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
    ];
    for tok in upper.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        if FORBIDDEN.contains(&tok) {
            return Err(format!("schedule select SQL contains forbidden keyword '{}'", tok));
        }
    }

    Ok(Some(sql.to_string()))
}

fn strip_sql_string_literals(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut in_quote = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if in_quote && chars.peek() == Some(&'\'') {
                chars.next(); // escaped ''
            } else {
                in_quote = !in_quote;
            }
            out.push(' ');
        } else if !in_quote {
            out.push(c);
        }
    }
    out
}

// ── Template interpolation ───────────────────────────────────────────────────

fn json_val_to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn resolve_row_path(row: &Map<String, Value>, path: &str) -> Option<Value> {
    if let Some(v) = row.get(path) {
        return Some(v.clone());
    }
    let mut cur: &Value = row.get(path.split('.').next()?)?;
    for part in path.split('.').skip(1) {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur.clone())
}

fn format_now_token(expr: &str, tz: &ScheduleTz) -> String {
    let now = tz.to_local(Utc::now());
    let offset_days = |sep: char| -> Option<i64> {
        let rest = &expr[expr.find(sep)? + 1..];
        rest.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().ok()
    };
    let target = if let Some(d) = offset_days('+') {
        now + chrono::Duration::days(d)
    } else if let Some(d) = offset_days('-') {
        now - chrono::Duration::days(d)
    } else {
        now
    };

    if expr.contains("YYYY-MM-DD HH:mm:ss") {
        target.format("%Y-%m-%d %H:%M:%S").to_string()
    } else if expr.contains("YYYY-MM-DD") || expr == "now()" {
        target.format("%Y-%m-%d").to_string()
    } else if expr.contains("YYYY/MM/DD") {
        target.format("%Y/%m/%d").to_string()
    } else if expr.contains("YYYY/MM") {
        target.format("%Y/%m").to_string()
    } else {
        target.to_rfc3339()
    }
}

/// Resolve a JSON value from a schedule action (`where`, `set`, `fields`,
/// `params`) against the current selected `row`.
///
/// A value that is exactly one `{row.<col>}` placeholder keeps the original
/// JSON type (number, bool, string, null); anything else is interpolated
/// into a string.
pub fn resolve_action_value(tmpl: &Value, row: &Map<String, Value>, schedule_name: &str, tz: &ScheduleTz) -> Value {
    let Value::String(s) = tmpl else {
        return tmpl.clone();
    };
    let trimmed = s.trim();
    if trimmed.starts_with("{row.") && trimmed.ends_with('}') && trimmed.matches('{').count() == 1 {
        let path = &trimmed[5..trimmed.len() - 1];
        if let Some(v) = resolve_row_path(row, path) {
            return v;
        }
    }
    Value::String(interpolate_string(s, row, schedule_name, tz))
}

/// Interpolate `{row.<col>}`, `{schedule.name}`, and `{now…}` tokens.
pub fn interpolate_string(template: &str, row: &Map<String, Value>, schedule_name: &str, tz: &ScheduleTz) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close_rel) = rest[open + 1..].find('}') else { break };
        out.push_str(&rest[..open]);
        let tok = rest[open + 1..open + 1 + close_rel].trim();
        let resolved = if let Some(path) = tok.strip_prefix("row.") {
            resolve_row_path(row, path).map(|v| json_val_to_str(&v)).unwrap_or_default()
        } else if tok == "schedule.name" {
            schedule_name.to_string()
        } else if tok == "now()" || tok.starts_with("now:") || tok.starts_with("now+") || tok.starts_with("now-") {
            format_now_token(tok, tz)
        } else {
            resolve_row_path(row, tok).map(|v| json_val_to_str(&v)).unwrap_or_default()
        };
        out.push_str(&resolved);
        rest = &rest[open + 1 + close_rel + 1..];
    }
    out.push_str(rest);
    out
}

fn value_to_db_param(v: &Value) -> DbParam {
    match v {
        Value::Null => DbParam::Null,
        Value::Bool(b) => DbParam::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DbParam::I64(i)
            } else if let Some(f) = n.as_f64() {
                DbParam::F64(f)
            } else {
                DbParam::Str(n.to_string())
            }
        }
        Value::String(s) => DbParam::Str(s.clone()),
        other => DbParam::Str(other.to_string()),
    }
}

/// Loose equality between a stored column value and a value about to be
/// written (`1` vs `"1"`, `true` vs `1` count as equal).
fn same_value(old: Option<&Value>, new: &Value) -> bool {
    let Some(old) = old else { return false };
    if old == new {
        return true;
    }
    let norm = |v: &Value| match v {
        Value::Bool(true) => "1".to_string(),
        Value::Bool(false) => "0".to_string(),
        other => json_val_to_str(other),
    };
    !old.is_null() && !new.is_null() && norm(old) == norm(new)
}

// ── Cross-replica coordination (flx_sequences) ───────────────────────────────

fn lease_scope(name: &str) -> String {
    format!("sched:lease:{}", name)
}

fn slot_scope(name: &str) -> String {
    format!("sched:slot:{}", name)
}

/// A held run lease; release it with [`Lease::release`].
struct Lease {
    scope: String,
    expiry: i64,
}

impl Lease {
    async fn release(self, state: &AppState) {
        if let Err(e) = sequences::compare_and_set_eq(state.store.as_ref(), &self.scope, 0, self.expiry).await {
            log_output("WARN", "SCHEDULER", &self.scope, format!("lease release failed (expires on its own): {}", e), true);
        }
    }
}

/// Take the run lease of `schedule` (`Ok(None)` = another run holds it).
async fn acquire_lease(state: &AppState, schedule: &ScheduleDef) -> Result<Option<Lease>, String> {
    let now = Utc::now().timestamp();
    let expiry = now + lease_ttl(schedule) as i64;
    let scope = lease_scope(&schedule.name);
    match sequences::compare_and_set_below(state.store.as_ref(), &scope, expiry, now).await {
        Ok(true) => Ok(Some(Lease { scope, expiry })),
        Ok(false) => Ok(None),
        Err(e) => Err(format!("lease '{}' failed: {}", scope, e)),
    }
}

// ── Execution engine ─────────────────────────────────────────────────────────

/// Summary returned by a run and by `POST /admin/schedules/<name>/run`.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRunReport {
    pub schedule: String,
    /// `"cron"` or `"manual"`.
    pub trigger: String,
    /// Cron slot (`YYYYMMDDHHMM`, UTC) for cron runs.
    pub slot: Option<i64>,
    /// Rows returned by `select` (after `max_rows`).
    pub matched_rows: usize,
    /// Rows where every action ran and something changed.
    pub succeeded_rows: usize,
    /// Rows skipped because an `update` found nothing to change (idempotency).
    pub skipped_rows: usize,
    /// Rows rolled back because an action failed.
    pub failed_rows: usize,
    /// Records modified by `update` actions.
    pub updated_rows: u64,
    /// Outbox rows queued (`webhook` actions + entity `on_update` webhooks).
    pub webhooks_enqueued: usize,
    /// `action_triggers` executed by `update` actions.
    pub triggers_executed: usize,
    /// `select` returned more than `max_rows`; the rest is processed next run.
    pub truncated: bool,
    pub errors: Vec<String>,
    pub elapsed_ms: u128,
}

/// Why a manual run did not start.
#[derive(Debug)]
pub enum RunError {
    /// Another run of the same schedule holds the lease (HTTP 409).
    Busy,
    /// Backend not supported / run aborted before rows were processed (HTTP 422).
    Failed(String),
}

struct RunCtx<'a> {
    state: &'a AppState,
    reg: &'a Registry,
    schedule: &'a ScheduleDef,
    tz: ScheduleTz,
    actor_id: &'a str,
    trigger: &'a str,
}

enum RowResult {
    Done(Box<RowOutcome>),
    Skipped,
}

#[derive(Default)]
struct RowOutcome {
    updated_rows: u64,
    webhooks_enqueued: usize,
    triggers_executed: usize,
    touched_routes: Vec<String>,
    primary_route: Option<String>,
    primary_id: Option<String>,
    last_route: Option<String>,
    last_old_record: Option<Map<String, Value>>,
    last_new_record: Map<String, Value>,
}

/// Resolve a table reference (route name or physical table name).
fn find_schema_for_table<'a>(reg: &'a Registry, name: &str) -> Option<(&'a str, &'a TableSchema)> {
    if let Some((k, s)) = reg.schemas.get_key_value(name) {
        return Some((k.as_str(), s.as_ref()));
    }
    reg.schemas
        .iter()
        .find(|(_, s)| s.table.eq_ignore_ascii_case(name))
        .map(|(route, s)| (route.as_str(), s.as_ref()))
}

/// Column present on a managed table (declared, or added by `auto_generate`
/// which always creates the audit columns).
fn has_column(schema: &TableSchema, col: &str) -> bool {
    schema.auto_generate || schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col))
}

fn column_type<'a>(schema: &'a TableSchema, col: &str, default: &'a str) -> &'a str {
    schema
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(col))
        .map(|c| c.type_data.as_str())
        .unwrap_or(default)
}

fn actor_param(schema: &TableSchema, col: &str, actor_id: &str) -> DbParam {
    if actor_id == SYSTEM_ROLE {
        DbParam::Null
    } else {
        crate::nocode::pk_utils::dbparam_from_str_and_type(actor_id, column_type(schema, col, "bigint"))
    }
}

fn audit(ctx: &RunCtx<'_>, action: &str, route: &str, id: Option<&str>, ip: Option<&str>, extra: Value) {
    let mut detail = json!({ "schedule": ctx.schedule.name, "trigger": ctx.trigger });
    if let (Value::Object(d), Value::Object(e)) = (&mut detail, extra) {
        d.extend(e);
    }
    write_audit_with(
        &AuditEntry {
            at: Utc::now().to_rfc3339(),
            actor_id: ctx.actor_id.to_string(),
            action,
            route,
            id,
            ip,
        },
        detail,
    );
}

/// Execute `schedule` immediately, **without** taking the lease. Prefer
/// [`run_now`] (manual) — the cron worker calls this after claiming its slot.
pub async fn run_schedule(
    state: &AppState,
    schedule: &ScheduleDef,
    trigger: &str,
    slot: Option<i64>,
    actor_id: &str,
    ip: Option<&str>,
) -> Result<ScheduleRunReport, String> {
    let started = Instant::now();
    let reg = registry::snapshot();
    let tz = parse_timezone(&schedule.timezone)?;
    let ctx = RunCtx { state, reg: &reg, schedule, tz, actor_id, trigger };
    let run_route = format!("schedule:{}", schedule.name);

    // 1. Select candidate rows (or one empty row when `select` is omitted).
    let limit = max_rows(schedule);
    let mut truncated = false;
    let rows: Vec<Map<String, Value>> = match extract_select_sql(&schedule.select) {
        Err(e) => {
            audit(&ctx, "SCHEDULE_FAILED", &run_route, None, ip, json!({ "slot": slot, "error": e }));
            return Err(e);
        }
        Ok(None) => vec![Map::new()],
        Ok(Some(sql)) => {
            let query = match state.db_type {
                // MSSQL rejects ORDER BY inside a derived table without TOP;
                // fetch as written and cap in memory instead.
                DbType::Mssql => sql,
                _ => format!("SELECT * FROM ({}) flx_sched_sel LIMIT {}", sql, limit + 1),
            };
            match state.store.raw_sql(&query, vec![]).await {
                Ok(raw) => {
                    let mut rows: Vec<Map<String, Value>> =
                        raw.into_iter().map(|v| v.as_object().cloned().unwrap_or_default()).collect();
                    if rows.len() > limit {
                        rows.truncate(limit);
                        truncated = true;
                    }
                    rows
                }
                Err(e) => {
                    let msg = format!("schedule '{}' select failed: {}", schedule.name, e);
                    audit(&ctx, "SCHEDULE_FAILED", &run_route, None, ip, json!({ "slot": slot, "error": msg }));
                    return Err(msg);
                }
            }
        }
    };

    let mut report = ScheduleRunReport {
        schedule: schedule.name.clone(),
        trigger: trigger.to_string(),
        slot,
        matched_rows: rows.len(),
        succeeded_rows: 0,
        skipped_rows: 0,
        failed_rows: 0,
        updated_rows: 0,
        webhooks_enqueued: 0,
        triggers_executed: 0,
        truncated,
        errors: Vec::new(),
        elapsed_ms: 0,
    };
    let mut touched_routes: HashSet<String> = HashSet::new();

    for (idx, row) in rows.iter().enumerate() {
        let row_label = row.get("id").map(json_val_to_str).unwrap_or_else(|| format!("#{}", idx));
        match execute_row_actions(&ctx, row).await {
            Ok(RowResult::Skipped) => report.skipped_rows += 1,
            Ok(RowResult::Done(outcome)) => {
                report.succeeded_rows += 1;
                report.updated_rows += outcome.updated_rows;
                report.webhooks_enqueued += outcome.webhooks_enqueued;
                report.triggers_executed += outcome.triggers_executed;
                touched_routes.extend(outcome.touched_routes.iter().cloned());
                let route = outcome
                    .primary_route
                    .as_deref()
                    .or(schedule.owner_route.as_deref())
                    .unwrap_or(&run_route);
                let id = outcome.primary_id.clone().or_else(|| row.get("id").map(json_val_to_str));
                audit(&ctx, "SCHEDULE", route, id.as_deref(), ip, json!({ "slot": slot }));
            }
            Err(e) => {
                report.failed_rows += 1;
                let route = schedule.owner_route.as_deref().unwrap_or(&run_route);
                audit(&ctx, "SCHEDULE_FAILED", route, Some(&row_label), ip, json!({ "slot": slot, "error": e }));
                report.errors.push(format!("row {}: {}", row_label, e));
            }
        }
    }

    // Invalidate GET caches of every route modified by this run.
    for route in touched_routes {
        bump_route_version(&route, state.is_cachedb).await;
    }

    report.elapsed_ms = started.elapsed().as_millis();
    let summary = json!({
        "slot": slot,
        "matched": report.matched_rows,
        "succeeded": report.succeeded_rows,
        "skipped": report.skipped_rows,
        "failed": report.failed_rows,
        "updated": report.updated_rows,
        "webhooks": report.webhooks_enqueued,
        "truncated": report.truncated,
    });
    let run_action = if report.failed_rows == 0 { "SCHEDULE" } else { "SCHEDULE_FAILED" };
    audit(&ctx, run_action, &run_route, None, ip, summary);

    log_output(
        if report.failed_rows > 0 || report.truncated { "WARN" } else { "INFO" },
        "SCHEDULER",
        &schedule.name,
        format!(
            "trigger={} matched={} ok={} skipped={} failed={} updated={} webhooks={} triggers={}{} in {}ms",
            report.trigger,
            report.matched_rows,
            report.succeeded_rows,
            report.skipped_rows,
            report.failed_rows,
            report.updated_rows,
            report.webhooks_enqueued,
            report.triggers_executed,
            if report.truncated { " (truncated at max_rows)" } else { "" },
            report.elapsed_ms
        ),
        true,
    );

    Ok(report)
}

/// Manual run (`POST /admin/schedules/<name>/run`): takes the run lease so it
/// never overlaps a cron run of the same schedule on any replica.
pub async fn run_now(
    state: &AppState,
    schedule: &ScheduleDef,
    actor_id: &str,
    ip: Option<&str>,
) -> Result<ScheduleRunReport, RunError> {
    if state.db_type == DbType::Mongodb {
        return Err(RunError::Failed("schedules require a SQL backend (not supported on MongoDB)".into()));
    }
    let lease = match acquire_lease(state, schedule).await {
        Ok(Some(l)) => l,
        Ok(None) => return Err(RunError::Busy),
        Err(e) => return Err(RunError::Failed(e)),
    };
    let res = run_schedule(state, schedule, "manual", None, actor_id, ip).await;
    lease.release(state).await;
    res.map_err(RunError::Failed)
}

/// Run all actions for one selected row inside one transaction.
async fn execute_row_actions(ctx: &RunCtx<'_>, row: &Map<String, Value>) -> Result<RowResult, String> {
    let mut tx = ctx
        .state
        .store
        .begin_tx()
        .await
        .map_err(|e| format!("failed to begin transaction: {}", e))?;

    let mut outcome = RowOutcome {
        primary_id: row.get("id").map(json_val_to_str),
        last_route: ctx.schedule.owner_route.clone(),
        last_new_record: row.clone(),
        ..Default::default()
    };

    for action in &ctx.schedule.actions {
        match execute_single_action(ctx, tx.as_mut(), action, row, &mut outcome).await {
            Ok(true) => {}
            Ok(false) => {
                // An `update` changed nothing: the row is already handled.
                let _ = tx.rollback().await;
                return Ok(RowResult::Skipped);
            }
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(e);
            }
        }
    }

    tx.commit().await.map_err(|e| format!("failed to commit transaction: {}", e))?;
    Ok(RowResult::Done(Box::new(outcome)))
}

/// Execute one action. `Ok(false)` = an `update` matched nothing to change
/// (stop this row, it is skipped); `Ok(true)` = continue.
async fn execute_single_action(
    ctx: &RunCtx<'_>,
    tx: &mut (dyn TxStore + '_),
    action: &ScheduleAction,
    row: &Map<String, Value>,
    outcome: &mut RowOutcome,
) -> Result<bool, String> {
    let resolve = |tmpl: &Value| resolve_action_value(tmpl, row, &ctx.schedule.name, &ctx.tz);
    let dialect = ctx.state.db_type.as_str();

    match action.action_type.trim().to_ascii_lowercase().as_str() {
        "update" => {
            let name = action.table.trim();
            if name.is_empty() || !is_valid_identifier(name) {
                return Err(format!("invalid or empty table '{}' in update action", name));
            }
            let where_map = action
                .r#where
                .as_ref()
                .filter(|m| !m.is_empty())
                .ok_or_else(|| format!("update action on '{}' requires non-empty 'where'", name))?;
            let set_map = action
                .set
                .as_ref()
                .filter(|m| !m.is_empty())
                .ok_or_else(|| format!("update action on '{}' requires non-empty 'set'", name))?;

            let mut where_clauses = Vec::with_capacity(where_map.len() + 1);
            let mut where_params = Vec::with_capacity(where_map.len());
            for (col, tmpl) in where_map {
                if !is_valid_identifier(col) {
                    return Err(format!("invalid column '{}' in where clause", col));
                }
                where_clauses.push(format!("{} = ?", col));
                where_params.push(value_to_db_param(&resolve(tmpl)));
            }
            let mut patch: Map<String, Value> = Map::with_capacity(set_map.len());
            for (col, tmpl) in set_map {
                if !is_valid_identifier(col) {
                    return Err(format!("invalid column '{}' in set clause", col));
                }
                patch.insert(col.clone(), resolve(tmpl));
            }

            let Some((route, schema)) = find_schema_for_table(ctx.reg, name) else {
                // Table not managed by an entity: plain parameterized UPDATE.
                let mut params: Vec<DbParam> = patch.values().map(value_to_db_param).collect();
                params.extend(where_params);
                let sets: Vec<String> = patch.keys().map(|c| format!("{} = ?", c)).collect();
                let sql = rehydrate_placeholders(
                    &format!("UPDATE {} SET {} WHERE {}", name, sets.join(", "), where_clauses.join(" AND ")),
                    dialect,
                );
                let affected = tx
                    .raw_execute(&sql, params)
                    .await
                    .map_err(|e| format!("failed to update '{}': {}", name, e))?;
                if affected == 0 {
                    return Ok(false);
                }
                outcome.updated_rows += affected;
                outcome.touched_routes.push(name.to_string());
                outcome.primary_route.get_or_insert_with(|| name.to_string());
                outcome.last_route = Some(name.to_string());
                outcome.last_new_record.extend(patch);
                return Ok(true);
            };

            let table = schema.table.as_str();
            if !is_valid_identifier(table) {
                return Err(format!("invalid table name '{}' for route '{}'", table, route));
            }
            if has_column(schema, "deleted_at") && !where_map.contains_key("deleted_at") {
                where_clauses.push("deleted_at IS NULL".to_string());
            }
            let where_sql = where_clauses.join(" AND ");

            // Lock the candidate rows for the rest of the transaction.
            let (from_target, lock_clause) = match ctx.state.db_type {
                DbType::Postgres | DbType::Mysql => (table.to_string(), " FOR UPDATE"),
                DbType::Mssql => (format!("{} WITH (UPDLOCK, ROWLOCK)", table), ""),
                _ => (table.to_string(), ""),
            };
            let select_sql = rehydrate_placeholders(
                &format!("SELECT * FROM {} WHERE {}{}", from_target, where_sql, lock_clause),
                dialect,
            );
            let existing_rows = tx
                .raw_sql(&select_sql, where_params)
                .await
                .map_err(|e| format!("failed to read '{}': {}", table, e))?;

            let pk_cols: Vec<String> = if schema.primary_key.columns.is_empty() {
                vec!["id".to_string()]
            } else {
                schema.primary_key.columns.clone()
            };
            let version_col = schema
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case("version"))
                .map(|c| c.name.clone());
            let request_body = Value::Object(row.clone());
            let mut changed_any = false;

            for existing in &existing_rows {
                let Some(old) = existing.as_object() else { continue };
                // Idempotency: nothing to change on this record.
                if patch.iter().all(|(k, v)| same_value(old.get(k), v)) {
                    continue;
                }
                check_transition_guards(schema, old, &patch, Some(SYSTEM_ROLE))?;

                // SET <patch>, updated_at, updated_by_id, version + 1
                let mut sets: Vec<String> = patch.keys().map(|c| format!("{} = ?", c)).collect();
                let mut params: Vec<DbParam> = patch.values().map(value_to_db_param).collect();
                if has_column(schema, "updated_at") && !patch.contains_key("updated_at") {
                    sets.push(format!("updated_at = {}", ctx.state.query_converter.datetime_now));
                }
                if has_column(schema, "updated_by_id") && !patch.contains_key("updated_by_id") {
                    sets.push("updated_by_id = ?".to_string());
                    params.push(actor_param(schema, "updated_by_id", ctx.actor_id));
                }
                if let Some(vc) = &version_col
                    && !patch.contains_key(vc)
                {
                    sets.push(format!("{c} = COALESCE({c}, 0) + 1", c = vc));
                }
                let mut pk_parts = Vec::with_capacity(pk_cols.len());
                let mut pk_where = Vec::with_capacity(pk_cols.len());
                for pk in &pk_cols {
                    let v = old
                        .get(pk)
                        .ok_or_else(|| format!("row of '{}' has no primary key column '{}'", table, pk))?;
                    pk_where.push(format!("{} = ?", pk));
                    params.push(value_to_db_param(v));
                    pk_parts.push(json_val_to_str(v));
                }
                let pk_str = pk_parts.join(",");
                let sql = rehydrate_placeholders(
                    &format!("UPDATE {} SET {} WHERE {}", table, sets.join(", "), pk_where.join(" AND ")),
                    dialect,
                );
                let affected = tx
                    .raw_execute(&sql, params)
                    .await
                    .map_err(|e| format!("failed to update '{}' {}: {}", table, pk_str, e))?;
                if affected == 0 {
                    continue;
                }

                // Re-read the row so triggers and webhooks see exactly what was
                // stored (DB-side updated_at, version, column type coercion).
                let pk_params: Vec<DbParam> = pk_cols
                    .iter()
                    .filter_map(|pk| old.get(pk))
                    .map(value_to_db_param)
                    .collect();
                let reread_sql = rehydrate_placeholders(
                    &format!("SELECT * FROM {} WHERE {}", table, pk_where.join(" AND ")),
                    dialect,
                );
                let new_record = match tx.raw_sql(&reread_sql, pk_params).await {
                    Ok(rows) => rows.into_iter().next().and_then(|v| v.as_object().cloned()),
                    Err(e) => return Err(format!("failed to re-read '{}' {}: {}", table, pk_str, e)),
                }
                .unwrap_or_else(|| {
                    let mut m = old.clone();
                    m.extend(patch.clone());
                    m
                });

                // action_triggers on_update — same engine and audit as PUT.
                if !schema.action_triggers.is_empty() {
                    let trigger_ctx = crate::nocode::trigger_engine::TriggerContext {
                        parent_table: table,
                        parent_pk: &pk_str,
                        old_record: old,
                        new_record: &new_record,
                        request_body: &request_body,
                        actor_id: Some(ctx.actor_id),
                    };
                    let executed = crate::nocode::trigger_engine::execute_triggers(
                        ctx.state.db_type.clone(),
                        tx,
                        schema,
                        &trigger_ctx,
                        "on_update",
                    )
                    .await
                    .map_err(|e| format!("trigger failed on '{}' {}: {}", route, pk_str, e))?;
                    for trig_name in executed {
                        outcome.triggers_executed += 1;
                        audit(ctx, "TRIGGER", &format!("{}:{}", route, trig_name), Some(&pk_str), None, json!({}));
                    }
                }

                // Entity webhooks subscribed to on_update.
                if !schema.webhooks.is_empty() {
                    outcome.webhooks_enqueued += outbox::enqueue_in_tx(
                        tx,
                        schema,
                        route,
                        "on_update",
                        &pk_str,
                        Some(ctx.actor_id),
                        &new_record,
                        Some(old),
                    )
                    .await
                    .map_err(|e| format!("failed to queue on_update webhook for '{}': {}", route, e))?;
                }

                outcome.updated_rows += affected;
                if !changed_any {
                    outcome.last_old_record = Some(old.clone());
                    outcome.last_new_record = new_record;
                    if outcome.primary_route.is_none() {
                        outcome.primary_route = Some(route.to_string());
                        outcome.primary_id = Some(pk_str.clone());
                    }
                }
                changed_any = true;
            }

            if !changed_any {
                return Ok(false);
            }
            outcome.touched_routes.push(route.to_string());
            outcome.last_route = Some(route.to_string());
            Ok(true)
        }

        "webhook" => {
            let event = action
                .event
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&ctx.schedule.name);
            let id_str = outcome.primary_id.clone().unwrap_or_default();
            let target_ref = if !action.table.trim().is_empty() {
                Some(action.table.trim().to_string())
            } else {
                outcome.last_route.clone()
            };
            let target_schema = target_ref.as_deref().and_then(|t| find_schema_for_table(ctx.reg, t));

            if let Some(direct_url) = action.url.as_deref().map(str::trim).filter(|u| !u.is_empty()) {
                let route_name = target_schema
                    .map(|(r, _)| r.to_string())
                    .or(target_ref.clone())
                    .unwrap_or_else(|| ctx.schedule.name.clone());
                let table_name = target_schema.map(|(_, s)| s.table.clone()).unwrap_or_else(|| route_name.clone());
                outcome.webhooks_enqueued += outbox::enqueue_direct_in_tx(
                    tx,
                    &route_name,
                    &table_name,
                    event,
                    direct_url,
                    &action.headers,
                    &id_str,
                    Some(ctx.actor_id),
                    &outcome.last_new_record,
                    outcome.last_old_record.as_ref(),
                )
                .await
                .map_err(|e| format!("failed to enqueue webhook '{}': {}", event, e))?;
                return Ok(true);
            }

            match target_schema {
                Some((route_name, schema)) => {
                    outcome.webhooks_enqueued += outbox::enqueue_in_tx(
                        tx,
                        schema,
                        route_name,
                        event,
                        &id_str,
                        Some(ctx.actor_id),
                        &outcome.last_new_record,
                        outcome.last_old_record.as_ref(),
                    )
                    .await
                    .map_err(|e| format!("failed to enqueue webhook '{}' for '{}': {}", event, route_name, e))?;
                    Ok(true)
                }
                None => Err(format!(
                    "webhook '{}' has no target: set 'url', 'table', or place an update action before it",
                    event
                )),
            }
        }

        "insert" => {
            let name = action.table.trim();
            if name.is_empty() || !is_valid_identifier(name) {
                return Err(format!("invalid or empty table '{}' in insert action", name));
            }
            let fields_map = action
                .fields
                .as_ref()
                .filter(|m| !m.is_empty())
                .ok_or_else(|| format!("insert action on '{}' requires non-empty 'fields'", name))?;
            let schema_hit = find_schema_for_table(ctx.reg, name);
            let table = schema_hit.map(|(_, s)| s.table.as_str()).unwrap_or(name);
            if !is_valid_identifier(table) {
                return Err(format!("invalid table name '{}'", table));
            }

            let mut cols = Vec::with_capacity(fields_map.len() + 2);
            let mut values = Vec::with_capacity(fields_map.len() + 2);
            let mut params = Vec::with_capacity(fields_map.len() + 1);
            for (col, tmpl) in fields_map {
                if !is_valid_identifier(col) {
                    return Err(format!("invalid column '{}' in insert action", col));
                }
                cols.push(col.clone());
                values.push("?".to_string());
                params.push(value_to_db_param(&resolve(tmpl)));
            }
            if let Some((_, schema)) = schema_hit {
                if has_column(schema, "created_at") && !fields_map.contains_key("created_at") {
                    cols.push("created_at".into());
                    values.push(ctx.state.query_converter.datetime_now.clone());
                }
                if has_column(schema, "created_by_id") && !fields_map.contains_key("created_by_id") {
                    cols.push("created_by_id".into());
                    values.push("?".into());
                    params.push(actor_param(schema, "created_by_id", ctx.actor_id));
                }
            }
            let sql = rehydrate_placeholders(
                &format!("INSERT INTO {} ({}) VALUES ({})", table, cols.join(", "), values.join(", ")),
                dialect,
            );
            tx.raw_execute(&sql, params)
                .await
                .map_err(|e| format!("failed to insert into '{}': {}", table, e))?;
            outcome
                .touched_routes
                .push(schema_hit.map(|(r, _)| r.to_string()).unwrap_or_else(|| name.to_string()));
            Ok(true)
        }

        "sql" => {
            let stmt = action
                .statement
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "sql action requires non-empty 'statement'".to_string())?;
            let params: Vec<DbParam> = action
                .params
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|p| value_to_db_param(&resolve(&Value::String(p.clone()))))
                .collect();
            tx.raw_execute(&rehydrate_placeholders(stmt, dialect), params)
                .await
                .map_err(|e| format!("sql action failed: {}", e))?;
            if !action.table.trim().is_empty() {
                let name = action.table.trim();
                outcome.touched_routes.push(
                    find_schema_for_table(ctx.reg, name).map(|(r, _)| r.to_string()).unwrap_or_else(|| name.to_string()),
                );
            }
            Ok(true)
        }

        other => Err(format!("unsupported schedule action type '{}'", other)),
    }
}

// ── Background worker ────────────────────────────────────────────────────────

/// Removes the schedule from [`LOCAL_RUNNING`] when the cron task ends.
struct RunningGuard(String);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        LOCAL_RUNNING.remove(&self.0);
    }
}

/// One cron firing: lease → claim slot → run → release.
async fn fire_cron(state: &AppState, schedule: &ScheduleDef, slot: i64) {
    let lease = match acquire_lease(state, schedule).await {
        Ok(Some(l)) => l,
        Ok(None) => {
            // A previous (or manual) run is still going; retry next tick while
            // the slot is inside the catch-up window.
            let first = BUSY_WARNED.insert(schedule.name.clone(), slot) != Some(slot);
            if first {
                log_output("WARN", "SCHEDULER", &schedule.name, format!("slot {} waiting: previous run still holds the lease", slot), true);
            }
            return;
        }
        Err(e) => {
            log_output("WARN", "SCHEDULER", &schedule.name, e, true);
            return;
        }
    };

    match sequences::compare_and_set_below(state.store.as_ref(), &slot_scope(&schedule.name), slot, slot).await {
        Ok(true) => {
            LOCAL_LAST_SLOT.insert(schedule.name.clone(), slot);
            if let Err(e) = run_schedule(state, schedule, "cron", Some(slot), SYSTEM_ROLE, None).await {
                log_output("ERROR", "SCHEDULER", &schedule.name, e, true);
            }
        }
        Ok(false) => {
            // Already executed by another replica (or by us before a restart).
            LOCAL_LAST_SLOT.insert(schedule.name.clone(), slot);
        }
        Err(e) => log_output("WARN", "SCHEDULER", &schedule.name, format!("slot claim failed: {}", e), true),
    }
    lease.release(state).await;
}

/// Start the background cron worker.
///
/// Every `SCHEDULER_TICK_SECS` (default 15s) it evaluates all enabled
/// schedules of the live registry, finds the most recent due slot inside the
/// catch-up window, and fires it on a separate task (schedules never block
/// each other). Not started on MongoDB.
pub fn start_worker(state: Arc<AppState>) {
    if state.db_type == DbType::Mongodb {
        if !registry::current().schedules.is_empty() {
            log_output("WARN", "SCHEDULER", "worker", "schedules require a SQL backend; scheduler disabled on MongoDB".to_string(), true);
        }
        return;
    }
    let tick_secs = env_u64("SCHEDULER_TICK_SECS", 15).clamp(1, 30);
    log_output(
        "SCHEDULER",
        "START",
        "worker",
        format!("tick={}s schedules={}", tick_secs, registry::current().schedules.len()),
        true,
    );

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(tick_secs)).await;
            if crate::shutdown::is_shutting_down() {
                break;
            }
            let now = Utc::now();
            let reg = registry::snapshot();
            for sched in reg.schedules.iter().filter(|s| s.enabled) {
                let (Ok(tz), Ok(cron)) = (parse_timezone(&sched.timezone), CronExpr::parse(&sched.cron)) else {
                    continue;
                };
                let Some(due) = cron.latest_due(now, &tz, catch_up_window(sched)) else { continue };
                let slot = slot_id(due);
                if LOCAL_LAST_SLOT.get(&sched.name).is_some_and(|v| *v >= slot) {
                    continue;
                }
                if LOCAL_RUNNING.insert(sched.name.clone(), ()).is_some() {
                    continue;
                }
                let state = state.clone();
                let sched = sched.clone();
                tokio::spawn(async move {
                    let _inflight = crate::shutdown::InflightGuard::new();
                    let _running = RunningGuard(sched.name.clone());
                    fire_cron(&state, &sched, slot).await;
                });
            }
        }
    });
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{StateMachine, StateTransition};
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn cron_parse_and_match_jakarta() {
        let expr = CronExpr::parse("0 1 * * *").unwrap();
        let wib = parse_timezone("Asia/Jakarta").unwrap();
        // 01:00 WIB = 18:00 UTC the previous day
        assert!(expr.matches(&wib.to_local(utc(2026, 11, 19, 18, 0))));
        assert!(!expr.matches(&wib.to_local(utc(2026, 11, 19, 18, 1))));

        let next = expr.next_after(utc(2026, 11, 19, 18, 0), &wib).unwrap();
        assert_eq!(next.with_timezone(&Utc), utc(2026, 11, 20, 18, 0));
        assert_eq!(next.offset().local_minus_utc(), 7 * 3600);
    }

    #[test]
    fn cron_supports_steps_ranges_lists_and_macros() {
        let step = CronExpr::parse("*/15 9-17 * * MON-FRI").unwrap();
        let z = parse_timezone("UTC").unwrap();
        // 2026-11-20 is Friday, 2026-11-21 Saturday
        assert!(step.matches(&z.to_local(utc(2026, 11, 20, 9, 0))));
        assert!(step.matches(&z.to_local(utc(2026, 11, 20, 17, 45))));
        assert!(!step.matches(&z.to_local(utc(2026, 11, 20, 18, 0))));
        assert!(!step.matches(&z.to_local(utc(2026, 11, 21, 10, 0))));

        assert_eq!(CronExpr::parse("@daily").unwrap(), CronExpr::parse("0 0 * * *").unwrap());
        assert_eq!(CronExpr::parse("@hourly").unwrap(), CronExpr::parse("0 * * * *").unwrap());
        assert!(CronExpr::parse("bad cron").is_err());
        assert!(CronExpr::parse("60 0 * * *").is_err());
        assert!(CronExpr::parse("*/0 * * * *").is_err());
    }

    #[test]
    fn cron_star_step_in_dom_is_unrestricted_like_vixie() {
        // "*/2" in DOM is still a wildcard for the DOM/DOW OR-rule.
        let e = CronExpr::parse("0 0 */2 * MON").unwrap();
        assert!(e.dom_wildcard);
        let z = parse_timezone("UTC").unwrap();
        // 2026-11-23 is a Monday, day 23 (odd → in */2 starting at 1) → match
        assert!(e.matches(&z.to_local(utc(2026, 11, 23, 0, 0))));
        // 2026-11-30 is a Monday on an even day: with DOM unrestricted the
        // rule is AND, so no match (the old OR rule would have matched).
        assert!(!e.matches(&z.to_local(utc(2026, 11, 30, 0, 0))));
    }

    #[test]
    fn timezone_parser_handles_iana_aliases_and_offsets() {
        let off = |s: &str, at: DateTime<Utc>| parse_timezone(s).unwrap().to_local(at).offset().local_minus_utc();
        let jan = utc(2026, 1, 15, 12, 0);
        let jul = utc(2026, 7, 15, 12, 0);
        assert_eq!(off("Asia/Jakarta", jan), 7 * 3600);
        assert_eq!(off("WIB", jan), 7 * 3600);
        assert_eq!(off("WITA", jan), 8 * 3600);
        assert_eq!(off("WIT", jan), 9 * 3600);
        assert_eq!(off("UTC", jan), 0);
        assert_eq!(off("+07:00", jan), 7 * 3600);
        assert_eq!(off("-05:00", jan), -5 * 3600);
        assert_eq!(off("UTC+7", jan), 7 * 3600);
        // DST-aware IANA zones
        assert_eq!(off("Europe/Berlin", jan), 3600);
        assert_eq!(off("Europe/Berlin", jul), 2 * 3600);
        assert_eq!(off("America/New_York", jul), -4 * 3600);
        assert!(parse_timezone("Mars/Olympus").is_err());
    }

    #[test]
    fn next_after_and_latest_due_across_dst() {
        // Europe/Berlin springs forward on 2026-03-29 (02:00 → 03:00).
        let berlin = parse_timezone("Europe/Berlin").unwrap();
        let e = CronExpr::parse("30 1 * * *").unwrap();
        let next = e.next_after(utc(2026, 3, 28, 1, 0), &berlin).unwrap();
        // 01:30 CET on 29 March = 00:30 UTC
        assert_eq!(next.with_timezone(&Utc), utc(2026, 3, 29, 0, 30));
        let next2 = e.next_after(next.with_timezone(&Utc), &berlin).unwrap();
        // 01:30 CEST on 30 March = 23:30 UTC on 29 March
        assert_eq!(next2.with_timezone(&Utc), utc(2026, 3, 29, 23, 30));

        let wib = parse_timezone("Asia/Jakarta").unwrap();
        let daily = CronExpr::parse("0 1 * * *").unwrap();
        // Process was down at 01:00 WIB (18:00 UTC), started 01:40 WIB.
        let now = utc(2026, 11, 19, 18, 40);
        assert_eq!(daily.latest_due(now, &wib, 3600), Some(utc(2026, 11, 19, 18, 0)));
        assert_eq!(daily.latest_due(now, &wib, 0), None);
        assert_eq!(slot_id(utc(2026, 11, 19, 18, 0)), 202611191800);
    }

    #[test]
    fn select_sql_validation_blocks_writes_and_multistatement() {
        let sql = extract_select_sql(
            "SQL:SELECT id FROM plant_capas WHERE status = 'IMPLEMENTATION' AND deleted_at IS NULL",
        )
        .unwrap()
        .unwrap();
        assert!(sql.starts_with("SELECT id FROM plant_capas"));
        assert!(extract_select_sql("SQL:SELECT id FROM t WHERE note = 'UPDATE required'").is_ok());
        assert_eq!(extract_select_sql("").unwrap(), None);
        assert!(extract_select_sql("SQL:DELETE FROM plant_capas").is_err());
        assert!(extract_select_sql("SQL:SELECT id FROM t; DROP TABLE t").is_err());
        assert!(extract_select_sql("UPDATE plant_capas SET status = 'X'").is_err());
    }

    #[test]
    fn template_interpolation_preserves_types_and_interpolates_strings() {
        let tz = parse_timezone("Asia/Jakarta").unwrap();
        let mut row = Map::new();
        row.insert("id".into(), json!(42));
        row.insert("code".into(), json!("CAPA-001"));
        row.insert("meta".into(), json!({ "owner": "qa" }));

        assert_eq!(resolve_action_value(&json!("{row.id}"), &row, "capa_overdue", &tz), json!(42));
        assert_eq!(
            resolve_action_value(&json!("CAPA {row.code} by {schedule.name}"), &row, "capa_overdue", &tz),
            json!("CAPA CAPA-001 by capa_overdue")
        );
        assert_eq!(resolve_action_value(&json!("{row.meta.owner}"), &row, "x", &tz), json!("qa"));
        let today = tz.to_local(Utc::now()).format("%Y-%m-%d").to_string();
        assert_eq!(interpolate_string("{now:YYYY-MM-DD}", &row, "x", &tz), today);
        assert_eq!(interpolate_string("a {unclosed", &row, "x", &tz), "a {unclosed");
    }

    #[test]
    fn same_value_is_loose_for_db_roundtrips() {
        assert!(same_value(Some(&json!("OVERDUE")), &json!("OVERDUE")));
        assert!(same_value(Some(&json!(1)), &json!("1")));
        assert!(same_value(Some(&json!(1)), &json!(true)));
        assert!(!same_value(Some(&json!("IMPLEMENTATION")), &json!("OVERDUE")));
        assert!(!same_value(None, &json!("OVERDUE")));
        assert!(!same_value(Some(&Value::Null), &json!("")));
    }

    #[test]
    fn shared_guard_respects_system_role_and_locked_when() {
        let schema = TableSchema {
            table: "plant_capas".into(),
            state_machine: Some(StateMachine {
                field: "status".into(),
                initial: Some("DRAFT".into()),
                transitions: vec![
                    StateTransition { from: json!("IMPLEMENTATION"), to: "OVERDUE".into(), roles: vec!["system".into()] },
                    StateTransition { from: json!("DRAFT"), to: "CLOSED".into(), roles: vec!["qa_manager".into()] },
                ],
            }),
            ..Default::default()
        };
        let rec = |s: &str| {
            let mut m = Map::new();
            m.insert("status".into(), json!(s));
            m
        };
        assert_eq!(check_transition_guards(&schema, &rec("IMPLEMENTATION"), &rec("OVERDUE"), Some(SYSTEM_ROLE)), Ok(true));
        assert_eq!(check_transition_guards(&schema, &rec("OVERDUE"), &rec("OVERDUE"), Some(SYSTEM_ROLE)), Ok(false));
        let err = check_transition_guards(&schema, &rec("DRAFT"), &rec("CLOSED"), Some(SYSTEM_ROLE)).unwrap_err();
        assert!(err.contains("role 'system' is not permitted"), "{}", err);
        let err2 = check_transition_guards(&schema, &rec("CLOSED"), &rec("OVERDUE"), Some(SYSTEM_ROLE)).unwrap_err();
        assert!(err2.contains("Illegal state transition"), "{}", err2);
    }

    fn capa_schemas() -> std::collections::HashMap<String, Arc<TableSchema>> {
        let mut m = std::collections::HashMap::new();
        m.insert("capas".to_string(), Arc::new(TableSchema { table: "plant_capas".into(), ..Default::default() }));
        m
    }

    #[test]
    fn lint_schedules_catches_invalid_config_and_accepts_valid() {
        let valid: ScheduleDef = serde_json::from_value(json!({
            "name": "capa_overdue",
            "cron": "0 1 * * *",
            "timezone": "Asia/Jakarta",
            "select": "SQL:SELECT id FROM plant_capas WHERE status = 'IMPLEMENTATION' AND plan_due_date < CURRENT_DATE AND deleted_at IS NULL",
            "actions": [
                { "type": "update", "table": "plant_capas", "where": { "id": "{row.id}" }, "set": { "status": "OVERDUE" } },
                { "type": "webhook", "event": "capa_overdue" }
            ]
        }))
        .unwrap();
        let rep = crate::nocode::validate::lint_schedules(std::slice::from_ref(&valid), &capa_schemas());
        assert!(rep.is_clean(), "expected clean report, got {:?}", rep.errors);

        let mut bad = valid.clone();
        bad.cron = "invalid".into();
        bad.timezone = "Mars/Phobos".into();
        bad.select = "SQL:DELETE FROM plant_capas".into();
        let rep_bad = crate::nocode::validate::lint_schedules(&[valid, bad], &capa_schemas());
        assert!(!rep_bad.is_clean());
        assert!(rep_bad.errors.iter().any(|e| e.contains("duplicate schedule name")));
        assert!(rep_bad.errors.iter().any(|e| e.contains("must have 5 fields")));
        assert!(rep_bad.errors.iter().any(|e| e.contains("unsupported timezone")));
        assert!(rep_bad.errors.iter().any(|e| e.contains("read-only SELECT")));

        // Route name resolves too ("capas" → table plant_capas): no warning.
        let mut by_route = serde_json::from_value::<ScheduleDef>(json!({
            "name": "by_route", "cron": "@daily",
            "actions": [ { "type": "update", "table": "capas", "where": { "id": "{row.id}" }, "set": { "status": "X" } },
                         { "type": "webhook", "event": "e" } ]
        }))
        .unwrap();
        let rep = crate::nocode::validate::lint_schedules(std::slice::from_ref(&by_route), &capa_schemas());
        assert!(rep.is_clean() && rep.warnings.is_empty(), "{:?} {:?}", rep.errors, rep.warnings);

        // Unknown table → warning (entity rules not applied); webhook then has no target → error.
        by_route.actions[0].table = "legacy_table".into();
        let rep = crate::nocode::validate::lint_schedules(&[by_route], &capa_schemas());
        assert!(rep.warnings.iter().any(|w| w.contains("not a configured entity")), "{:?}", rep.warnings);
        assert!(rep.errors.iter().any(|e| e.contains("webhook has no target")), "{:?}", rep.errors);
    }
}
