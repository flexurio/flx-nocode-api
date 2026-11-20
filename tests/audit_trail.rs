//! End-to-end HTTP tests for CPOB Audit Trail (`flx_audit_trail`):
//! - Automatic creation of `flx_audit_trail` core table at startup
//! - Per-entity `audit_trail` config (`enabled`, `columns`, `require_reason_when`, `readers`)
//! - Column-level diff on PUT/PATCH (one row per changed tracked column; unchanged & untracked ignored)
//! - Mandatory `X-Change-Reason` enforcement (`400 Bad Request` + rollback when missing/blank)
//! - `actor_id`, `actor_custom`, `reason`, `request_id`, `at` capture
//! - Read-only `GET /audit_trail?record_id.eq=…&entity.eq=…` with standard filters
//! - RBAC: `ADMIN_ROLES` (full access) vs `audit_trail.readers` (entity-scoped) vs unauthorized (`401`/`403`)
//! - Immutability: `POST`/`PUT`/`PATCH`/`DELETE` on `/audit_trail` and `/audit_trail/{id}` return `405`

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::{Value, json};

const TEST_SECRET: &str = "cpob-audit-trail-test-secret-0123456789abcdef";

struct Server {
    child: Child,
    base: String,
    log_path: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking()
            && let Ok(log) = std::fs::read_to_string(&self.log_path)
        {
            eprintln!("---- server log ----\n{}\n--------------------", log);
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn mint_token(id: &str, name: &str, role: &str, custom: &str) -> String {
    let now = chrono::Utc::now().timestamp() as usize;
    let claims = flx_nocode_api::auth::Claims {
        id: id.to_string(),
        nm: name.to_string(),
        exp: now + 3600,
        at: now,
        rl: role.to_string(),
        cs: custom.to_string(),
        jti: Some(format!("jti-{}-{}", id, now)),
        typ: None,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap()
}

fn capa_entity_json() -> String {
    r#"{
  "table": "capa",
  "primary_key": { "columns": ["id"], "auto_increment": true },
  "columns": [
    { "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" },
    { "name": "status", "nullable": false, "type_data": "varchar(40)" },
    { "name": "problem", "nullable": true, "type_data": "text" },
    { "name": "evaluation", "nullable": true, "type_data": "text" },
    { "name": "notes", "nullable": true, "type_data": "text" },
    { "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" },
    { "name": "created_by_id", "nullable": true, "type_data": "int" },
    { "name": "updated_at", "nullable": true, "type_data": "timestamp" },
    { "name": "updated_by_id", "nullable": true, "type_data": "int" },
    { "name": "deleted_at", "nullable": true, "type_data": "timestamp" },
    { "name": "deleted_by_id", "nullable": true, "type_data": "int" }
  ],
  "audit_trail": {
    "enabled": true,
    "columns": ["status", "problem", "evaluation"],
    "require_reason_when": {
      "status": ["CLOSED", "IMPLEMENTATION"]
    },
    "readers": ["qa_reviewer"]
  },
  "redis": { "keys": [], "ttl": 0 },
  "get": {
    "enable_method": true,
    "columns": ["id", "status", "problem", "evaluation", "notes"],
    "parameters": ["id.eq", "status.eq"],
    "order_by": ["id"]
  },
  "post": { "enable_method": true, "columns": ["status*", "problem", "evaluation", "notes"] },
  "put": { "enable_method": true, "columns": ["status", "problem", "evaluation", "notes"] },
  "del": { "enable_method": true, "type_delete": "soft" },
  "auto_generate": true
}"#
    .to_string()
}

fn secret_log_entity_json() -> String {
    r#"{
  "table": "secret_log",
  "primary_key": { "columns": ["id"], "auto_increment": true },
  "columns": [
    { "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" },
    { "name": "title", "nullable": false, "type_data": "varchar(120)" },
    { "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" },
    { "name": "created_by_id", "nullable": true, "type_data": "int" },
    { "name": "updated_at", "nullable": true, "type_data": "timestamp" },
    { "name": "updated_by_id", "nullable": true, "type_data": "int" },
    { "name": "deleted_at", "nullable": true, "type_data": "timestamp" },
    { "name": "deleted_by_id", "nullable": true, "type_data": "int" }
  ],
  "audit_trail": {
    "enabled": true,
    "columns": ["title"],
    "readers": ["internal_auditor"]
  },
  "redis": { "keys": [], "ttl": 0 },
  "get": {
    "enable_method": true,
    "columns": ["id", "title"],
    "parameters": ["id.eq"],
    "order_by": ["id"]
  },
  "post": { "enable_method": true, "columns": ["title*"] },
  "put": { "enable_method": true, "columns": ["title"] },
  "del": { "enable_method": true, "type_delete": "soft" },
  "auto_generate": true
}"#
    .to_string()
}

fn rules_json() -> String {
    json!({
        "role": ["admin", "qa_reviewer", "operator"],
        "rule": [
            {
                "match": "/capa",
                "allows": [
                    { "method": "GET", "permission_id": "capa_get", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "qa_reviewer"] },
                        { "eq": ["$user.role", "operator"] }
                    ]}},
                    { "method": "POST", "permission_id": "capa_post", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "qa_reviewer"] }
                    ]}}
                ]
            },
            {
                "match": "/capa/{id}",
                "allows": [
                    { "method": "PUT", "permission_id": "capa_put", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "qa_reviewer"] }
                    ]}},
                    { "method": "PATCH", "permission_id": "capa_patch", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "qa_reviewer"] }
                    ]}},
                    { "method": "DELETE", "permission_id": "capa_del", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "qa_reviewer"] }
                    ]}}
                ]
            },
            {
                "match": "/secret_log",
                "allows": [
                    { "method": "GET", "permission_id": "sl_get", "if": { "eq": ["$user.role", "admin"] }},
                    { "method": "POST", "permission_id": "sl_post", "if": { "eq": ["$user.role", "admin"] }}
                ]
            },
            {
                "match": "/secret_log/{id}",
                "allows": [
                    { "method": "PUT", "permission_id": "sl_put", "if": { "eq": ["$user.role", "admin"] }}
                ]
            }
        ]
    })
    .to_string()
}

async fn start_server() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("flx-audit-trail-{}-{}", std::process::id(), port));
    let cfg = dir.join("config").join("entity");
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::create_dir_all(dir.join("static")).unwrap();
    std::fs::create_dir_all(dir.join("seed")).unwrap();

    let routes_json = json!({
        "routes": ["flx_users", "flx_roles", "capa", "secret_log"],
        "route_publics": []
    });
    std::fs::write(
        dir.join("config").join("routes.json"),
        serde_json::to_string_pretty(&routes_json).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("config").join("rules.json"), rules_json()).unwrap();
    std::fs::write(cfg.join("capa.json"), capa_entity_json()).unwrap();
    std::fs::write(cfg.join("secret_log.json"), secret_log_entity_json()).unwrap();

    let db_path = dir.join("test.db");
    let log_path = dir.join("server.log");
    let log_file = std::fs::File::create(&log_path).unwrap();
    let log_err = log_file.try_clone().unwrap();

    let bin = env!("CARGO_BIN_EXE_flx-nocode-api");
    let child = Command::new(bin)
        .current_dir(&dir)
        .env("DB_TYPE", "sqlite")
        .env("SQLITE_URL", format!("sqlite://{}", db_path.display()))
        .env("LOC_CONFIG", dir.join("config"))
        .env("LOC_STATIC", dir.join("static"))
        .env("LOC_SEED", dir.join("seed"))
        .env("LOC_AUDIT", dir.join("audit.log"))
        .env("SECRET_KEY", TEST_SECRET)
        .env("ENCRYPT_KEY", "0123456789abcdef0123456789abcdef")
        .env("PORT", port.to_string())
        .env("HOST", "127.0.0.1")
        .env("REQUIRE_AUTH", "true")
        .env("ADMIN_ROLES", "admin,Super Admin")
        .env("RATE_LIMIT_ALL_PER_SEC", "0")
        .env("OUTBOX_ENABLED", "false")
        .env("SCHEDULER_ENABLED", "false")
        .env("DEBUG", "false")
        .env("LOGGING", "false")
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("spawn flx-nocode-api");

    let base = format!("http://127.0.0.1:{}", port);
    let srv = Server {
        child,
        base: base.clone(),
        log_path: log_path.clone(),
    };
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(r) = client.get(format!("{}/livez", base)).send().await
            && r.status().is_success()
        {
            return srv;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    panic!("server did not start in time. Log:\n{}", log);
}

#[tokio::test]
async fn cpob_audit_trail_end_to_end() {
    let srv = start_server().await;
    let client = reqwest::Client::new();

    let admin_tok = mint_token("1", "Admin", "admin/127", "dept:IT");
    let qa_tok = mint_token("42", "QA Reviewer", "qa_reviewer/127", "dept:QA;site:JKT");
    let op_tok = mint_token("99", "Operator", "operator/127", "dept:PROD");

    // 1. POST /capa creates record and records POST audit rows
    let create_res = client
        .post(format!("{}/capa", srv.base))
        .bearer_auth(&qa_tok)
        .header("X-Request-Id", "req-create-001")
        .json(&json!({
            "status": "OPEN",
            "problem": "Initial deviation",
            "notes": "internal scratch"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_res.status(), 200, "POST /capa should succeed");
    let create_body: Value = create_res.json().await.unwrap();
    let capa_id = create_body["data"]["id"].to_string().trim_matches('"').to_string();

    // Also create a secret_log record as admin so we can verify entity-scoped readers
    let sl_res = client
        .post(format!("{}/secret_log", srv.base))
        .bearer_auth(&admin_tok)
        .json(&json!({ "title": "Confidential finding" }))
        .send()
        .await
        .unwrap();
    assert_eq!(sl_res.status(), 200);

    // 2. PUT /capa/{id} changing `problem` (tracked) + `notes` (untracked), keeping `status` unchanged
    //    No X-Change-Reason required because `status` did not change.
    let put_res = client
        .put(format!("{}/capa/{}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .header("X-Request-Id", "req-put-002")
        .json(&json!({
            "status": "OPEN",
            "problem": "Refined deviation description",
            "notes": "updated scratch"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), 200, "PUT without status change should succeed without reason");

    // Verify exactly 1 PUT audit row (only `problem` changed; `status` unchanged, `notes` untracked)
    let put_audit: Value = client
        .get(format!(
            "{}/audit_trail?entity.eq=capa&record_id.eq={}&action.eq=PUT",
            srv.base, capa_id
        ))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let put_rows = put_audit["data"].as_array().unwrap();
    assert_eq!(put_rows.len(), 1, "expected 1 PUT audit row, got {:?}", put_rows);
    assert_eq!(put_rows[0]["column_name"], "problem");
    assert_eq!(put_rows[0]["old_value"], "Initial deviation");
    assert_eq!(put_rows[0]["new_value"], "Refined deviation description");
    assert_eq!(put_rows[0]["actor_id"], "42");
    assert_eq!(put_rows[0]["actor_custom"], "dept:QA;site:JKT");
    assert_eq!(put_rows[0]["request_id"], "req-put-002");
    assert!(put_rows[0]["reason"].is_null());
    assert!(!put_rows[0]["at"].as_str().unwrap().is_empty());

    // 3. PATCH /capa/{id} changing status -> IMPLEMENTATION WITHOUT X-Change-Reason -> 400
    let missing_reason = client
        .patch(format!("{}/capa/{}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_reason.status(), 400, "missing X-Change-Reason must return 400");

    // Whitespace-only X-Change-Reason -> 400
    let blank_reason = client
        .patch(format!("{}/capa/{}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .header("X-Change-Reason", "   ")
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(blank_reason.status(), 400, "blank X-Change-Reason must return 400");

    // Verify status is STILL OPEN (transaction rolled back)
    let check_capa: Value = client
        .get(format!("{}/capa?id.eq={}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(check_capa["data"][0]["status"], "OPEN");

    // 4. PATCH /capa/{id} changing status -> IMPLEMENTATION WITH X-Change-Reason -> 200
    let ok_patch = client
        .patch(format!("{}/capa/{}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .header("X-Change-Reason", "Approved by QA Manager")
        .header("X-Request-Id", "req-patch-003")
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_patch.status(), 200);

    let patch_audit: Value = client
        .get(format!(
            "{}/audit_trail?entity.eq=capa&record_id.eq={}&action.eq=PATCH",
            srv.base, capa_id
        ))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let patch_rows = patch_audit["data"].as_array().unwrap();
    assert_eq!(patch_rows.len(), 1);
    assert_eq!(patch_rows[0]["entity"], "capa");
    assert_eq!(patch_rows[0]["table_name"], "capa");
    assert_eq!(patch_rows[0]["record_id"], capa_id);
    assert_eq!(patch_rows[0]["action"], "PATCH");
    assert_eq!(patch_rows[0]["column_name"], "status");
    assert_eq!(patch_rows[0]["old_value"], "OPEN");
    assert_eq!(patch_rows[0]["new_value"], "IMPLEMENTATION");
    assert_eq!(patch_rows[0]["reason"], "Approved by QA Manager");
    assert_eq!(patch_rows[0]["request_id"], "req-patch-003");

    // 5. PUT changing two tracked columns (status -> CLOSED, evaluation -> Effective)
    let close_res = client
        .put(format!("{}/capa/{}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .header("X-Change-Reason", "CAPA effectiveness verified")
        .header("X-Request-Id", "req-close-004")
        .json(&json!({
            "status": "CLOSED",
            "evaluation": "Effective"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(close_res.status(), 200);

    let close_audit: Value = client
        .get(format!(
            "{}/audit_trail?request_id.eq=req-close-004&count=true",
            srv.base
        ))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(close_audit["total_data"], 2);
    let close_rows = close_audit["data"].as_array().unwrap();
    assert_eq!(close_rows.len(), 2);
    assert!(close_rows.iter().any(|r| r["column_name"] == "status" && r["old_value"] == "IMPLEMENTATION" && r["new_value"] == "CLOSED"));
    assert!(close_rows.iter().any(|r| r["column_name"] == "evaluation" && r["old_value"].is_null() && r["new_value"] == "Effective"));

    // 6. RBAC on GET /audit_trail
    // 6a. Unauthenticated -> 401
    let unauth = client
        .get(format!("{}/audit_trail", srv.base))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    // 6b. Operator (not admin, not in any readers) -> 403
    let op_res = client
        .get(format!("{}/audit_trail", srv.base))
        .bearer_auth(&op_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(op_res.status(), 403);

    // 6c. QA reviewer requesting secret_log -> 403
    let qa_secret = client
        .get(format!("{}/audit_trail?entity.eq=secret_log", srv.base))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(qa_secret.status(), 403);

    // 6d. QA reviewer requesting unfiltered /audit_trail -> only sees `capa` rows, never `secret_log`
    let qa_all: Value = client
        .get(format!("{}/audit_trail", srv.base))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let qa_all_rows = qa_all["data"].as_array().unwrap();
    assert!(!qa_all_rows.is_empty());
    assert!(
        qa_all_rows.iter().all(|r| r["entity"] == "capa"),
        "qa_reviewer must only see capa audit rows, got {:?}",
        qa_all_rows
    );

    // 6e. Admin sees BOTH `capa` and `secret_log`
    let admin_all: Value = client
        .get(format!("{}/audit_trail", srv.base))
        .bearer_auth(&admin_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let admin_rows = admin_all["data"].as_array().unwrap();
    assert!(admin_rows.iter().any(|r| r["entity"] == "capa"));
    assert!(admin_rows.iter().any(|r| r["entity"] == "secret_log"));
    let count_before_mut = admin_rows.len();

    // 7. Immutability: audit rows can NEVER be created/updated/deleted via API
    let post_at = client
        .post(format!("{}/audit_trail", srv.base))
        .bearer_auth(&admin_tok)
        .json(&json!({ "entity": "capa" }))
        .send()
        .await
        .unwrap();
    assert_eq!(post_at.status(), 405, "POST /audit_trail must be 405");

    let put_at = client
        .put(format!("{}/audit_trail/1", srv.base))
        .bearer_auth(&admin_tok)
        .json(&json!({ "reason": "tampered" }))
        .send()
        .await
        .unwrap();
    assert_eq!(put_at.status(), 405, "PUT /audit_trail/1 must be 405");

    let patch_at = client
        .patch(format!("{}/audit_trail/1", srv.base))
        .bearer_auth(&admin_tok)
        .json(&json!({ "reason": "tampered" }))
        .send()
        .await
        .unwrap();
    assert_eq!(patch_at.status(), 405, "PATCH /audit_trail/1 must be 405");

    let del_at = client
        .delete(format!("{}/audit_trail/1", srv.base))
        .bearer_auth(&admin_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(del_at.status(), 405, "DELETE /audit_trail/1 must be 405");

    // Verify audit trail row count is completely intact
    let admin_after: Value = client
        .get(format!("{}/audit_trail", srv.base))
        .bearer_auth(&admin_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(admin_after["data"].as_array().unwrap().len(), count_before_mut);

    // 8. DELETE /capa/{id} records DELETE audit rows
    let del_capa = client
        .delete(format!("{}/capa/{}", srv.base, capa_id))
        .bearer_auth(&qa_tok)
        .header("X-Change-Reason", "Duplicate CAPA removed")
        .send()
        .await
        .unwrap();
    assert_eq!(del_capa.status(), 200);

    let del_audit: Value = client
        .get(format!(
            "{}/audit_trail?entity.eq=capa&record_id.eq={}&action.eq=DELETE",
            srv.base, capa_id
        ))
        .bearer_auth(&qa_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let del_rows = del_audit["data"].as_array().unwrap();
    assert!(!del_rows.is_empty());
    assert!(del_rows.iter().all(|r| r["new_value"].is_null() && r["reason"] == "Duplicate CAPA removed"));
}
