//! End-to-end HTTP tests for GxP Electronic Signatures (CPOB / 21 CFR Part 11):
//! 1. `POST /auth/verify-password` (Bearer required, isolated per-user rate limit,
//!    non-leaking 401 on wrong password, short-lived `signature_token` JWT that
//!    cannot be used as a Bearer access token).
//! 2. `state_machine.transitions[].require_signature` on `PATCH /{route}/{id}`:
//!    - Missing `X-Signature-Token` -> 403 `Signature required`
//!    - Another user's `X-Signature-Token` -> 403 `Signature required`
//!    - Valid `X-Signature-Token` -> 200, auto-populates `by_column` (NIP via
//!      `actor_claim: "cs"`), `at_column` (server timestamp), `meaning_column`
//!      (from token, ignoring spoofed body values), passes `locked_when`, writes
//!      an immutable `SIGN` row to `flx_audit_trail`, and consumes the token
//!      (single-use: replay on a second record -> 403).

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::{Value, json};

const TEST_SECRET: &str = "cpob-esign-e2e-test-secret-0123456789abcdef";

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
  "table": "plant_capas",
  "primary_key": { "columns": ["id"], "auto_increment": true },
  "columns": [
    { "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" },
    { "name": "code", "nullable": false, "type_data": "varchar(40)" },
    { "name": "status", "nullable": false, "type_data": "varchar(40)" },
    { "name": "prepared_by", "nullable": true, "type_data": "varchar(40)" },
    { "name": "approved_meaning", "nullable": true, "type_data": "varchar(64)" },
    { "name": "approved_by", "nullable": true, "type_data": "varchar(40)" },
    { "name": "approved_at", "nullable": true, "type_data": "varchar(40)" },
    { "name": "version", "nullable": true, "type_data": "int" },
    { "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" },
    { "name": "created_by_id", "nullable": true, "type_data": "int" },
    { "name": "updated_at", "nullable": true, "type_data": "timestamp" },
    { "name": "updated_by_id", "nullable": true, "type_data": "int" }
  ],
  "locked_when": {
    "status": ["EVALUATION_APPROVAL"]
  },
  "state_machine": {
    "field": "status",
    "initial": "EVALUATION_APPROVAL",
    "actor_claim": "cs",
    "transitions": [
      {
        "from": "EVALUATION_APPROVAL",
        "to": "IMPLEMENTATION",
        "roles": ["plant_qa_manager"],
        "not_same_as": ["prepared_by"],
        "require_signature": {
          "meaning_column": "approved_meaning",
          "by_column": "approved_by",
          "at_column": "approved_at"
        }
      }
    ]
  },
  "audit_trail": {
    "enabled": true,
    "columns": ["status", "approved_meaning", "approved_by", "approved_at"],
    "readers": ["plant_qa_manager"]
  },
  "redis": { "keys": [], "ttl": 0 },
  "get": {
    "enable_method": true,
    "columns": ["id", "code", "status", "prepared_by", "approved_meaning", "approved_by", "approved_at", "version"],
    "parameters": ["id.eq"],
    "order_by": ["id"]
  },
  "post": {
    "enable_method": true,
    "columns": ["code*", "status*", "prepared_by"]
  },
  "put": {
    "enable_method": true,
    "columns": ["status"]
  },
  "auto_generate": true
}"#
    .to_string()
}

fn rules_json() -> String {
    json!({
        "role": ["admin", "plant_qa_manager"],
        "rule": [
            {
                "match": "/capas",
                "allows": [
                    { "method": "GET", "permission_id": "capa_get", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}},
                    { "method": "POST", "permission_id": "capa_post", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}}
                ]
            },
            {
                "match": "/capas/{id}",
                "allows": [
                    { "method": "PUT", "permission_id": "capa_put", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}},
                    { "method": "PATCH", "permission_id": "capa_patch", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}}
                ]
            },
            {
                "match": "/flx_users",
                "allows": [
                    { "method": "POST", "permission_id": "users_post", "if": { "eq": ["$user.role", "admin"] } },
                    { "method": "GET", "permission_id": "users_get", "if": { "eq": ["$user.role", "admin"] } }
                ]
            }
        ]
    })
    .to_string()
}

async fn start_server() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("flx-esign-{}-{}", std::process::id(), port));
    let cfg = dir.join("config").join("entity");
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::create_dir_all(dir.join("static")).unwrap();
    std::fs::create_dir_all(dir.join("seed")).unwrap();

    let routes_json = json!({
        "routes": ["flx_users", "flx_roles", "capas"],
        "route_publics": []
    });
    std::fs::write(
        dir.join("config").join("routes.json"),
        serde_json::to_string_pretty(&routes_json).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("config").join("rules.json"), rules_json()).unwrap();
    std::fs::write(cfg.join("capas.json"), capa_entity_json()).unwrap();

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
        .env("ADMIN_ROLES", "admin")
        .env("RATE_LIMIT_ALL_PER_SEC", "0")
        .env("RATE_LIMIT_LOGIN_PER_MIN", "3")
        .env("RATE_LIMIT_VERIFY_PASSWORD_PER_MIN", "5")
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
    panic!("server did not start in time:\n{}", log);
}

#[tokio::test]
async fn electronic_signature_verify_password_and_state_machine_e2e() {
    let srv = start_server().await;
    let client = reqwest::Client::new();

    // Seed two real users via POST /register so their Argon2 hashes live in flx_users.
    for (name, email, phone, pw) in [
        ("QA Manager", "qa@cpob.local", "0811000001", "QaSecret#2026"),
        ("Other QA", "other@cpob.local", "0811000002", "OtherSecret#2026"),
    ] {
        let boundary = "----FlxEsignTestBoundary";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\n{name}\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"email\"\r\n\r\n{email}\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"phone\"\r\n\r\n{phone}\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"password\"\r\n\r\n{pw}\r\n\
             --{b}--\r\n",
            b = boundary,
            name = name,
            email = email,
            phone = phone,
            pw = pw
        );
        let r = client
            .post(format!("{}/register", srv.base))
            .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
            .body(body)
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success(), "register {} failed: {}", email, r.status());
    }

    // Read the assigned user IDs by logging in once per user (also verifies /login works).
    let login_id = |email: &str, pw: &str| {
        let client = client.clone();
        let base = srv.base.clone();
        let email = email.to_string();
        let pw = pw.to_string();
        async move {
            let basic = base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", email, pw));
            let resp: Value = client
                .post(format!("{}/login", base))
                .header("Authorization", format!("Basic {}", basic))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(resp["success"], true, "login {} failed: {}", email, resp);
            let jwt = resp["data"].as_str().unwrap();
            let payload_b64 = jwt.split('.').nth(1).unwrap();
            let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload_b64)
                .unwrap();
            let claims: Value = serde_json::from_slice(&payload_bytes).unwrap();
            claims["id"].as_str().unwrap().to_string()
        }
    };

    let admin_token = mint_token("1", "Admin", "admin", "NIP-ADMIN");
    let qa_id = login_id("qa@cpob.local", "QaSecret#2026").await;
    let other_id = login_id("other@cpob.local", "OtherSecret#2026").await;

    let qa_token = mint_token(&qa_id, "QA Manager", "plant_qa_manager", "001982");
    let other_token = mint_token(&other_id, "Other QA", "plant_qa_manager", "002001");

    // 1. POST /auth/verify-password without Bearer -> 401
    let r = client
        .post(format!("{}/auth/verify-password", srv.base))
        .json(&json!({ "password": "QaSecret#2026", "meaning": "APPROVED" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // 2. Wrong password -> 401 without leaking reason
    let r = client
        .post(format!("{}/auth/verify-password", srv.base))
        .bearer_auth(&qa_token)
        .json(&json!({ "password": "wrong-password", "meaning": "APPROVED" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let body_wrong: Value = r.json().await.unwrap();
    assert_eq!(body_wrong["success"], false);
    let msg_wrong = body_wrong["message"].as_str().unwrap_or("");
    assert!(!msg_wrong.to_lowercase().contains("user"), "must not leak reason: {}", msg_wrong);

    // 3. Valid password -> 200 with signature_token
    let r = client
        .post(format!("{}/auth/verify-password", srv.base))
        .bearer_auth(&qa_token)
        .json(&json!({ "password": "QaSecret#2026", "meaning": "APPROVED" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body_ok: Value = r.json().await.unwrap();
    assert_eq!(body_ok["success"], true);
    let sig_token = body_ok["data"]["signature_token"].as_str().expect("signature_token").to_string();
    assert!(!sig_token.is_empty());

    // 4. Signature token cannot be used as Bearer access token on normal API -> 401
    let r = client
        .get(format!("{}/capas", srv.base))
        .bearer_auth(&sig_token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "signature_token must not work as Bearer access token");

    // 5. Create two CAPA records in EVALUATION_APPROVAL
    let mut capa_ids = Vec::new();
    for code in ["CAPA-ESIGN-001", "CAPA-ESIGN-002"] {
        let created: Value = client
            .post(format!("{}/capas", srv.base))
            .bearer_auth(&admin_token)
            .json(&json!({
                "code": code,
                "status": "EVALUATION_APPROVAL",
                "prepared_by": "009999"
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(created["success"], true, "{}", created);
        capa_ids.push(created["data"]["id"].as_i64().unwrap());
    }

    // 6. PATCH without X-Signature-Token -> 403 Signature required
    let r = client
        .patch(format!("{}/capas/{}", srv.base, capa_ids[0]))
        .bearer_auth(&qa_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let err_no_sig: Value = r.json().await.unwrap();
    assert!(
        err_no_sig["message"].as_str().unwrap_or("").contains("Signature required"),
        "expected 'Signature required', got: {}",
        err_no_sig
    );

    // 7. PATCH with another user's X-Signature-Token -> 403 Signature required
    let r = client
        .patch(format!("{}/capas/{}", srv.base, capa_ids[0]))
        .bearer_auth(&other_token)
        .header("X-Signature-Token", &sig_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "token from user A must not sign for user B");

    // 8. PATCH with valid X-Signature-Token for same user -> 200 & auto-fills columns
    let r = client
        .patch(format!("{}/capas/{}", srv.base, capa_ids[0]))
        .bearer_auth(&qa_token)
        .header("X-Signature-Token", &sig_token)
        .json(&json!({
            "status": "IMPLEMENTATION",
            "approved_by": "SPOOFED",
            "approved_meaning": "SPOOFED"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let patched: Value = r.json().await.unwrap();
    assert_eq!(patched["success"], true);
    assert_eq!(patched["data"]["status"], "IMPLEMENTATION");
    assert_eq!(patched["data"]["approved_by"], "001982");
    assert_eq!(patched["data"]["approved_meaning"], "APPROVED");
    assert!(
        !patched["data"]["approved_at"].as_str().unwrap_or("").is_empty(),
        "approved_at should be server-populated: {}",
        patched
    );

    // Verify persisted row in DB
    let fetched: Value = client
        .get(format!("{}/capas?id.eq={}", srv.base, capa_ids[0]))
        .bearer_auth(&qa_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = &fetched["data"][0];
    assert_eq!(row["status"], "IMPLEMENTATION");
    assert_eq!(row["approved_by"], "001982");
    assert_eq!(row["approved_meaning"], "APPROVED");
    assert!(!row["approved_at"].as_str().unwrap_or("").is_empty());

    // 9. Verify SIGN row was written to flx_audit_trail
    let audit: Value = client
        .get(format!(
            "{}/audit_trail?entity.eq=capas&record_id.eq={}&action.eq=SIGN",
            srv.base, capa_ids[0]
        ))
        .bearer_auth(&qa_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(audit["success"], true, "{}", audit);
    let audit_rows = audit["data"].as_array().expect("audit rows");
    assert_eq!(audit_rows.len(), 1, "expected 1 SIGN audit row: {}", audit);
    assert_eq!(audit_rows[0]["action"], "SIGN");
    assert_eq!(audit_rows[0]["column_name"], "status");
    assert_eq!(audit_rows[0]["old_value"], "EVALUATION_APPROVAL");
    assert_eq!(audit_rows[0]["new_value"], "IMPLEMENTATION");
    assert_eq!(audit_rows[0]["actor_id"], qa_id);
    assert_eq!(audit_rows[0]["actor_custom"], "001982");
    assert_eq!(audit_rows[0]["reason"], "APPROVED");

    // 10. Single-use: replaying the consumed signature_token on a second CAPA -> 403
    let r = client
        .patch(format!("{}/capas/{}", srv.base, capa_ids[1]))
        .bearer_auth(&qa_token)
        .header("X-Signature-Token", &sig_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "consumed signature token must not be replayable");

    // 11. Dedicated per-user rate limit on /auth/verify-password (limit=5) does NOT affect /login
    // We already made 2 calls for qa_token above; 3 more succeed/401, 6th hits 429.
    for _ in 0..3 {
        let r = client
            .post(format!("{}/auth/verify-password", srv.base))
            .bearer_auth(&qa_token)
            .json(&json!({ "password": "QaSecret#2026", "meaning": "APPROVED" }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    let r_429 = client
        .post(format!("{}/auth/verify-password", srv.base))
        .bearer_auth(&qa_token)
        .json(&json!({ "password": "QaSecret#2026", "meaning": "APPROVED" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r_429.status(), 429, "expected 429 after 5 verify-password calls");

    // Other user is unaffected (per-user bucket)
    let r_other = client
        .post(format!("{}/auth/verify-password", srv.base))
        .bearer_auth(&other_token)
        .json(&json!({ "password": "OtherSecret#2026", "meaning": "REVIEWED" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r_other.status(), 200, "other user must have independent rate limit bucket");

    // POST /login is completely unaffected by verify-password calls
    let basic = base64::engine::general_purpose::STANDARD.encode("qa@cpob.local:QaSecret#2026");
    let r_login = client
        .post(format!("{}/login", srv.base))
        .header("Authorization", format!("Basic {}", basic))
        .send()
        .await
        .unwrap();
    assert_eq!(r_login.status(), 200, "POST /login must not share rate limit with verify-password");
}
