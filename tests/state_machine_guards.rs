//! End-to-end HTTP tests for per-record `state_machine` transition guards:
//! - `transitions[].actor_column` (matching caller JWT `id` or `cs` via `actor_claim`)
//! - `transitions[].not_same_as` (separation of duties, HTTP 403 Forbidden)
//! - Anti-spoofing: overwriting `pic_nip` or `prepared_by` in the same PUT/PATCH
//!   cannot bypass guards because evaluation runs against the `FOR UPDATE` row.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::{Value, json};

const TEST_SECRET: &str = "cpob-sm-record-guards-test-secret-0123456789abcdef";

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
    { "name": "pic_nip", "nullable": true, "type_data": "varchar(40)" },
    { "name": "prepared_by", "nullable": true, "type_data": "varchar(40)" },
    { "name": "owner_id", "nullable": true, "type_data": "int" },
    { "name": "version", "nullable": true, "type_data": "int" },
    { "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" },
    { "name": "created_by_id", "nullable": true, "type_data": "int" },
    { "name": "updated_at", "nullable": true, "type_data": "timestamp" },
    { "name": "updated_by_id", "nullable": true, "type_data": "int" }
  ],
  "state_machine": {
    "field": "status",
    "initial": "ROOT_CAUSE_ANALYSIS",
    "transitions": [
      {
        "from": "ROOT_CAUSE_ANALYSIS",
        "to": "ACTION_PLANNING",
        "actor_column": "pic_nip",
        "actor_claim": "cs"
      },
      {
        "from": "ACTION_PLANNING",
        "to": "EVALUATION_APPROVAL",
        "actor_column": "owner_id"
      },
      {
        "from": "EVALUATION_APPROVAL",
        "to": "IMPLEMENTATION",
        "roles": ["plant_qa_manager"],
        "actor_claim": "cs",
        "not_same_as": ["prepared_by", "pic_nip"]
      }
    ]
  },
  "redis": { "keys": [], "ttl": 0 },
  "get": {
    "enable_method": true,
    "columns": ["id", "code", "status", "pic_nip", "prepared_by", "owner_id", "version"],
    "parameters": ["id.eq"],
    "order_by": ["id"]
  },
  "post": {
    "enable_method": true,
    "columns": ["code*", "status*", "pic_nip", "prepared_by", "owner_id"]
  },
  "put": {
    "enable_method": true,
    "columns": ["status", "pic_nip", "prepared_by", "owner_id"]
  },
  "auto_generate": true
}"#
    .to_string()
}

fn rules_json() -> String {
    json!({
        "role": ["admin", "staff", "plant_qa_manager"],
        "rule": [
            {
                "match": "/capas",
                "allows": [
                    { "method": "GET", "permission_id": "capa_get", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "staff"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}},
                    { "method": "POST", "permission_id": "capa_post", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "staff"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}}
                ]
            },
            {
                "match": "/capas/{id}",
                "allows": [
                    { "method": "PUT", "permission_id": "capa_put", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "staff"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}},
                    { "method": "PATCH", "permission_id": "capa_patch", "if": { "or": [
                        { "eq": ["$user.role", "admin"] },
                        { "eq": ["$user.role", "staff"] },
                        { "eq": ["$user.role", "plant_qa_manager"] }
                    ]}}
                ]
            }
        ]
    })
    .to_string()
}

async fn start_server() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("flx-sm-guards-{}-{}", std::process::id(), port));
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
async fn state_machine_actor_column_and_not_same_as_e2e() {
    let srv = start_server().await;
    let client = reqwest::Client::new();

    let pic_token = mint_token("101", "PIC Budi", "staff", "NIP-PIC-01");
    let other_staff_token = mint_token("102", "Other Staff", "staff", "NIP-OTHER-02");
    let preparer_qa_token = mint_token("201", "Preparer QA", "plant_qa_manager", "NIP-PREP-01");
    let pic_qa_token = mint_token("101", "PIC QA", "plant_qa_manager", "NIP-PIC-01");
    let independent_qa_token = mint_token("202", "Independent QA", "plant_qa_manager", "NIP-QA-99");
    let owner_token = mint_token("301", "Owner", "staff", "NIP-OWN-01");

    // 0. state_machine.initial: POST cannot create a record mid-workflow (400),
    //    which would otherwise skip every transition guard below.
    let r = client
        .post(format!("{}/capas", srv.base))
        .bearer_auth(&preparer_qa_token)
        .json(&json!({
            "code": "CAPA-2026-000",
            "status": "IMPLEMENTATION",
            "pic_nip": "NIP-PIC-01",
            "prepared_by": "NIP-PREP-01"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "POST outside initial state must get 400");

    //    Omitted status (even though `status*` is required) is filled with `initial`.
    let filled: Value = client
        .post(format!("{}/capas", srv.base))
        .bearer_auth(&preparer_qa_token)
        .json(&json!({ "code": "CAPA-2026-00X", "pic_nip": "NIP-PIC-01" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filled["success"], true, "create without status failed: {}", filled);
    let filled_id = filled["data"]["id"].as_i64().unwrap();
    let row: Value = client
        .get(format!("{}/capas?id.eq={}", srv.base, filled_id))
        .bearer_auth(&pic_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(row["data"][0]["status"], "ROOT_CAUSE_ANALYSIS", "{}", row);

    // 1. Create CAPA in ROOT_CAUSE_ANALYSIS
    let created: Value = client
        .post(format!("{}/capas", srv.base))
        .bearer_auth(&preparer_qa_token)
        .json(&json!({
            "code": "CAPA-2026-001",
            "status": "ROOT_CAUSE_ANALYSIS",
            "pic_nip": "NIP-PIC-01",
            "prepared_by": "NIP-PREP-01",
            "owner_id": 301
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["success"], true, "create failed: {}", created);
    let id = created["data"]["id"].as_i64().unwrap();

    // 2. Non-PIC cannot transition ROOT_CAUSE_ANALYSIS -> ACTION_PLANNING (403),
    //    even if trying to spoof `pic_nip` in the same request!
    let r = client
        .put(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&other_staff_token)
        .json(&json!({ "status": "ACTION_PLANNING", "pic_nip": "NIP-OTHER-02" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "non-PIC must get 403");

    // Verify status & pic_nip were NOT modified (transaction rolled back)
    let fetched: Value = client
        .get(format!("{}/capas?id.eq={}", srv.base, id))
        .bearer_auth(&pic_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["data"][0]["status"], "ROOT_CAUSE_ANALYSIS");
    assert_eq!(fetched["data"][0]["pic_nip"], "NIP-PIC-01");

    // 3. Matching PIC (`cs == "NIP-PIC-01"`) succeeds -> ACTION_PLANNING (200)
    let r = client
        .put(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&pic_token)
        .json(&json!({ "status": "ACTION_PLANNING" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "matching PIC must succeed");

    // 4. Default actor_claim ("id") against numeric column `owner_id`:
    //    non-owner gets 403, owner (id=301) succeeds via PATCH
    let r = client
        .patch(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&pic_token)
        .json(&json!({ "status": "EVALUATION_APPROVAL" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "non-owner must get 403");

    let r = client
        .patch(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&owner_token)
        .json(&json!({ "status": "EVALUATION_APPROVAL" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "matching owner_id must succeed");

    // 5a. Role guard (`roles: ["plant_qa_manager"]`): wrong role -> 403, not 401
    //     (401 would make clients treat it as an expired session).
    let r = client
        .put(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&other_staff_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "wrong role must get 403");

    // 5. Separation of duties (`not_same_as: ["prepared_by", "pic_nip"]`):
    //    - preparer with role `plant_qa_manager` -> 403
    let r = client
        .put(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&preparer_qa_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "preparer must be rejected by not_same_as");

    //    - PIC with role `plant_qa_manager` -> 403
    let r = client
        .put(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&pic_qa_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "PIC must be rejected by not_same_as");

    //    - independent `plant_qa_manager` -> 200
    let r = client
        .put(format!("{}/capas/{}", srv.base, id))
        .bearer_auth(&independent_qa_token)
        .json(&json!({ "status": "IMPLEMENTATION" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "independent QA manager must succeed");

    let final_row: Value = client
        .get(format!("{}/capas?id.eq={}", srv.base, id))
        .bearer_auth(&independent_qa_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(final_row["data"][0]["status"], "IMPLEMENTATION");
}
