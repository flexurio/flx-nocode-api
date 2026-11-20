//! End-to-end tests for CPOB audit trail coverage of *indirect* writes:
//! - `details[]` child rows on POST / PUT (upsert): `DETAIL_CREATE` / `DETAIL_UPDATE` / `DETAIL_DELETE`
//! - `require_reason_when` on a detail entity changed through the parent PUT (400 + rollback)
//! - `action_triggers` `update` / `insert` on audited targets: `TRIGGER_UPDATE` / `TRIGGER_CREATE`
//! - FK `on_delete: cascade` (soft) on parent DELETE: `CASCADE_DELETE`
//! - bulk `POST /import/{route}` (CSV): `IMPORT`
//! - parent rows never contain a bogus `column_name = <details field>` row

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

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
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

const META_COLS: &str = r#"
    { "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" },
    { "name": "created_by_id", "nullable": true, "type_data": "int" },
    { "name": "updated_at", "nullable": true, "type_data": "timestamp" },
    { "name": "updated_by_id", "nullable": true, "type_data": "int" },
    { "name": "deleted_at", "nullable": true, "type_data": "timestamp" },
    { "name": "deleted_by_id", "nullable": true, "type_data": "int" }"#;

fn batch_rec_json() -> String {
    format!(
        r#"{{
  "table": "batch_rec",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "name", "nullable": false, "type_data": "varchar(120)" }},
    {{ "name": "status", "nullable": false, "type_data": "varchar(40)" }},{META_COLS}
  ],
  "details": [
    {{ "field": "steps", "target_table": "batch_step", "foreign_key_column": "batch_id", "update_strategy": "upsert" }}
  ],
  "action_triggers": [
    {{
      "name": "sync_equipment",
      "event": "on_update",
      "condition": {{ "field": "status", "to": "RUNNING" }},
      "actions": [
        {{ "type": "update", "target_table": "equipment", "filter": {{ "id": "1" }}, "set": {{ "status": "RUNNING" }} }},
        {{ "type": "insert", "target_table": "equipment_log", "fields": {{ "equipment_id": "1", "note": "batch {{parent.id}} started" }} }}
      ]
    }}
  ],
  "audit_trail": {{ "enabled": true }},
  "redis": {{ "keys": [], "ttl": 0 }},
  "get": {{ "enable_method": true, "columns": ["id", "name", "status"], "parameters": ["id.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["name*", "status*"] }},
  "put": {{ "enable_method": true, "columns": ["name", "status"] }},
  "del": {{ "enable_method": true, "type_delete": "soft" }},
  "auto_generate": true
}}"#
    )
}

fn batch_step_json() -> String {
    format!(
        r#"{{
  "table": "batch_step",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "batch_id", "nullable": false, "type_data": "int" }},
    {{ "name": "title", "nullable": false, "type_data": "varchar(120)" }},
    {{ "name": "qty", "nullable": false, "type_data": "int" }},{META_COLS}
  ],
  "audit_trail": {{ "enabled": true, "columns": ["title", "qty"], "require_reason_when": {{ "qty": "*" }} }},
  "redis": {{ "keys": [], "ttl": 0 }},
  "get": {{ "enable_method": true, "columns": ["id", "batch_id", "title", "qty"], "parameters": ["batch_id.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["batch_id", "title*", "qty*"] }},
  "put": {{ "enable_method": true, "columns": ["title", "qty"] }},
  "del": {{ "enable_method": true, "type_delete": "soft" }},
  "auto_generate": true
}}"#
    )
}

fn equipment_json() -> String {
    format!(
        r#"{{
  "table": "equipment",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "name", "nullable": false, "type_data": "varchar(120)" }},
    {{ "name": "status", "nullable": false, "type_data": "varchar(40)" }},{META_COLS}
  ],
  "audit_trail": {{ "enabled": true, "columns": ["status"] }},
  "redis": {{ "keys": [], "ttl": 0 }},
  "get": {{ "enable_method": true, "columns": ["id", "name", "status"], "parameters": ["id.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["id", "name*", "status*"] }},
  "put": {{ "enable_method": true, "columns": ["name", "status"] }},
  "del": {{ "enable_method": true, "type_delete": "soft" }},
  "auto_generate": true
}}"#
    )
}

fn equipment_log_json() -> String {
    format!(
        r#"{{
  "table": "equipment_log",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "equipment_id", "nullable": false, "type_data": "int" }},
    {{ "name": "note", "nullable": true, "type_data": "varchar(255)" }},{META_COLS}
  ],
  "audit_trail": {{ "enabled": true }},
  "redis": {{ "keys": [], "ttl": 0 }},
  "get": {{ "enable_method": true, "columns": ["id", "equipment_id", "note"], "parameters": ["id.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["equipment_id*", "note"] }},
  "put": {{ "enable_method": true, "columns": ["note"] }},
  "del": {{ "enable_method": true, "type_delete": "soft" }},
  "auto_generate": true
}}"#
    )
}

fn batch_note_json() -> String {
    format!(
        r#"{{
  "table": "batch_note",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "batch_id", "nullable": false, "type_data": "int" }},
    {{ "name": "text", "nullable": false, "type_data": "varchar(255)" }},{META_COLS}
  ],
  "foreign_keys": [
    {{ "column": "batch_id", "reference_table": "batch_rec", "reference_column": "id", "on_delete": "cascade", "on_update": "no action" }}
  ],
  "audit_trail": {{ "enabled": true, "columns": ["text"] }},
  "redis": {{ "keys": [], "ttl": 0 }},
  "get": {{ "enable_method": true, "columns": ["id", "batch_id", "text"], "parameters": ["batch_id.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["batch_id*", "text*"] }},
  "put": {{ "enable_method": true, "columns": ["text"] }},
  "del": {{ "enable_method": true, "type_delete": "soft" }},
  "auto_generate": true
}}"#
    )
}

async fn start_server() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("flx-audit-cascade-{}-{}", std::process::id(), port));
    let cfg = dir.join("config").join("entity");
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::create_dir_all(dir.join("static")).unwrap();
    std::fs::create_dir_all(dir.join("seed")).unwrap();

    let routes = json!({
        "routes": ["flx_users", "flx_roles", "equipment", "equipment_log", "batch_rec", "batch_step", "batch_note"],
        "route_publics": []
    });
    std::fs::write(dir.join("config").join("routes.json"), routes.to_string()).unwrap();
    std::fs::write(cfg.join("batch_rec.json"), batch_rec_json()).unwrap();
    std::fs::write(cfg.join("batch_step.json"), batch_step_json()).unwrap();
    std::fs::write(cfg.join("equipment.json"), equipment_json()).unwrap();
    std::fs::write(cfg.join("equipment_log.json"), equipment_log_json()).unwrap();
    std::fs::write(cfg.join("batch_note.json"), batch_note_json()).unwrap();

    let log_path = dir.join("server.log");
    let log_file = std::fs::File::create(&log_path).unwrap();
    let log_err = log_file.try_clone().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_flx-nocode-api"))
        .current_dir(&dir)
        .env("DB_TYPE", "sqlite")
        .env("SQLITE_URL", format!("sqlite://{}", dir.join("test.db").display()))
        .env("LOC_CONFIG", dir.join("config"))
        .env("LOC_STATIC", dir.join("static"))
        .env("LOC_SEED", dir.join("seed"))
        .env("LOC_AUDIT", dir.join("audit.log"))
        .env("SECRET_KEY", "cpob-audit-cascade-test-secret-0123456789")
        .env("ENCRYPT_KEY", "0123456789abcdef0123456789abcdef")
        .env("PORT", port.to_string())
        .env("HOST", "127.0.0.1")
        .env("REQUIRE_AUTH", "false")
        .env("RATE_LIMIT_ALL_PER_SEC", "0")
        .env("OUTBOX_ENABLED", "false")
        .env("SCHEDULER_ENABLED", "false")
        .env("MCP_ENABLED", "false")
        .env("DEBUG", "false")
        .env("LOGGING", "false")
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("spawn flx-nocode-api");

    let base = format!("http://127.0.0.1:{}", port);
    let mut srv = Server { child, base: base.clone(), log_path };
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = srv.child.try_wait() {
            let log = std::fs::read_to_string(&srv.log_path).unwrap_or_default();
            panic!("server exited during boot with {}\n{}", status, log);
        }
        if let Ok(r) = client.get(format!("{}/livez", base)).send().await
            && r.status().is_success()
        {
            return srv;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not start in time");
}

async fn audit_rows(c: &reqwest::Client, base: &str, query: &str) -> Vec<Value> {
    let r = c.get(format!("{}/audit_trail?{}&limit=500", base, query)).send().await.unwrap();
    assert_eq!(r.status(), 200, "GET /audit_trail?{} failed", query);
    let v: Value = r.json().await.unwrap();
    v["data"].as_array().cloned().unwrap_or_default()
}

fn id_of(v: &Value) -> String {
    v.to_string().trim_matches('"').to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpob_audit_trail_covers_details_triggers_cascades_and_import() {
    let srv = start_server().await;
    let c = reqwest::Client::new();
    let url = |p: &str| format!("{}{}", srv.base, p);

    // Seed equipment #1 (trigger target).
    let r = c.post(url("/equipment")).json(&json!({ "name": "Granulator", "status": "IDLE" })).send().await.unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());

    // ── 1. POST parent with details → DETAIL_CREATE rows per child, no bogus `steps` row ──
    let r = c
        .post(url("/batch_rec"))
        .header("X-Request-Id", "req-batch-create")
        .json(&json!({
            "name": "BATCH-001",
            "status": "DRAFT",
            "steps": [ { "title": "Weigh", "qty": 10 }, { "title": "Mix", "qty": 5 } ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let body: Value = r.json().await.unwrap();
    let batch_id = id_of(&body["data"]["id"]);

    let created = audit_rows(&c, &srv.base, "request_id.eq=req-batch-create").await;
    assert!(
        created.iter().all(|r| r["column_name"] != "steps"),
        "parent must not audit the details array as a column: {:?}",
        created
    );
    assert!(created.iter().any(|r| r["entity"] == "batch_rec" && r["action"] == "POST" && r["column_name"] == "status"));
    let detail_created: Vec<&Value> = created.iter().filter(|r| r["entity"] == "batch_step").collect();
    assert_eq!(detail_created.len(), 4, "2 children x (title, qty): {:?}", detail_created);
    assert!(detail_created.iter().all(|r| r["action"] == "DETAIL_CREATE" && r["old_value"].is_null()));
    assert!(detail_created.iter().any(|r| r["column_name"] == "title" && r["new_value"] == "Weigh"));

    let steps: Value = c.get(url(&format!("/batch_step?batch_id.eq={}", batch_id))).send().await.unwrap().json().await.unwrap();
    let steps = steps["data"].as_array().unwrap().clone();
    assert_eq!(steps.len(), 2);
    let weigh_id = id_of(&steps[0]["id"]);
    let mix_id = id_of(&steps[1]["id"]);

    // ── 2. Detail qty change without reason → 400 + rollback (require_reason_when on child) ──
    let r = c
        .put(url(&format!("/batch_rec/{}", batch_id)))
        .json(&json!({
            "name": "BATCH-001",
            "status": "DRAFT",
            "steps": [ { "id": weigh_id.parse::<i64>().unwrap(), "title": "Weigh", "qty": 12 }, { "id": mix_id.parse::<i64>().unwrap(), "title": "Mix", "qty": 5 } ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "detail qty change without reason must be 400: {}", r.text().await.unwrap());
    let steps_after: Value = c.get(url(&format!("/batch_step?batch_id.eq={}", batch_id))).send().await.unwrap().json().await.unwrap();
    assert_eq!(steps_after["data"][0]["qty"], 10, "rolled back");

    // ── 3. Same with reason; drop `Mix` (upsert delete_missing) → DETAIL_UPDATE + DETAIL_DELETE ──
    let r = c
        .put(url(&format!("/batch_rec/{}", batch_id)))
        .header("X-Change-Reason", "Recalculated by QA")
        .header("X-Request-Id", "req-batch-steps")
        .json(&json!({
            "name": "BATCH-001",
            "status": "DRAFT",
            "steps": [ { "id": weigh_id.parse::<i64>().unwrap(), "title": "Weigh", "qty": 12 } ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let rows = audit_rows(&c, &srv.base, "request_id.eq=req-batch-steps&entity.eq=batch_step").await;
    let upd: Vec<&Value> = rows.iter().filter(|r| r["action"] == "DETAIL_UPDATE").collect();
    assert_eq!(upd.len(), 1, "{:?}", rows);
    assert_eq!(upd[0]["record_id"], weigh_id);
    assert_eq!(upd[0]["column_name"], "qty");
    assert_eq!(upd[0]["old_value"], "10");
    assert_eq!(upd[0]["new_value"], "12");
    assert_eq!(upd[0]["reason"], "Recalculated by QA");
    let del: Vec<&Value> = rows.iter().filter(|r| r["action"] == "DETAIL_DELETE").collect();
    assert_eq!(del.len(), 2, "Mix: title + qty: {:?}", rows);
    assert!(del.iter().all(|r| r["record_id"] == mix_id && r["new_value"].is_null()));

    // ── 4. Status → RUNNING fires trigger: equipment update + equipment_log insert ──
    let r = c
        .patch(url(&format!("/batch_rec/{}", batch_id)))
        .header("X-Change-Reason", "Batch release")
        .header("X-Request-Id", "req-batch-run")
        .json(&json!({ "status": "RUNNING" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let rows = audit_rows(&c, &srv.base, "request_id.eq=req-batch-run").await;
    let eq_upd: Vec<&Value> = rows.iter().filter(|r| r["entity"] == "equipment").collect();
    assert_eq!(eq_upd.len(), 1, "{:?}", rows);
    assert_eq!(eq_upd[0]["action"], "TRIGGER_UPDATE");
    assert_eq!(eq_upd[0]["record_id"], "1");
    assert_eq!(eq_upd[0]["column_name"], "status");
    assert_eq!(eq_upd[0]["old_value"], "IDLE");
    assert_eq!(eq_upd[0]["new_value"], "RUNNING");
    assert_eq!(eq_upd[0]["reason"], "Batch release [trigger:sync_equipment]");
    let log_rows: Vec<&Value> = rows.iter().filter(|r| r["entity"] == "equipment_log").collect();
    assert!(!log_rows.is_empty(), "trigger insert must be audited: {:?}", rows);
    assert!(log_rows.iter().all(|r| r["action"] == "TRIGGER_CREATE" && !r["record_id"].as_str().unwrap_or("").is_empty()));
    assert!(log_rows.iter().any(|r| r["column_name"] == "note" && r["new_value"] == format!("batch {} started", batch_id)));

    // ── 5. FK cascade (soft) on parent DELETE → CASCADE_DELETE on batch_note ──
    let r = c.post(url("/batch_note")).json(&json!({ "batch_id": batch_id.parse::<i64>().unwrap(), "text": "Line clearance OK" })).send().await.unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let note_id = id_of(&r.json::<Value>().await.unwrap()["data"]["id"]);
    let r = c
        .delete(url(&format!("/batch_rec/{}", batch_id)))
        .header("X-Change-Reason", "Batch cancelled")
        .header("X-Request-Id", "req-batch-del")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let rows = audit_rows(&c, &srv.base, "request_id.eq=req-batch-del&entity.eq=batch_note").await;
    assert_eq!(rows.len(), 1, "{:?}", rows);
    assert_eq!(rows[0]["action"], "CASCADE_DELETE");
    assert_eq!(rows[0]["record_id"], note_id);
    assert_eq!(rows[0]["column_name"], "text");
    assert_eq!(rows[0]["old_value"], "Line clearance OK");
    assert_eq!(rows[0]["reason"], "Batch cancelled");

    // ── 6. Bulk CSV import → IMPORT rows ──
    let boundary = "----flxauditboundary";
    // `id` is auto-increment, so the DB assigns ids; audit must record the real ones.
    let csv = "name,status\nMixer,IDLE\nDryer,MAINTENANCE\n";
    let multipart = format!(
        "--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"equipment.csv\"\r\nContent-Type: text/csv\r\n\r\n{csv}\r\n--{b}--\r\n",
        b = boundary,
        csv = csv
    );
    let r = c
        .post(url("/import/equipment"))
        .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
        .header("X-Change-Reason", "Initial equipment master load")
        .header("X-Request-Id", "req-import")
        .body(multipart)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let rows = audit_rows(&c, &srv.base, "request_id.eq=req-import").await;
    assert_eq!(rows.len(), 2, "one `status` row per imported record: {:?}", rows);
    assert!(rows.iter().all(|r| r["action"] == "IMPORT" && r["entity"] == "equipment" && r["reason"] == "Initial equipment master load"));
    let eq: Value = c.get(url("/equipment")).send().await.unwrap().json().await.unwrap();
    let id_by_name = |name: &str| {
        eq["data"].as_array().unwrap().iter().find(|e| e["name"] == name).map(|e| id_of(&e["id"])).unwrap()
    };
    let (mixer, dryer) = (id_by_name("Mixer"), id_by_name("Dryer"));
    assert!(rows.iter().any(|r| r["record_id"] == mixer && r["new_value"] == "IDLE"), "{:?}", rows);
    assert!(rows.iter().any(|r| r["record_id"] == dryer && r["new_value"] == "MAINTENANCE"), "{:?}", rows);
}
