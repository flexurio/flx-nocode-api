//! End-to-end tests for `details[].update_strategy`:
//! - `upsert` with `delete_missing: true` (soft delete & hard delete)
//! - `upsert` with `delete_missing: false`
//! - `upsert` cross-parent ID ownership check + transaction rollback
//! - `append` (insert-only; existing rows untouched)
//! - `replace` (backward-compatible delete + re-insert)
//! - `action_triggers` `iterate_detail` operating on the final active child rows inside the same tx

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

fn child_entity_json(table: &str, fk_col: &str, type_delete: &str) -> String {
    format!(
        r#"{{
  "table": "{table}",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "{fk_col}", "nullable": false, "type_data": "int" }},
    {{ "name": "title", "nullable": false, "type_data": "varchar(255)" }},
    {{ "name": "qty", "nullable": false, "type_data": "int" }},
    {{ "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" }},
    {{ "name": "created_by_id", "nullable": true, "type_data": "int" }},
    {{ "name": "updated_at", "nullable": true, "type_data": "timestamp" }},
    {{ "name": "updated_by_id", "nullable": true, "type_data": "int" }},
    {{ "name": "deleted_at", "nullable": true, "type_data": "timestamp" }},
    {{ "name": "deleted_by_id", "nullable": true, "type_data": "int" }}
  ],
  "redis": {{ "keys": [], "ttl": 3600 }},
  "get": {{ "enable_method": true, "columns": ["id", "{fk_col}", "title", "qty", "created_at", "created_by_id", "updated_at", "updated_by_id"], "parameters": ["{fk_col}.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["{fk_col}", "title*", "qty*"] }},
  "put": {{ "enable_method": true, "columns": ["title", "qty"] }},
  "del": {{ "enable_method": true, "type_delete": "{type_delete}" }},
  "auto_generate": true
}}"#
    )
}

fn parent_entity_json(
    table: &str,
    child_table: &str,
    fk_col: &str,
    strategy: &str,
    delete_missing: Option<bool>,
    with_trigger: bool,
) -> String {
    let dm = match delete_missing {
        Some(b) => format!(r#", "delete_missing": {b}"#),
        None => String::new(),
    };
    let triggers = if with_trigger {
        format!(
            r#",
  "action_triggers": [
    {{
      "name": "recalc_total_qty",
      "event": "on_update",
      "actions": [
        {{
          "type": "iterate_detail",
          "detail_table": "{child_table}",
          "foreign_key": "{fk_col}",
          "actions": [],
          "accumulate": {{
            "sum_qty": "calc: item.qty"
          }}
        }},
        {{
          "type": "update",
          "target_table": "{table}",
          "filter": {{ "id": "{{parent.id}}" }},
          "set": {{ "total_qty": "{{acc.sum_qty}}" }}
        }}
      ]
    }}
  ]"#
        )
    } else {
        String::new()
    };

    format!(
        r#"{{
  "table": "{table}",
  "primary_key": {{ "columns": ["id"], "auto_increment": true }},
  "columns": [
    {{ "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" }},
    {{ "name": "name", "nullable": false, "type_data": "varchar(255)" }},
    {{ "name": "total_qty", "nullable": true, "type_data": "int", "default_value": "0" }},
    {{ "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" }},
    {{ "name": "updated_at", "nullable": true, "type_data": "timestamp" }},
    {{ "name": "updated_by_id", "nullable": true, "type_data": "int" }},
    {{ "name": "deleted_at", "nullable": true, "type_data": "timestamp" }}
  ],
  "details": [
    {{
      "field": "actions",
      "target_table": "{child_table}",
      "foreign_key_column": "{fk_col}",
      "update_strategy": "{strategy}"{dm}
    }}
  ]{triggers},
  "redis": {{ "keys": [], "ttl": 3600 }},
  "get": {{ "enable_method": true, "columns": ["id", "name", "total_qty"], "parameters": ["id.eq"], "order_by": ["id"] }},
  "post": {{ "enable_method": true, "columns": ["name*", "total_qty"] }},
  "put": {{ "enable_method": true, "columns": ["name", "total_qty"] }},
  "del": {{ "enable_method": true, "type_delete": "soft" }},
  "auto_generate": true
}}"#
    )
}

async fn start_server() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("flx-detail-strat-{}-{}", std::process::id(), port));
    let cfg = dir.join("config").join("entity");
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::create_dir_all(dir.join("static")).unwrap();
    std::fs::create_dir_all(dir.join("seed")).unwrap();

    let routes = [
        "capa_upsert_soft",
        "capa_upsert_soft_actions",
        "capa_upsert_keep",
        "capa_upsert_keep_actions",
        "capa_upsert_hard",
        "capa_upsert_hard_actions",
        "capa_append",
        "capa_append_actions",
        "capa_replace",
        "capa_replace_actions",
    ];
    let routes_json = serde_json::json!({
        "routes": routes,
        "route_publics": []
    });
    std::fs::write(
        dir.join("config").join("routes.json"),
        serde_json::to_string_pretty(&routes_json).unwrap(),
    )
    .unwrap();

    std::fs::write(
        cfg.join("capa_upsert_soft_actions.json"),
        child_entity_json("capa_upsert_soft_actions", "capa_id", "soft"),
    )
    .unwrap();
    std::fs::write(
        cfg.join("capa_upsert_soft.json"),
        parent_entity_json(
            "capa_upsert_soft",
            "capa_upsert_soft_actions",
            "capa_id",
            "upsert",
            Some(true),
            true,
        ),
    )
    .unwrap();

    std::fs::write(
        cfg.join("capa_upsert_keep_actions.json"),
        child_entity_json("capa_upsert_keep_actions", "capa_id", "soft"),
    )
    .unwrap();
    std::fs::write(
        cfg.join("capa_upsert_keep.json"),
        parent_entity_json(
            "capa_upsert_keep",
            "capa_upsert_keep_actions",
            "capa_id",
            "upsert",
            Some(false),
            false,
        ),
    )
    .unwrap();

    std::fs::write(
        cfg.join("capa_upsert_hard_actions.json"),
        child_entity_json("capa_upsert_hard_actions", "capa_id", "hard"),
    )
    .unwrap();
    std::fs::write(
        cfg.join("capa_upsert_hard.json"),
        parent_entity_json(
            "capa_upsert_hard",
            "capa_upsert_hard_actions",
            "capa_id",
            "upsert",
            None,
            false,
        ),
    )
    .unwrap();

    std::fs::write(
        cfg.join("capa_append_actions.json"),
        child_entity_json("capa_append_actions", "capa_id", "soft"),
    )
    .unwrap();
    std::fs::write(
        cfg.join("capa_append.json"),
        parent_entity_json(
            "capa_append",
            "capa_append_actions",
            "capa_id",
            "append",
            None,
            true,
        ),
    )
    .unwrap();

    std::fs::write(
        cfg.join("capa_replace_actions.json"),
        child_entity_json("capa_replace_actions", "capa_id", "hard"),
    )
    .unwrap();
    std::fs::write(
        cfg.join("capa_replace.json"),
        parent_entity_json(
            "capa_replace",
            "capa_replace_actions",
            "capa_id",
            "replace",
            None,
            false,
        ),
    )
    .unwrap();

    let env_file = format!(
        "DB_TYPE=sqlite\nSQLITE_URL=sqlite://{db}\nSECRET_KEY=e2e-secret-key-not-for-prod\n\
         ENCRYPT_KEY=0123456789abcdef0123456789abcdef\nPORT={port}\nREQUIRE_AUTH=false\n\
         ACTIX_WORKERS=2\nCONFIG_LINT=warn\nDEBUG=false\nLOGGING=false\n\
         LOC_CONFIG={cfgdir}\nLOC_STATIC={st}\nLOC_SEED={seed}\nLOC_AUDIT={audit}\n\
         RUST_LOG=warn\nMCP_ENABLED=false\nRATE_LIMIT_MUTATE_PER_SEC=1000\nRATE_LIMIT_GET_PER_SEC=1000\n",
        db = dir.join("data.db").display(),
        port = port,
        cfgdir = dir.join("config").display(),
        st = dir.join("static").display(),
        seed = dir.join("seed").display(),
        audit = dir.join("audit.log").display(),
    );
    std::fs::write(dir.join(".env"), env_file).unwrap();

    let log_path = dir.join("server.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_flx-nocode-api"))
        .current_dir(&dir)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn server binary");
    let base = format!("http://127.0.0.1:{}", port);
    let mut server = Server {
        child,
        base: base.clone(),
        log_path,
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(Some(status)) = server.child.try_wait() {
            let log = std::fs::read_to_string(&server.log_path).unwrap_or_default();
            panic!(
                "server exited during boot with {}\n---- server log ----\n{}",
                status, log
            );
        }
        if let Ok(r) = client.get(format!("{}/healthz", base)).send().await
            && r.status().is_success()
        {
            break;
        }
        assert!(Instant::now() < deadline, "server did not become healthy in 60 s");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    server
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detail_update_strategies_upsert_append_replace() {
    let srv = start_server().await;
    let c = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let url = |p: &str| format!("{}{}", srv.base, p);

    // ── 1. UPSERT + delete_missing: true + soft delete + iterate_detail trigger ──
    let r = c
        .post(url("/capa_upsert_soft"))
        .json(&serde_json::json!({
            "name": "CAPA-001",
            "total_qty": 0,
            "actions": [
                { "title": "Action 1", "qty": 3 },
                { "title": "Action 2", "qty": 7 }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "POST capa_upsert_soft: {}", r.text().await.unwrap_or_default());

    let list: serde_json::Value = c
        .get(url("/capa_upsert_soft"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let capa1 = &list["data"][0];
    let capa1_id = capa1["id"].as_i64().unwrap();
    let actions_before = capa1["actions"].as_array().unwrap();
    assert_eq!(actions_before.len(), 2);
    let a1_id = actions_before[0]["id"].as_i64().unwrap();
    let a1_created_at = actions_before[0]["created_at"].clone();
    let a1_created_by = actions_before[0]["created_by_id"].clone();
    let a2_id = actions_before[1]["id"].as_i64().unwrap();

    // Update Action 1 in place, omit Action 2 (soft-delete), insert Action 3
    let r = c
        .put(url(&format!("/capa_upsert_soft/{}", capa1_id)))
        .json(&serde_json::json!({
            "name": "CAPA-001-rev2",
            "actions": [
                { "id": a1_id, "title": "Action 1 updated", "qty": 10 },
                { "title": "Action 3 new", "qty": 5 }
            ]
        }))
        .send()
        .await
        .unwrap();
    let status = r.status();
    let put_body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(status, 200, "PUT capa_upsert_soft failed: {}", put_body);

    // Re-read parent + embedded details
    let after: serde_json::Value = c
        .get(url(&format!("/capa_upsert_soft?id.eq={}", capa1_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let capa1_after = &after["data"][0];
    assert_eq!(capa1_after["name"], "CAPA-001-rev2");
    // Trigger `iterate_detail` must sum ONLY active final rows: 10 + 5 = 15 (excluding soft-deleted Action 2's 7)
    assert_eq!(
        capa1_after["total_qty"].as_i64().unwrap(),
        15,
        "iterate_detail must operate on final active rows only: {}",
        capa1_after
    );

    let actions_after = capa1_after["actions"].as_array().unwrap();
    assert_eq!(actions_after.len(), 2, "soft-deleted action must not appear in GET");
    // Action 1 preserved id, created_at, created_by_id and updated title/qty/updated_at
    assert_eq!(actions_after[0]["id"].as_i64().unwrap(), a1_id);
    assert_eq!(actions_after[0]["created_at"], a1_created_at);
    assert_eq!(actions_after[0]["created_by_id"], a1_created_by);
    assert_eq!(actions_after[0]["title"], "Action 1 updated");
    assert_eq!(actions_after[0]["qty"].as_i64().unwrap(), 10);
    assert!(!actions_after[0]["updated_at"].is_null());
    // Action 3 inserted with new id > a2_id
    let a3_id = actions_after[1]["id"].as_i64().unwrap();
    assert!(a3_id > a2_id);
    assert_eq!(actions_after[1]["title"], "Action 3 new");

    // Attempting to update the already soft-deleted a2_id must fail (404) and roll back parent update
    let r = c
        .put(url(&format!("/capa_upsert_soft/{}", capa1_id)))
        .json(&serde_json::json!({
            "name": "SHOULD-ROLLBACK",
            "actions": [{ "id": a2_id, "title": "Revive attempt", "qty": 99 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "updating soft-deleted child id must return 404");

    // Cross-parent IDOR check: create second CAPA and try to mutate its child from capa1
    let r = c
        .post(url("/capa_upsert_soft"))
        .json(&serde_json::json!({
            "name": "CAPA-002",
            "total_qty": 0,
            "actions": [{ "title": "Other CAPA action", "qty": 1 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let all_capas: serde_json::Value = c
        .get(url("/capa_upsert_soft"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let other_action_id = all_capas["data"][1]["actions"][0]["id"].as_i64().unwrap();

    let r = c
        .put(url(&format!("/capa_upsert_soft/{}", capa1_id)))
        .json(&serde_json::json!({
            "name": "CROSS-PARENT-ROLLBACK",
            "actions": [{ "id": other_action_id, "title": "Hijacked", "qty": 999 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "cross-parent child id must be rejected with 404");

    // Verify capa1 name was NOT changed (rollback worked)
    let verify_capa1: serde_json::Value = c
        .get(url(&format!("/capa_upsert_soft?id.eq={}", capa1_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(verify_capa1["data"][0]["name"], "CAPA-001-rev2");

    // ── 2. UPSERT + delete_missing: false ──
    let r = c
        .post(url("/capa_upsert_keep"))
        .json(&serde_json::json!({
            "name": "KEEP-001",
            "actions": [
                { "title": "Keep 1", "qty": 1 },
                { "title": "Keep 2", "qty": 2 }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let keep_list: serde_json::Value = c
        .get(url("/capa_upsert_keep"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let keep_id = keep_list["data"][0]["id"].as_i64().unwrap();
    let k1_id = keep_list["data"][0]["actions"][0]["id"].as_i64().unwrap();
    let k2_id = keep_list["data"][0]["actions"][1]["id"].as_i64().unwrap();

    let r = c
        .put(url(&format!("/capa_upsert_keep/{}", keep_id)))
        .json(&serde_json::json!({
            "name": "KEEP-001-upd",
            "actions": [
                { "id": k1_id, "title": "Keep 1 updated", "qty": 11 },
                { "title": "Keep 3 new", "qty": 3 }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let keep_after: serde_json::Value = c
        .get(url(&format!("/capa_upsert_keep?id.eq={}", keep_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let keep_actions = keep_after["data"][0]["actions"].as_array().unwrap();
    assert_eq!(keep_actions.len(), 3, "delete_missing=false must preserve omitted row");
    assert_eq!(keep_actions[0]["id"].as_i64().unwrap(), k1_id);
    assert_eq!(keep_actions[0]["title"], "Keep 1 updated");
    assert_eq!(keep_actions[1]["id"].as_i64().unwrap(), k2_id);
    assert_eq!(keep_actions[1]["title"], "Keep 2");
    assert_eq!(keep_actions[2]["title"], "Keep 3 new");

    // ── 3. UPSERT + hard delete ──
    let r = c
        .post(url("/capa_upsert_hard"))
        .json(&serde_json::json!({
            "name": "HARD-001",
            "actions": [
                { "title": "Hard 1", "qty": 1 },
                { "title": "Hard 2", "qty": 2 }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let hard_list: serde_json::Value = c
        .get(url("/capa_upsert_hard"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let hard_id = hard_list["data"][0]["id"].as_i64().unwrap();
    let h1_id = hard_list["data"][0]["actions"][0]["id"].as_i64().unwrap();

    let r = c
        .put(url(&format!("/capa_upsert_hard/{}", hard_id)))
        .json(&serde_json::json!({
            "name": "HARD-001-upd",
            "actions": [{ "id": h1_id, "title": "Hard 1 updated", "qty": 9 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let hard_after: serde_json::Value = c
        .get(url(&format!("/capa_upsert_hard?id.eq={}", hard_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let hard_actions = hard_after["data"][0]["actions"].as_array().unwrap();
    assert_eq!(hard_actions.len(), 1);
    assert_eq!(hard_actions[0]["id"].as_i64().unwrap(), h1_id);
    assert_eq!(hard_actions[0]["title"], "Hard 1 updated");

    // ── 4. APPEND + iterate_detail trigger ──
    let r = c
        .post(url("/capa_append"))
        .json(&serde_json::json!({
            "name": "APP-001",
            "total_qty": 0,
            "actions": [{ "title": "First", "qty": 4 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let app_list: serde_json::Value = c
        .get(url("/capa_append"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let app_id = app_list["data"][0]["id"].as_i64().unwrap();
    let p1_id = app_list["data"][0]["actions"][0]["id"].as_i64().unwrap();
    let p1_created_at = app_list["data"][0]["actions"][0]["created_at"].clone();

    let r = c
        .put(url(&format!("/capa_append/{}", app_id)))
        .json(&serde_json::json!({
            "name": "APP-001-upd",
            "actions": [{ "title": "Second", "qty": 6 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let app_after: serde_json::Value = c
        .get(url(&format!("/capa_append?id.eq={}", app_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let app_actions = app_after["data"][0]["actions"].as_array().unwrap();
    assert_eq!(app_actions.len(), 2, "append must keep old rows and insert new ones");
    assert_eq!(app_actions[0]["id"].as_i64().unwrap(), p1_id);
    assert_eq!(app_actions[0]["created_at"], p1_created_at);
    assert_eq!(app_actions[0]["title"], "First");
    assert_eq!(app_actions[1]["title"], "Second");
    // Trigger `iterate_detail` must see both old (4) + appended (6) = 10
    assert_eq!(app_after["data"][0]["total_qty"].as_i64().unwrap(), 10);

    // ── 5. REPLACE (backward compatibility) ──
    let r = c
        .post(url("/capa_replace"))
        .json(&serde_json::json!({
            "name": "REP-001",
            "actions": [
                { "title": "Old 1", "qty": 1 },
                { "title": "Old 2", "qty": 2 }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let rep_list: serde_json::Value = c
        .get(url("/capa_replace"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rep_id = rep_list["data"][0]["id"].as_i64().unwrap();
    let r2_id = rep_list["data"][0]["actions"][1]["id"].as_i64().unwrap();

    let r = c
        .put(url(&format!("/capa_replace/{}", rep_id)))
        .json(&serde_json::json!({
            "name": "REP-001-upd",
            "actions": [{ "title": "Replaced", "qty": 99 }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let rep_after: serde_json::Value = c
        .get(url(&format!("/capa_replace?id.eq={}", rep_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rep_actions = rep_after["data"][0]["actions"].as_array().unwrap();
    assert_eq!(rep_actions.len(), 1);
    assert!(rep_actions[0]["id"].as_i64().unwrap() > r2_id);
    assert_eq!(rep_actions[0]["title"], "Replaced");
}
