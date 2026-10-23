//! End-to-end smoke test: boots the real binary against a database, then
//! exercises the write path, the cached read path (ETag / 304 / invalidation),
//! request ids and the Prometheus endpoint.
//!
//! * Default: SQLite in a temp dir, no external service needed.
//! * `IT_DB_TYPE=postgres|mysql` + `IT_DB_URL=...` runs the same test against a
//!   real server (used by CI with service containers).

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server {
    child: Child,
    base: String,
    log_path: std::path::PathBuf,
    config_dir: std::path::PathBuf,
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

const ENTITY: &str = r#"{
  "table": "master_department",
  "primary_key": { "columns": ["id"], "auto_increment": true },
  "columns": [
    { "name": "id", "auto_increment": true, "nullable": false, "type_data": "int" },
    { "name": "code", "nullable": false, "type_data": "varchar(10)" },
    { "name": "name", "nullable": false, "type_data": "varchar(255)" },
    { "name": "is_active", "nullable": true, "type_data": "boolean" },
    { "name": "created_at", "nullable": false, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" },
    { "name": "updated_at", "nullable": true, "type_data": "timestamp" },
    { "name": "deleted_at", "nullable": true, "type_data": "timestamp" }
  ],
  "indexes": [ { "name": "idx_department_code", "columns": ["code"], "unique": true } ],
  "redis": { "keys": [], "ttl": 3600 },
  "get": { "enable_method": true, "columns": ["id", "code", "name", "is_active"], "parameters": ["code.eq"], "order_by": ["id"] },
  "post": { "enable_method": true, "columns": ["code*", "name*", "is_active"] },
  "put": { "enable_method": true, "columns": ["code", "name", "is_active"] },
  "del": { "enable_method": true, "type_delete": "soft" },
  "auto_generate": true
}"#;

async fn start_server() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("flx-e2e-{}-{}", std::process::id(), port));
    let cfg = dir.join("config").join("entity");
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::create_dir_all(dir.join("static")).unwrap();
    std::fs::create_dir_all(dir.join("seed")).unwrap();
    std::fs::write(dir.join("config").join("routes.json"), r#"{"routes":["master_department"],"route_publics":[]}"#).unwrap();
    std::fs::write(cfg.join("master_department.json"), ENTITY).unwrap();

    let (db_type, db_env) = match std::env::var("IT_DB_TYPE").ok().as_deref() {
        Some("postgres") => ("postgres", format!("POSTGRES_URL={}", std::env::var("IT_DB_URL").expect("IT_DB_URL"))),
        Some("mysql") => ("mysql", format!("MYSQL_URL={}", std::env::var("IT_DB_URL").expect("IT_DB_URL"))),
        _ => ("sqlite", format!("SQLITE_URL=sqlite://{}", dir.join("data.db").display())),
    };
    let env_file = format!(
        "DB_TYPE={db}\n{db_env}\nSECRET_KEY=e2e-secret-key-not-for-prod\nENCRYPT_KEY=0123456789abcdef0123456789abcdef\n\
         PORT={port}\nREQUIRE_AUTH=false\nACTIX_WORKERS=2\nCONFIG_LINT=warn\nDEBUG=false\nLOGGING=false\n\
         LOC_CONFIG={cfgdir}\nLOC_STATIC={st}\nLOC_SEED={seed}\nRUST_LOG=warn\nMCP_ENABLED=false\n\
         RATE_LIMIT_MUTATE_PER_SEC=1000\nRATE_LIMIT_GET_PER_SEC=1000\n",
        db = db_type,
        db_env = db_env,
        port = port,
        cfgdir = dir.join("config").display(),
        st = dir.join("static").display(),
        seed = dir.join("seed").display(),
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
    let mut server = Server { child, base: base.clone(), log_path, config_dir: dir.join("config") };

    // Wait for readiness; fail immediately if the process died during boot.
    let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(Some(status)) = server.child.try_wait() {
            let log = std::fs::read_to_string(&server.log_path).unwrap_or_default();
            panic!("server exited during boot with {}\n---- server log ----\n{}", status, log);
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
async fn write_read_cache_etag_metrics() {
    let srv = start_server().await;
    let c = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    let url = |p: &str| format!("{}{}", srv.base, p);
    let suffix = format!("{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() % 100_000);

    // 1. health carries a request id
    let r = c.get(url("/healthz")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().get("x-request-id").is_some(), "x-request-id header missing");

    // 2. write
    let r = c.post(url("/master_department"))
        .json(&serde_json::json!({ "code": format!("A{}", suffix), "name": "Finance", "is_active": true }))
        .send().await.unwrap();
    assert_eq!(r.status(), 200, "POST body: {}", r.text().await.unwrap_or_default());

    // 3. cached read: ETag present, request id echoed
    let r = c.get(url("/master_department?limit=10")).header("x-request-id", "e2e-abc-123").send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("x-request-id").unwrap(), "e2e-abc-123");
    let etag = r.headers().get("etag").expect("etag header").to_str().unwrap().to_string();
    let body: serde_json::Value = r.json().await.unwrap();
    let n1 = body["data"].as_array().unwrap().len();
    assert!(n1 >= 1);

    // 4. conditional GET -> 304
    let r = c.get(url("/master_department?limit=10")).header("if-none-match", &etag).send().await.unwrap();
    assert_eq!(r.status(), 304, "If-None-Match must yield 304");

    // 5. a write invalidates the cached page (version bump), no SCAN needed
    let r = c.post(url("/master_department"))
        .json(&serde_json::json!({ "code": format!("B{}", suffix), "name": "HR" }))
        .send().await.unwrap();
    assert_eq!(r.status(), 200, "POST body: {}", r.text().await.unwrap_or_default());
    let r = c.get(url("/master_department?limit=10")).header("if-none-match", &etag).send().await.unwrap();
    assert_eq!(r.status(), 200, "stale ETag must not match after a write");
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), n1 + 1);

    // 6. unique violation -> 409 Conflict (typed error mapping)
    let r = c.post(url("/master_department"))
        .json(&serde_json::json!({ "code": format!("A{}", suffix), "name": "Dup" }))
        .send().await.unwrap();
    assert_eq!(r.status(), 409, "duplicate code must be a 409, body: {}", r.text().await.unwrap_or_default());

    // 6b. atomic document numbering: 25 concurrent inserts, 25 distinct gap-free numbers
    std::fs::write(
        srv.config_dir.join("entity").join("document.json"),
        r#"{
          "table": "document",
          "primary_key": { "columns": ["id"], "auto_increment": false },
          "columns": [
            { "name": "id", "nullable": false, "type_data": "varchar(40)", "function": "INV/%Y/000ID" },
            { "name": "title", "nullable": false, "type_data": "varchar(100)" },
            { "name": "created_at", "nullable": true, "type_data": "timestamp", "default_value": "CURRENT_TIMESTAMP" }
          ],
          "get": { "enable_method": true, "columns": ["id", "title"], "order_by": ["id"] },
          "post": { "enable_method": true, "columns": ["title*"] },
          "auto_generate": true
        }"#,
    )
    .unwrap();
    std::fs::write(
        srv.config_dir.join("routes.json"),
        r#"{"routes":["master_department","document"],"route_publics":[]}"#,
    )
    .unwrap();
    let r = c.post(url("/admin/reload")).send().await.unwrap();
    assert_eq!(r.status(), 200, "reload document: {}", r.text().await.unwrap_or_default());
    let mut handles = Vec::new();
    for i in 0..25 {
        let c2 = c.clone();
        let u = url("/document");
        handles.push(tokio::spawn(async move {
            let r = match c2.post(u).json(&serde_json::json!({ "title": format!("doc {}", i) })).send().await {
                Ok(r) => r,
                Err(e) => return (reqwest::StatusCode::BAD_GATEWAY, format!("send error: {}", e)),
            };
            let status = r.status();
            let text = r.text().await.unwrap_or_default();
            let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
            if !status.is_success() {
                return (status, text);
            }
            (status, body["data"]["id"].as_str().map(|s| s.to_string()).unwrap_or_default())
        }));
    }
    let mut ids = std::collections::BTreeSet::new();
    for h in handles {
        let (status, id) = h.await.unwrap();
        assert_eq!(status, 200, "concurrent custom-id insert failed: {}", id);
        assert!(id.starts_with("INV/"), "unexpected id {}", id);
        assert!(ids.insert(id.clone()), "duplicate document number {}", id);
    }
    assert_eq!(ids.len(), 25);
    let nums: Vec<i64> = ids.iter().map(|id| id.rsplit('/').next().unwrap().parse::<i64>().unwrap()).collect();
    assert_eq!(nums.iter().min(), Some(&1));
    assert_eq!(nums.iter().max(), Some(&25), "numbers must be gap-free: {:?}", nums);
    let r = c.get(url("/document?limit=100")).send().await.unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 25);

    // 7. hot reload: add a brand-new entity on disk, reload, use it immediately
    let r = c.get(url("/master_position?limit=1")).send().await.unwrap();
    assert_eq!(r.status(), 404, "unknown route before reload");
    std::fs::write(
        srv.config_dir.join("entity").join("master_position.json"),
        ENTITY.replace("master_department", "master_position").replace("idx_department_code", "idx_position_code"),
    )
    .unwrap();
    std::fs::write(
        srv.config_dir.join("routes.json"),
        r#"{"routes":["master_department","document","master_position"],"route_publics":[]}"#,
    )
    .unwrap();
    let r = c.post(url("/admin/reload")).send().await.unwrap();
    assert_eq!(r.status(), 200, "reload: {}", r.text().await.unwrap_or_default());
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["data"]["added"], serde_json::json!(["master_position"]));
    assert_eq!(body["data"]["tables_generated"], serde_json::json!(["master_position"]));
    let r = c.post(url("/master_position"))
        .json(&serde_json::json!({ "code": format!("P{}", suffix), "name": "Manager" }))
        .send().await.unwrap();
    assert_eq!(r.status(), 200, "POST new entity: {}", r.text().await.unwrap_or_default());
    let r = c.get(url("/master_position?limit=10")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = c.get(url("/admin/registry")).send().await.unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["data"]["generation"], 3, "boot=1, +document, +master_position");

    // 7a. webhooks via the transactional outbox: deliver to a local listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let hook_port = listener.local_addr().unwrap().port();
    let (tx_hook, rx_hook) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        if let Ok((mut sock, _)) = listener.accept() {
            let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));
            let mut buf = vec![0u8; 65536];
            let mut total = 0;
            // Read headers + body (small payload: one read usually suffices, loop until blank line + body length).
            loop {
                match sock.read(&mut buf[total..]) {
                    Ok(0) => break,
                    Ok(n) => {
                        total += n;
                        let text = String::from_utf8_lossy(&buf[..total]).to_string();
                        if let Some(idx) = text.find("\r\n\r\n") {
                            let cl = text.lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)));
                            if cl.map(|c| text.len() - (idx + 4) >= c).unwrap_or(true) { break; }
                        }
                        if total >= buf.len() { break; }
                    }
                    Err(_) => break,
                }
            }
            let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = tx_hook.send(String::from_utf8_lossy(&buf[..total]).to_string());
        }
    });
    let hooked_entity = ENTITY
        .replace("master_department", "master_position")
        .replace("idx_department_code", "idx_position_code")
        .replace(
            "\"auto_generate\": true",
            &format!("\"auto_generate\": true, \"webhooks\": [{{ \"event\": \"on_create\", \"url\": \"http://127.0.0.1:{}/hook\", \"headers\": {{ \"X-Hook-Secret\": \"s3cr3t\" }} }}]", hook_port),
        );
    std::fs::write(srv.config_dir.join("entity").join("master_position.json"), hooked_entity).unwrap();
    let r = c.post(url("/admin/reload")).send().await.unwrap();
    assert_eq!(r.status(), 200, "reload with webhook: {}", r.text().await.unwrap_or_default());
    let r = c.post(url("/master_position"))
        .json(&serde_json::json!({ "code": format!("W{}", suffix), "name": "Webhooked" }))
        .send().await.unwrap();
    assert_eq!(r.status(), 200, "POST with webhook: {}", r.text().await.unwrap_or_default());
    let delivered = rx_hook.recv_timeout(Duration::from_secs(15)).expect("webhook delivered within 15 s");
    assert!(delivered.starts_with("POST /hook"), "{}", delivered);
    assert!(delivered.to_ascii_lowercase().contains("x-hook-secret: s3cr3t"), "{}", delivered);
    assert!(delivered.contains("\"event\":\"on_create\""), "{}", delivered);
    assert!(delivered.contains("Webhooked"), "{}", delivered);

    // 7b. a broken edit is rejected and the running configuration survives
    std::fs::write(srv.config_dir.join("routes.json"), "{ not json").unwrap();
    let r = c.post(url("/admin/reload")).send().await.unwrap();
    assert_eq!(r.status(), 422);
    let r = c.get(url("/master_position?limit=1")).send().await.unwrap();
    assert_eq!(r.status(), 200, "previous registry still serves");
    std::fs::write(
        srv.config_dir.join("routes.json"),
        r#"{"routes":["master_department","document","master_position"],"route_publics":[]}"#,
    )
    .unwrap();

    // 8. metrics expose histograms and pool gauges
    let txt = c.get(url("/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(txt.contains("flx_http_request_duration_seconds_bucket{route=\"master_department\",method=\"GET\",status=\"200\""), "{}", txt);
    assert!(txt.contains("flx_db_query_duration_seconds_count{kind=\"query\"}"), "{}", txt);
    assert!(txt.contains("flx_db_pool_size "), "{}", txt);
    assert!(txt.contains("flx_cache_hits "), "{}", txt);

    let _ = std::io::stderr().flush();
}
