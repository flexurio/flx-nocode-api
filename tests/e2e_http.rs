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
         LOC_CONFIG={cfgdir}\nLOC_STATIC={st}\nLOC_SEED={seed}\nRUST_LOG=warn\nMCP_ENABLED=false\n",
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
    let mut server = Server { child, base: base.clone(), log_path };

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

    // 6. unique violation is rejected (not a 2xx)
    let r = c.post(url("/master_department"))
        .json(&serde_json::json!({ "code": format!("A{}", suffix), "name": "Dup" }))
        .send().await.unwrap();
    assert!(!r.status().is_success(), "duplicate code must be rejected, got {}", r.status());

    // 7. metrics expose histograms and pool gauges
    let txt = c.get(url("/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(txt.contains("flx_http_request_duration_seconds_bucket{route=\"master_department\",method=\"GET\",status=\"200\""), "{}", txt);
    assert!(txt.contains("flx_db_query_duration_seconds_count{kind=\"query\"}"), "{}", txt);
    assert!(txt.contains("flx_db_pool_size "), "{}", txt);
    assert!(txt.contains("flx_cache_hits "), "{}", txt);

    let _ = std::io::stderr().flush();
}
