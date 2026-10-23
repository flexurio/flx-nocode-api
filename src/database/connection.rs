use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::database::state::DbRepository;
use crate::log::log_output;

#[cfg(feature = "mysql")]
use crate::database::mysql::MySqlRepo;
#[cfg(feature = "postgres")]
use crate::database::postgres::PostgresRepo;
#[cfg(feature = "sqlite")]
use crate::database::sqlite::SqliteRepo;
#[cfg(feature = "mssql")]
use crate::database::mssql::MssqlRepo;
#[cfg(all(feature = "mssql", feature = "bb8"))]
use crate::database::mssql::MssqlConnectionManager;

#[derive(Debug, Clone)]
pub struct PoolSettings {
    pub max_pool: u32,
    pub min_pool: Option<u32>,
    pub acquire_timeout: Duration,
    pub max_lifetime: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}

impl PoolSettings {
    pub fn from_env(cpu: usize) -> Self {
        // An API process is I/O bound: a handful of connections per core keeps
        // the database's own CPUs busy without piling up hundreds of idle
        // sessions once several replicas run (3 x 16 vCPU used to mean 384
        // Postgres connections). Put PgBouncer/ProxySQL in front for more.
        let max_pool = env::var("MAX_POOL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(((cpu as u32) * 2 + 4).clamp(8, 64));

        // Fail fast: a request should get a 503 after a few seconds rather than
        // queue for half a minute on a saturated pool (that queue only makes
        // every other request slower too). Override via CONNECT_TIMEOUT.
        let acquire_timeout_secs = env::var("CONNECT_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        // Min pool: enough warm connections for the first burst after boot.
        let min_pool = env::var("MIN_POOL")
            .ok()
            .and_then(|s| s.parse().ok())
            .or_else(|| Some((cpu as u32).clamp(2, 16)));

        let max_lifetime = env::var("POOL_MAX_LIFETIME_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .or(Some(Duration::from_secs(60 * 60)));

        let idle_timeout = env::var("POOL_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .or(Some(Duration::from_secs(300)));  // Increased from 60s to 300s to keep idle connections longer

        Self {
            max_pool,
            min_pool,
            acquire_timeout: Duration::from_secs(acquire_timeout_secs),
            max_lifetime,
            idle_timeout,
        }
    }
}

pub struct DbInitialization {
    pub db_type: crate::model::DbType,
    pub repo: Arc<dyn DbRepository>,
}

pub async fn initialize_database(cpu: usize) -> anyhow::Result<DbInitialization> {
    let db_type_str = env::var("DB_TYPE")
        .unwrap_or_else(|_| "mysql".to_string())
        .to_lowercase();

    let db_type = match db_type_str.as_str() {
        "mysql" => crate::model::DbType::Mysql,
        "postgres" => crate::model::DbType::Postgres,
        "sqlite" => crate::model::DbType::Sqlite,
        "mssql" => crate::model::DbType::Mssql,
        "mongodb" => crate::model::DbType::Mongodb,
        _ => {
            eprintln!("Unsupported DB_TYPE: {}", db_type_str);
            return Err(anyhow::anyhow!("Unsupported DB_TYPE: {}", db_type_str));
        }
    };


    let pool_settings = PoolSettings::from_env(cpu);

    log_output(
        "BOOT",
        "POOL",
        &db_type_str,
        format!(
            "cpu={} max_pool={} min_pool={:?} acquire_timeout={}s max_lifetime={:?} idle_timeout={:?}",
            cpu,
            pool_settings.max_pool,
            pool_settings.min_pool,
            pool_settings.acquire_timeout.as_secs(),
            pool_settings.max_lifetime,
            pool_settings.idle_timeout,
        ),
        false,
    );

    let repo: Arc<dyn DbRepository> = match db_type {
        crate::model::DbType::Mysql => {
            #[cfg(not(feature = "mysql"))]
            {
                eprintln!("Feature 'mysql' not enabled at compile time");
                return Err(anyhow::anyhow!("mysql feature disabled"));
            }
            #[cfg(feature = "mysql")]
            {
                let url = match env::var("MYSQL_URL") {
                    Ok(url) => url,
                    Err(_) => {
                        log_output(
                            "ERROR",
                            ".ENV",
                            "MYSQL_URL",
                            "Please set MYSQL_URL on .env file".to_string(),
                            true,
                        );
                        return Err(anyhow::anyhow!("MYSQL_URL not set"));
                    }
                };

                let stmt_ms = *crate::database::timeout::STATEMENT_TIMEOUT_MS;
                let mut opts = sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(pool_settings.max_pool)
                    .acquire_timeout(pool_settings.acquire_timeout)
                    .after_connect(move |conn, _meta| {
                        Box::pin(async move {
                            // MySQL >= 5.7.8 (milliseconds); MariaDB spells it
                            // max_statement_time in seconds. Try both, never fail.
                            let sql = format!("SET SESSION max_execution_time = {}", stmt_ms);
                            if sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&mut *conn).await.is_err() {
                                let sql = format!("SET SESSION max_statement_time = {}", (stmt_ms as f64 / 1000.0).max(1.0));
                                let _ = sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&mut *conn).await;
                            }
                            let sql = format!("SET SESSION innodb_lock_wait_timeout = {}", (stmt_ms / 1000).max(1));
                            let _ = sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&mut *conn).await;
                            Ok(())
                        })
                    });
                if let Some(min) = pool_settings.min_pool {
                    opts = opts.min_connections(min);
                }
                if let Some(d) = pool_settings.max_lifetime {
                    opts = opts.max_lifetime(d);
                }
                if let Some(d) = pool_settings.idle_timeout {
                    opts = opts.idle_timeout(d);
                }
                let pool = opts
                    .connect(&url)
                    .await
                    .map_err(|e| {
                        eprintln!("Failed to connect to MySQL: {}", e);
                        anyhow::anyhow!("Failed to connect to MySQL: {}", e)
                    })?;

                Arc::new(MySqlRepo { pool })
            }
        }
        crate::model::DbType::Postgres => {
            #[cfg(not(feature = "postgres"))]
            {
                eprintln!("Feature 'postgres' not enabled at compile time");
                return Err(anyhow::anyhow!("postgres feature disabled"));
            }
            #[cfg(feature = "postgres")]
            {
                let url = match env::var("POSTGRES_URL") {
                    Ok(url) => url,
                    Err(_) => {
                        log_output(
                            "ERROR",
                            ".ENV",
                            "POSTGRES_URL",
                            "Please set POSTGRES_URL on .env file".to_string(),
                            true,
                        );
                        return Err(anyhow::anyhow!("POSTGRES_URL not set"));
                    }
                };

                let stmt_ms = *crate::database::timeout::STATEMENT_TIMEOUT_MS;
                let mut opts = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(pool_settings.max_pool)
                    .acquire_timeout(pool_settings.acquire_timeout)
                    // One runaway filter must not pin a connection for hours.
                    .after_connect(move |conn, _meta| {
                        Box::pin(async move {
                            let sql = format!("SET statement_timeout = {}", stmt_ms);
                            sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&mut *conn).await?;
                            let sql = format!("SET lock_timeout = {}", stmt_ms);
                            sqlx::query(sqlx::AssertSqlSafe(sql)).execute(&mut *conn).await?;
                            Ok(())
                        })
                    });
                if let Some(min) = pool_settings.min_pool {
                    opts = opts.min_connections(min);
                }
                if let Some(d) = pool_settings.max_lifetime {
                    opts = opts.max_lifetime(d);
                }
                if let Some(d) = pool_settings.idle_timeout {
                    opts = opts.idle_timeout(d);
                }
                let pool = opts
                    .connect(&url)
                    .await
                    .map_err(|e| {
                        eprintln!("Failed to connect to PostgreSQL: {}", e);
                        anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e)
                    })?;

                Arc::new(PostgresRepo { pool })
            }
        }
        crate::model::DbType::Sqlite => {
            #[cfg(not(feature = "sqlite"))]
            {
                eprintln!("Feature 'sqlite' not enabled at compile time");
                return Err(anyhow::anyhow!("sqlite feature disabled"));
            }
            #[cfg(feature = "sqlite")]
            {
                let url = match env::var("SQLITE_URL") {
                    Ok(url) => url,
                    Err(_) => {
                        log_output(
                            "ERROR",
                            ".ENV",
                            "SQLITE_URL",
                            "Please set SQLITE_URL on .env file".to_string(),
                            true,
                        );
                        return Err(anyhow::anyhow!("SQLITE_URL not set"));
                    }
                };
                let path_db = url.replace("sqlite://", "");
                let db_path = Path::new(&path_db);

                if let Some(parent) = db_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                if !db_path.exists() {
                    std::fs::File::create(db_path)?;
                    println!("File {} berhasil dibuat.", db_path.display());
                }

                use std::str::FromStr;
                // PRAGMAs are per-connection, so they belong in the connect
                // options (the old `pool.execute("PRAGMA ...")` only reached one
                // connection of the pool).
                let connect_opts = sqlx::sqlite::SqliteConnectOptions::from_str(&url)
                    .map_err(|e| anyhow::anyhow!("Invalid SQLITE_URL: {}", e))?
                    .create_if_missing(true)
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
                    .busy_timeout(Duration::from_millis(*crate::database::timeout::STATEMENT_TIMEOUT_MS))
                    .pragma("temp_store", "MEMORY")
                    .pragma("cache_size", "-65536") // 64 MiB page cache per connection
                    .pragma("mmap_size", "268435456");

                // Readers: WAL allows them to run concurrently with the writer.
                let reader_max = pool_settings.max_pool.clamp(2, 16);
                let mut read_opts = sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(reader_max)
                    .acquire_timeout(pool_settings.acquire_timeout);
                if let Some(min) = pool_settings.min_pool {
                    read_opts = read_opts.min_connections(min.min(reader_max));
                }
                if let Some(d) = pool_settings.idle_timeout {
                    read_opts = read_opts.idle_timeout(d);
                }
                let pool = read_opts
                    .connect_with(connect_opts.clone())
                    .await
                    .map_err(|e| {
                        eprintln!("Failed to connect to SQLite: {}", e);
                        anyhow::anyhow!("Failed to connect to SQLite: {}", e)
                    })?;

                // Single writer: serialises writes in-process instead of on the file lock.
                let write_pool = sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(1)
                    .min_connections(1)
                    .acquire_timeout(pool_settings.acquire_timeout)
                    .connect_with(connect_opts)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to open SQLite writer: {}", e))?;

                Arc::new(SqliteRepo { pool, write_pool })
            }
        }
        crate::model::DbType::Mssql => {
            #[cfg(not(feature = "mssql"))]
            {
                eprintln!("Feature 'mssql' not enabled at compile time");
                return Err(anyhow::anyhow!("mssql feature disabled"));
            }
            #[cfg(feature = "mssql")]
            {
                let url = match env::var("MSSQL_URL") {
                    Ok(url) => url,
                    Err(_) => {
                        log_output(
                            "ERROR",
                            ".ENV",
                            "MSSQL_URL",
                            "Please set MSSQL_URL on .env file".to_string(),
                            true,
                        );
                        return Err(anyhow::anyhow!("MSSQL_URL not set"));
                    }
                };
                let tcp_timeout = pool_settings.acquire_timeout.as_secs();

                #[cfg(feature = "bb8")]
                {
                    let manager = MssqlConnectionManager::new(url.clone(), tcp_timeout);
                    let pool = bb8::Pool::builder()
                        .max_size(pool_settings.max_pool)
                        .build(manager)
                        .await
                        .map_err(|e| {
                            eprintln!("Failed to create MSSQL pool: {}", e);
                            anyhow::anyhow!("Failed to create MSSQL pool: {}", e)
                        })?;

                    log_output(
                        "INFO",
                        "MSSQL",
                        "CONNECTION POOL",
                        format!("✅ BB8 pool created with max_size={}", pool_settings.max_pool),
                        false,
                    );

                    Arc::new(MssqlRepo { pool })
                }

                #[cfg(not(feature = "bb8"))]
                {
                    let client = connect_mssql(&url, tcp_timeout).await.map_err(|e| {
                        eprintln!("Failed to connect to MSSQL: {}", e);
                        anyhow::anyhow!("Failed to connect to MSSQL: {}", e)
                    })?;

                    eprintln!("⚠️  WARNING: MSSQL running with single client. Enable 'bb8' feature for better performance!");

                    Arc::new(MssqlRepo {
                        client: std::sync::Arc::new(tokio::sync::Mutex::new(client)),
                    })
                }
            }
        }
        #[cfg(feature = "mongodb")]
        crate::model::DbType::Mongodb => {
            struct DummyRepo;

            #[async_trait::async_trait]
            impl DbRepository for DummyRepo {
                async fn query(&self, _sql: &str) -> anyhow::Result<Vec<Value>, anyhow::Error> {
                    Ok(vec![])
                }
                 async fn query_with_params(
                    &self,
                    _sql: &str,
                    _params: Vec<crate::database::state::DbParam>,
                ) -> anyhow::Result<Vec<Value>, anyhow::Error> {
                     Ok(vec![])
                }

                async fn begin_transaction(
                    &self,
                ) -> anyhow::Result<Box<dyn crate::database::state::DbTransaction>, anyhow::Error>
                {
                    Err(anyhow::anyhow!("Transactions not supported for DummyRepo"))
                }
            }

            Arc::new(DummyRepo)
        }

    };

    // Client-side timeout + duration metrics for every statement (see timeout.rs).
    let repo = crate::database::timeout::TimeoutRepo::wrap(repo);
    Ok(DbInitialization { db_type, repo })
}
