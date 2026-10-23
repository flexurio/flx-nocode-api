# Flexurio API — Analisa Performa, Robustness & Latency dan Rencana Implementasi

Tanggal audit: 2026-10-23 · Versi kode: 0.4.10 (`cbb1292`) · ~31.5k baris Rust, 306 unit test, 0 integration test.

Sudut pandang: Flexurio adalah *engine* ERP no-code yang harus hidup dan dipercaya 30 tahun.
Artinya prioritasnya bukan hanya "cepat di benchmark", tetapi **cepat secara konsisten, tidak
pernah kehilangan data, tidak pernah salah hitung, dan bisa di-upgrade tanpa downtime**.
Dokumen ini menilai kode apa adanya, lalu memberi rencana bertahap yang bisa diukur.

---

## 0. Ringkasan eksekutif

Fondasi yang sudah benar (pertahankan):

- Query dikompilasi dari AST dengan parameter ter-*bind* (anti SQL-injection), satu compiler untuk 4 dialek.
- Cache dua tingkat: L1 `moka` (budget byte) + L2 Redis dengan timeout 100 ms dan `ConnectionManager` auto-reconnect.
- Write-queue Redis dengan `BLMOVE` ke *processing list*, DLQ, dan *requeue* saat boot (at-least-once).
- Validasi FK dan unique digabung satu `UNION ALL` (bukan N+1), keyset pagination, `count` opt-in.
- Linter konfigurasi fail-fast saat startup, `mimalloc`, LTO, rate limiter ter-shard.

Lima temuan dengan dampak terbesar ke pengalaman pengguna:

| # | Temuan | Dampak yang dirasakan user | Bukti |
|---|--------|----------------------------|-------|
| 1 | Nomor urut custom-ID & `{seq:}` dicari dengan `LOWER(id) LIKE LOWER('prefix%') ORDER BY id DESC` tanpa lock | POST makin lambat seiring tabel membesar (full scan), dan dua user menyimpan bersamaan bisa dapat nomor dokumen sama | `data_create_repo.rs:433-437` memakai `F::ILike`; `sql_store.rs:934-938` mengompilasinya ke `LOWER()`; `trigger_engine.rs:966-971` |
| 2 | Invalidasi cache saat tulis = `SCAN` seluruh keyspace Redis, dijalankan **sinkron** di jalur request | Setiap POST/PUT/DELETE membayar waktu scan Redis; makin banyak key, makin lambat semua tulisan | `redis.rs:redis_delete_by_prefix`, dipanggil di `data_create_service.rs:678-681` |
| 3 | Cache hit tetap men-*clone* dan men-serialisasi ulang seluruh `WebResponse`; tidak ada single-flight | Halaman list 2000 baris di-serialisasi dari nol pada tiap hit; saat TTL habis semua request serentak memukul DB (stampede) | `data_read_service.rs:155-176, 195-212` |
| 4 | Argon2 (login), build XLSX, parsing CSV/XLSX, ekstrak zip berjalan di thread async actix | Satu login atau satu export membekukan worker itu untuk semua koneksi lain selama 50-500 ms | `core.rs:215-224`, `data_export_service.rs:560`, `data_import_service.rs` |
| 5 | L1 cache tidak koheren antar replika; PUT membaca record lama tanpa `FOR UPDATE` | Di deployment multi-instance user bisa melihat stok/status basi sampai 60 detik; dua approval bersamaan bisa lolos state-machine | `main.rs` L1 TTL 60 s; `data_update_repo.rs:281-283` |

Target setelah rencana ini selesai (diukur dengan k6 pada 4 vCPU, Postgres lokal):

| Metrik | Sekarang (perkiraan, belum ada histogram) | Target |
|--------|-------------------------------------------|--------|
| GET list cache-hit p50 / p99 | ~1-3 ms / ~15 ms (serialisasi ulang) | < 0.5 ms / < 3 ms |
| GET list uncached 200 baris p95 | tidak terukur | < 25 ms |
| POST header+detail+trigger p95 | tidak terukur | < 60 ms |
| Unique violation → kode HTTP | 500 | 409 |
| Restart saat antrian penuh | job in-flight dipulihkan saat boot | 0 job hilang, 0 audit hilang |
| Ubah entity JSON | restart proses | reload < 1 s tanpa putus koneksi |

---

## 1. Peta arsitektur yang diaudit

```
HTTP (actix 4.15) ─ StatusLogger ─ Compress(off) ─ CORS ─ GlobalRateLimit ─ AuthMiddleware(JWT)
   └─ routes/mod.rs (route statis per entity saat boot)
        └─ handlers → services → repositories
              ├─ data_read_repo: build_read_query (AST) → L1 moka → L2 Redis → DataStore
              ├─ data_create_repo: FK/unique batch → tx → custom ID → insert → details bulk → triggers → commit
              ├─ data_update_repo: tx → SELECT old → version/state_machine/locked_when → UPDATE → triggers
              └─ trigger_engine: cascading multi-tabel dalam tx yang sama (FOR UPDATE opsional)
   storage/sql_store.rs: compiler AST → SQL dialek (mysql/postgres/sqlite/mssql) + mongodb_store
   database/*: sqlx pool per dialek, baris → serde_json::Value
   consumer.rs: Redis list write-queue (BLMOVE/processing/DLQ) · email_queue.rs · audit.rs (mpsc → file)
```

Klasifikasi file (semua kode proyek sendiri; tidak ada vendored third-party di `src/`):

| Modul | LOC | Verdict | Catatan |
|-------|-----|---------|---------|
| `storage/` (ast, sql_store, ddl) | ~1.9k | **Core asset** | Pusat nilai produk; tambah benchmark + property test |
| `nocode/repositories`, `services` | ~6.5k | **Core asset, refactor bertahap** | Logika create/update terduplikasi di `consumer.rs` |
| `nocode/trigger_engine.rs` | 2.8k | **Core asset** | Test bagus; butuh lock/sequence yang benar |
| `database/*` | ~2.4k | Core, perlu hardening | Deteksi tipe kolom via `format!("{:?}")` rapuh |
| `mcp/` | ~3.4k | Core (fitur AI) | Di luar scope performa ini |
| `core.rs` (login, bootstrap download config dari GitHub) | 1.2k | **Pisahkan** | Download `.env`/config dari internet saat boot tidak layak untuk prod |
| `config_asli/`, `configmftl/`, `configxx/`, `__MACOSX/`, `*.zip`, `*.png`, `error.md`, `flexurio.txt`, `.drawio.bkp` | — | **Deprecate / pindah** | Sampah workspace; sebagian ter-track di git |

---

## 2. Temuan rinci

Skala: **Dampak** (ke user) dan **Usaha** (hari kerja 1 engineer) · S = kecil (≤1 hari), M = 2-4 hari, L = 1-2 minggu.

### 2.1 Latency jalur baca (GET)

| ID | Temuan | Dampak | Usaha | Perbaikan |
|----|--------|--------|-------|-----------|
| R-1 | Cache menyimpan `WebResponse` (Value tree). Tiap hit: `cached.clone()` + serialisasi ulang ke JSON. Tiap miss: `result.clone()` dua kali (L1 + task Redis). | Tinggi | S | Simpan **bytes JSON yang sudah diserialisasi** (`bytes::Bytes`) di L1 dan L2. Hit = kirim `Bytes` (clone O(1)) dengan `content_type(json)`. Tambah `ETag` (xxhash dari bytes) dan jawab `304` untuk `If-None-Match`. |
| R-2 | Tidak ada single-flight; saat key kedaluwarsa, N request serentak semuanya query DB. | Tinggi saat beban | S | Ganti `get` + `insert` dengan `l1_cache.try_get_with(key, async {...})` (moka sudah menggabungkan pemanggil serentak). |
| R-3 | `parameters.clone().into_inner()` lalu `params_map_awal.clone()`, `table_schema.get.parameters.clone()`, `declared_columns()` dibangun ulang tiap request. | Sedang | S | Pre-compute per entity saat load schema: `declared_columns`, `allowed_order_cols`, daftar `post_column_names`. Simpan di `TableSchema` (field `#[serde(skip)]`) atau struct `CompiledSchema`. |
| R-4 | Baris → `serde_json::Map` mengalokasikan `String` nama kolom per sel (`meta.name.clone()`), lalu di-serialisasi lagi. | Sedang (list besar) | M | Serialisasi **langsung dari row ke buffer** (`serde::Serializer` manual: tulis `"col":` dari `&str` meta) tanpa membangun `Value`. Jalur `Value` tetap ada untuk trigger/detail yang butuh manipulasi. |
| R-5 | Kompresi HTTP default mati (`ENABLE_COMPRESSION=false`). JSON list ERP terkompresi 70-85 %. | Tinggi di jaringan kantor/VPN | S | Default **on** untuk respons > 1 KB; pilih `zstd` level 1 atau `br` level 3. Lewati untuk download file. |
| R-6 | `?search=` memakai `ILIKE '%term%'` di semua kolom ber-index (dan `LOWER(col) LIKE` di MySQL/SQLite/MSSQL) → tidak pernah memakai index. | Tinggi pada tabel besar | M | Linter + generator: saat entity punya `search`, buat `pg_trgm` GIN index (Postgres), `FULLTEXT` (MySQL), FTS5 virtual table (SQLite). Fallback tetap LIKE. |
| R-7 | Sort/filter diizinkan pada kolom mana pun yang terdaftar walau tidak ber-index. | Sedang | S | Aturan lint baru: **warning** bila `get.parameters`/`order_by` menyebut kolom tanpa index; mode strict bisa jadi error. |
| R-8 | `healthz` menjalankan `SELECT 1` tanpa timeout. | Rendah | S | `/livez` tanpa DB; `/readyz` DB + Redis dengan timeout 2 s. |

### 2.2 Latency & kebenaran jalur tulis (POST/PUT/DELETE)

| ID | Temuan | Dampak | Usaha | Perbaikan |
|----|--------|--------|-------|-----------|
| W-1 | **Nomor urut** (`function` pada kolom id, dan `{seq:}` di trigger) = `SELECT id WHERE LOWER(id) LIKE LOWER('PFX%') ORDER BY id DESC LIMIT 1` di dalam tx **tanpa lock**. Full scan + race condition. | **Kritis** | M | Tabel `flx_sequences(scope PK, last_value)` dengan increment atomik per dialek: PG/SQLite `UPDATE ... SET last_value=last_value+1 RETURNING`, MySQL `INSERT ... ON DUPLICATE KEY UPDATE last_value=LAST_INSERT_ID(last_value+1)`, MSSQL `UPDATE ... OUTPUT inserted.last_value`. O(1), ter-serialisasi oleh row lock, tetap dalam tx yang sama. Migrasi: seed `last_value` dari MAX yang ada per prefix saat pertama kali. |
| W-2 | FK batch + unique batch dijalankan **di luar** transaksi lalu tx dibuka → 3 round-trip sebelum INSERT dan TOCTOU. Pelanggaran unique dari DB jatuh ke 500. | Tinggi | M | Gabungkan FK+unique jadi satu `UNION ALL` **di dalam** tx (satu round-trip). Petakan error DB (`23505`/`1062`/`2627`/`SQLITE_CONSTRAINT`) → **409 Conflict** dengan nama index. |
| W-3 | PUT membaca record lama dengan `SELECT *` biasa, lalu cek `state_machine`/`locked_when`, lalu UPDATE. Dua request bersamaan lolos keduanya kecuali entity punya kolom `version`. | **Kritis untuk ERP** | S | `SELECT ... FOR UPDATE` (PG/MySQL), `WITH (UPDLOCK, ROWLOCK)` (MSSQL) seperti yang sudah dilakukan trigger engine. Generator menambahkan kolom `version` otomatis untuk entity ber-`state_machine`/`locked_when`. |
| W-4 | Invalidasi cache = `SCAN` seluruh keyspace + `UNLINK`, **menunggu** di request. L1 `invalidate_entries_if` juga memindai. | Tinggi | S | **Version key**: `flx:ver:<route>` di-`INCR` saat tulis (O(1)); key cache = `flx:<route>:v<N>:<params>`; key lama mati oleh TTL. GET membaca versi dari L1 mini-cache (TTL 1 s) → koheren antar replika dalam ≤ 1 s. Menyelesaikan W-4 **dan** S-3 sekaligus. |
| W-5 | Consumer write-queue (`exec_post/put/delete`) **melewati** validasi FK/unique, formula, details, triggers, `state_machine`, `locked_when`. Data via `?isqueue=true` tidak setara data sinkron. | **Kritis (integritas)** | M | Refactor `process_insert_request`/`process_update_request` agar menerima `WriteContext { actor, headers, body }` alih-alih `HttpRequest`, lalu consumer memanggil fungsi yang sama. Sementara: linter menolak `isqueue` pada entity yang punya details/triggers/formula. |
| W-6 | `panic = "abort"` di release + ±180 `unwrap()/expect()` di luar test. Satu panic di handler mematikan seluruh proses untuk semua user. | Tinggi | S | `panic = "unwind"` (overhead ≈ 0; actix menangkap panic per koneksi). `#![deny(clippy::unwrap_used, clippy::expect_used)]` pada modul `handlers`, `services`, `repositories`. |
| W-7 | Error dipetakan ke status HTTP lewat `message.contains("not found")` dll. Pesan error membawa SQL mentah ke klien. | Sedang + kebocoran info | M | Enum `ApiError { NotFound, Validation, Conflict, Forbidden, Unavailable, Internal }` implement `ResponseError`; detail SQL hanya ke log dengan `request_id`. |
| W-8 | Audit log di-flush tiap 100 baris; tidak ada handler shutdown untuk flush/stop consumer. | Sedang | S | `tokio::signal` → set flag berhenti, consumer selesaikan batch, flush audit, baru `HttpServer` berhenti (`shutdown_timeout`). |

### 2.3 Pekerjaan blocking & pool

| ID | Temuan | Dampak | Usaha | Perbaikan |
|----|--------|--------|-------|-----------|
| B-1 | `verify_password`/`hash_password` (Argon2) di thread async. | Tinggi (login serentak jam 08.00) | S | `tokio::task::spawn_blocking`. Pertimbangkan parameter Argon2id `m=64MiB,t=3,p=1` eksplisit. |
| B-2 | `rust_xlsxwriter::Workbook` untuk ≤100k baris, `calamine`, `csv`, `ZipArchive` berjalan inline. | Tinggi | S | `spawn_blocking` + semaphore `EXPORT_CONCURRENCY` yang sudah ada. Export besar → streaming CSV (`actix_web::body::BodyStream`) alih-alih buffer penuh. |
| B-3 | Default pool: `min = cpu*4 (8..64)`, `max = cpu*8 (32..256)`, `acquire_timeout 30 s`. 3 replika × 16 vCPU = 384 koneksi Postgres; request bisa antre 30 s. | Tinggi | S | Default `max = cpu*2+4` (≤ 64), `min = cpu`, `acquire_timeout 5 s` → 503 cepat + `Retry-After`. Rekomendasikan **PgBouncer (transaction mode)** / ProxySQL di deployment. |
| B-4 | Tidak ada **statement timeout**; satu filter buruk menahan koneksi berjam-jam. | Tinggi | S | `after_connect`: PG `SET statement_timeout`, MySQL `SET SESSION max_execution_time`, MSSQL `SET LOCK_TIMEOUT`; plus `tokio::time::timeout` 30 s di `DbRepository::query*` dengan metrik. |
| B-5 | SQLite diberi pool hingga 256 koneksi → kontensi writer, "database is locked". | Sedang (SQLite = mode default README) | S | Pool writer = 1, pool reader = N (WAL). `PRAGMA mmap_size`, `cache_size`, `temp_store=MEMORY`. |
| B-6 | sqlx statement cache per koneksi (default 100). SQL dinamis dengan `IN (?,?,?...)` panjang variabel membuat cache *thrash*. | Rendah-Sedang | S | Normalisasi ukuran `IN` ke bucket (1,2,4,8,16,32,64) dengan padding nilai pertama; naikkan `statement_cache_capacity` ke 512. |

### 2.4 Observability (prasyarat semua optimasi)

| ID | Temuan | Perbaikan |
|----|--------|-----------|
| O-1 | Metrik hanya counter; tidak ada histogram latency per route, durasi query DB, waktu tunggu pool. `db_pool_active` tidak pernah di-set. | Crate `metrics` + `metrics-exporter-prometheus`: `http_request_duration_seconds{route,method,status}`, `db_query_duration_seconds{dialect,kind}`, `db_pool_{size,idle,wait_seconds}` (dari `pool.size()/num_idle()`), `cache_requests_total{tier,result}`, `queue_depth`, `queue_lag_seconds`, `trigger_duration_seconds{entity,event}`. |
| O-2 | Logging via `print!` + `colored` dengan lock stdout; `eprintln!` per token gagal (amplifier DoS). | `tracing` + `tracing-subscriber` JSON + `tracing-appender` non-blocking; `request_id` (header `x-request-id` atau ULID) di setiap span; sampling untuk WARN berulang. Opsional `tracing-opentelemetry` → Tempo/Jaeger. |
| O-3 | Tidak ada integration test ke DB nyata; CI tanpa service DB; clippy advisory. | `testcontainers` matrix (postgres, mysql, mssql, sqlite) menjalankan skenario end-to-end (POST master-detail + trigger + GET cache + queue). k6 smoke di CI dengan **gate latency** (p95 regress > 20 % = gagal). `cargo-nextest`, `cargo-deny`, clippy `-D warnings` wajib. |
| O-4 | Tidak ada benchmark untuk compiler AST / `build_read_query`. | `criterion` bench: compile 1 query kompleks < 20 µs; mencegah regresi di core asset. |

### 2.5 Stabilitas operasional & arsitektur jangka panjang

| ID | Temuan | Perbaikan |
|----|--------|-----------|
| S-1 | Semua schema `Lazy` static; route didaftarkan saat boot → ubah 1 entity = restart = downtime + cache dingin. | **Hot reload**: `ArcSwap<Registry>` berisi schema terkompilasi; router catch-all `/{route}` dan `/{route}/{id}` mencari entity di registry; `POST /admin/reload` atau `SIGHUP` memvalidasi (lint) lalu swap atomik. Reload gagal = konfigurasi lama tetap aktif. |
| S-2 | Bootstrap mengunduh `.env`, `config.zip`, `core_config.zip` dari GitHub saat boot. | Pindahkan ke subcommand eksplisit `flx init`; proses server tidak boleh bergantung ke internet. Sertakan `flx_users`/`flx_roles` sebagai `include_str!` dalam binary. |
| S-3 | L1 cache tidak koheren antar replika (TTL 60 s). | Diselesaikan oleh version key (W-4). Alternatif: Redis pub/sub channel `flx:invalidate`. |
| S-4 | Write-queue custom di atas Redis List. Tidak ada metrik lag, tidak ada consumer group, requeue mengandalkan pola nama worker. | Migrasi ke **Redis Streams** (`XADD`/`XREADGROUP`/`XACK`/`XAUTOCLAIM`): at-least-once bawaan, pending-entries list, lag terukur (`XINFO`), banyak replika aman. Kode consumer menyusut. |
| S-5 | JWT 24 jam, role di dalam token, tanpa revokasi. Ubah role baru terasa setelah login ulang. | Access token 15 menit + refresh token; `jti` + denylist Redis untuk logout/paksa keluar. |
| S-6 | Rate limiter per proses. | Cukup untuk sekarang; saat multi-replika pakai Redis `INCR`+`EXPIRE` (atau sliding window Lua) untuk login/mutate saja. |
| S-7 | Deteksi tipe kolom via `format!("{:?}", type_info())`. | Pakai `type_info().name()` + tabel pemetaan; unit test per dialek agar upgrade sqlx tidak diam-diam mengubah tipe JSON. |
| S-8 | `post_process`/`API:` formula memanggil HTTP eksternal di dalam request. | **Transactional outbox**: tulis event ke `flx_outbox` dalam tx yang sama; worker mengirim webhook/integrasi dengan retry. Request user tidak lagi menunggu pihak ketiga. |
| S-9 | Dockerfile tanpa cache dependensi; clippy tidak memblokir. | `cargo-chef` multi-stage, `--locked`, `cargo-deny` (lisensi/advisory), SBOM. |
| S-10 | Workspace penuh berkas sisa (`config_asli/`, `configmftl/`, `*.zip`, PNG, `data.db`, `.env_umar`). | Bersihkan dan perketat `.gitignore`; pindahkan diagram ke `docs/`. |

---

## 3. Teknologi yang diusulkan (belum ada di proyek)

| Kebutuhan | Pilihan | Alasan |
|-----------|---------|--------|
| Tracing & metrik | `tracing`, `tracing-subscriber`, `tracing-appender`, `metrics`, `metrics-exporter-prometheus`, opsional `tracing-opentelemetry` | Standar ekosistem Rust; histogram per route; bisa dikirim ke Grafana/Tempo. |
| Hot reload konfigurasi | `arc-swap` | Swap atomik tanpa lock di jalur request. |
| Single-flight & cache bytes | `moka::future::Cache::try_get_with`, `bytes` | Sudah ada moka; tinggal ganti pola pemakaian. |
| Antrian tulis & event | **Redis Streams** (sudah punya Redis); alternatif NATS JetStream bila ingin lepas dari Redis | Consumer group, ack, lag, replay. |
| Pooling DB di deployment | PgBouncer (transaction), ProxySQL | Menjaga koneksi DB tetap kecil saat replika bertambah. |
| Pencarian teks | `pg_trgm` GIN, MySQL FULLTEXT, SQLite FTS5 | `%term%` memakai index. |
| Serialisasi lebih cepat (opsional, ukur dulu) | `sonic-rs` atau `simd-json` untuk `to_vec` | 1.5-3× lebih cepat dari serde_json pada payload besar; hanya setelah R-1/R-4 selesai. |
| Pengujian | `testcontainers`, `cargo-nextest`, `k6`, `criterion`, `proptest` (compiler AST) | Bukti regresi, bukan asumsi. |
| Build | `cargo-chef`, `cargo-deny`, PGO (`cargo-pgo`) untuk rilis Linux x86_64 | PGO memberi 5-15 % tanpa ubah kode. |
| Push ke klien | SSE/WebSocket (`actix-web` fitur `ws` sudah aktif): channel `entity-changed` per route | UI bisa refresh list tanpa polling; "terasa instan". |

---

## 4. Rencana implementasi bertahap

> **Status 2026-10-23 — Fase 0 dan Fase 1 diimplementasikan** (belum di-commit, lihat `git status`).
>
> | Fase | Item | Status | Catatan |
> |------|------|--------|---------|
> | 0 | Histogram latency HTTP per route/method/status, durasi DB per kind, gauge pool, `x-request-id`, `tracing` (text/JSON) | ✅ | `src/metrics.rs`, `src/middleware.rs`, `src/database/timeout.rs`, `/metrics` |
> | 0 | Skenario k6 dengan threshold | ✅ | `perf/k6/*.js`, `make perf-baseline`; baseline angka **belum direkam** (butuh server + DB nyata) |
> | 0 | Integration test DB nyata di CI | ✅ | `tests/e2e_http.rs` (SQLite lokal; Postgres & MySQL via service container di `.github/workflows/ci.yml`) |
> | 0 | Benchmark criterion compiler/planner | ✅ | `benches/query_compiler.rs`; crate dipisah lib + bin agar bench bisa impor |
> | 1 | Cache menyimpan `Bytes` + ETag/304 + single-flight `try_get_with` | ✅ | `CachedPage` di `state.rs`; cursor `X-Next-Cursor` kini juga ikut saat cache hit (bug lama) |
> | 1 | Version-key menggantikan SCAN; koheren antar replika ≤ `ROUTE_VERSION_REFRESH_MS` | ✅ | `redis.rs::{route_version,bump_route_version}`, `state.rs::invalidate_route`; namespace key L2 baru `flx:c2:` |
> | 1 | Kompresi default on | ✅ | `ENABLE_COMPRESSION=false` untuk mematikan; export file opt-out (`Content-Encoding: identity`) |
> | 1 | `spawn_blocking` Argon2 (login/register), XLSX/CSV export, XLSX/CSV import | ✅ | zip ekstrak hanya saat boot, dibiarkan |
> | 1 | Statement timeout per koneksi + acquire 5 s + default pool `cpu*2+4` | ✅ | PG `statement_timeout`+`lock_timeout`, MySQL/MariaDB, MSSQL `LOCK_TIMEOUT`, SQLite `busy_timeout`; wrapper `TimeoutRepo` |
> | 1 | `panic = "unwind"` | ✅ | lint `unwrap_used`/`expect_used` dipasang sebagai **warn** di handlers/services/repositories (±50 situs tersisa; `deny` di Fase 2) |
> | 1 | Pre-compute allow-list kolom per entity | ✅ sebagian | `declared_cols` di-cache via `OnceLock`; satu clone param map dihapus. Allow-list ORDER BY masih dibangun per request |
> | 1 | SQLite writer pool = 1 | ✅ | reader pool WAL + writer pool 1; PRAGMA kini per koneksi (dulu hanya 1 koneksi yang kena) |


Setiap fase punya **kriteria selesai yang terukur**. Urutan dibuat agar yang berisiko rendah dan berdampak tinggi jalan dulu, dan setiap perubahan performa dibuktikan dengan angka.

### Fase 0 — Alat ukur (minggu 1)

1. Tambah `tracing` + `metrics` + endpoint Prometheus baru (O-1, O-2). `request_id` di setiap log.
2. Skenario k6: `get-list-cached`, `get-list-uncached-200`, `post-master-detail-trigger`, `login-burst`, `mixed-80-20`. Simpan hasil baseline di `docs/perf/baseline-<tanggal>.json`.
3. `testcontainers` untuk Postgres + MySQL + SQLite di CI; 1 skenario end-to-end master-detail + trigger (O-3).
4. `criterion` bench `build_read_query` + `compile_query_with_dialect` (O-4).

Selesai bila: dashboard menampilkan p50/p95/p99 per route, CI menjalankan k6 smoke, baseline tercatat.

### Fase 1 — Quick wins berisiko rendah (minggu 2-3)

| Item | Temuan | Perkiraan dampak |
|------|--------|------------------|
| Cache menyimpan `Bytes` + ETag/304 + `try_get_with` | R-1, R-2 | Cache-hit p99 turun 5-10×, stampede hilang |
| Version-key invalidation (ganti SCAN) | W-4, S-3 | POST/PUT tidak lagi menunggu scan Redis; koheren antar replika |
| Kompresi default on | R-5 | Byte transfer −70-85 % |
| `spawn_blocking` untuk Argon2, XLSX, CSV, zip | B-1, B-2 | Login/export tidak membekukan worker |
| Statement timeout + acquire 5 s + default pool baru | B-3, B-4 | Tidak ada antrean 30 s; query liar dipotong |
| `panic = "unwind"` + lint `unwrap_used` di jalur request | W-6 | Satu request buruk tidak mematikan server |
| Pre-compute `declared_columns`/allow-list per entity | R-3 | −20-40 µs per GET, lebih sedikit alokasi |
| SQLite writer pool = 1 | B-5 | Hilang "database is locked" |

Selesai bila: k6 menunjukkan p95 GET cached < 3 ms dan POST tidak regresi; semua test hijau di 4 dialek.

### Fase 2 — Kebenaran & robustness transaksi (minggu 4-6)

1. **`flx_sequences`** atomik untuk custom-ID dan `{seq:}` (W-1). Migrasi otomatis: saat tabel/prefix pertama kali dipakai, seed dari `MAX` lama di dalam tx yang sama.
2. **`FOR UPDATE` / `UPDLOCK`** saat PUT/PATCH membaca record lama; `version` otomatis untuk entity ber-state-machine (W-3).
3. FK+unique batch **di dalam tx**, satu round-trip; pemetaan error DB → 409 (W-2).
4. `ApiError` bertipe menggantikan string matching; SQL tidak pernah ke klien (W-7).
5. Graceful shutdown: hentikan consumer setelah batch, flush audit, lalu stop HTTP (W-8).
6. `/livez`, `/readyz` dengan timeout (R-8).
7. Consumer memakai jalur service yang sama (`WriteContext`) sehingga data antrian = data sinkron (W-5). Sampai selesai: linter menolak `isqueue` untuk entity dengan details/trigger/formula.

Selesai bila: uji konkurensi (100 POST paralel dengan custom-ID) menghasilkan 100 nomor unik tanpa 500; uji 2 PATCH status bersamaan hanya 1 yang lolos; `kill -TERM` saat antrian 1000 job → 0 hilang.

### Fase 3 — Arsitektur untuk 30 tahun (minggu 7-10)

1. **Hot reload** schema dengan `ArcSwap` + router catch-all (S-1). Reload memvalidasi penuh (lint + diff DDL), lalu swap; cache versi route di-bump.
2. **Redis Streams** untuk write-queue dan email (S-4); metrik lag.
3. **Transactional outbox** untuk `post_process`/webhook/integrasi (S-8).
4. Search index per dialek + lint "kolom filter tanpa index" (R-6, R-7).
5. Serialisasi langsung dari row ke buffer (R-4); baru setelah itu evaluasi `sonic-rs`.
6. `flx init` menggantikan download saat boot; core config di-embed (S-2).
7. JWT pendek + refresh + revokasi (S-5).

Selesai bila: ubah entity JSON → endpoint baru aktif < 1 s tanpa koneksi putus; export 100k baris streaming dengan memori datar; semua integrasi eksternal lewat outbox.

### Fase 4 — Perasaan "instan" di sisi pengguna (setelah Fase 3)

1. SSE channel `entity-changed` → UI refresh otomatis dan optimistic update aman karena `version`/409.
2. `Cache-Control: private, max-age` + ETag untuk master data yang jarang berubah.
3. Build rilis dengan PGO per target; `-C target-cpu=x86-64-v3` untuk image x86_64.
4. Prefetch hint: respons list menyertakan `X-Next-Cursor` (sudah ada) dan `Link: rel=next`; klien bisa prefetch halaman berikut.

---

## 5. Risiko & cara menjaganya

- **Perubahan format cache (Bytes)**: key L2 diberi prefix versi `flx:c2:`; deployment campuran versi lama/baru tidak saling membaca.
- **`flx_sequences`**: nomor lama tetap valid; seed dari MAX dilakukan dalam tx dengan lock baris sequence, sehingga tidak ada lompatan ganda.
- **`FOR UPDATE` pada PUT**: menambah kontensi pada baris yang sama, itulah tujuannya; deadlock dihindari dengan urutan lock tetap (header dulu, lalu detail, lalu target trigger) dan `lock_timeout`.
- **Hot reload**: swap hanya setelah lint strict lulus; DDL perubahan kolom tetap butuh persetujuan eksplisit (`?apply_ddl=true`).
- **Setiap fase** dibuktikan dengan k6 + testcontainers di CI sebelum merge; angka regresi > 20 % memblokir merge.

---

## 6. Yang tidak perlu diubah

- Compiler AST dan model `DbParam` sudah tepat; jangan ganti ke ORM.
- `mimalloc`, LTO thin, `codegen-units=1` sudah optimal; hanya `panic` yang diubah.
- Rate limiter sharded sudah efisien untuk satu proses.
- Pola `BLMOVE` + processing list sudah benar secara semantik; migrasi ke Streams adalah penyederhanaan, bukan perbaikan bug.
