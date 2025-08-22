<div align="center">

# Flexurio No‑Code API Engine

Declarative, database‑driven REST endpoints generated from JSON configuration. Ship CRUD + advanced data operations (GET / POST / PUT / DELETE / PATCH / TRACE) without writing boilerplate code.

</div>

---

## Instalasi & Cara Pakai (Bahasa Indonesia)

Panduan singkat untuk memasang dan menjalankan Flexurio No‑Code API di macOS/Linux, menggunakan skrip installer bawaan atau Docker, serta cara login dan mengakses endpoint.

### Persyaratan
- macOS atau Linux (Windows bisa via WSL).
- Database: MySQL/PostgreSQL/SQLite (disarankan mulai dari SQLite untuk coba cepat).
- Rust toolchain hanya jika ingin build dari source (opsional).

### 1) Instal dengan Flexurio Installer (opsional, memudahkan)
- Jalankan `install-flexurio.sh` untuk memasang binary ke `~/.local/bin` dan membuat perintah `flexurio`.
- Setelah selesai, reload shell Anda (contoh: `source ~/.zshrc`).
- Perintah `flexurio` akan otomatis membaca file `.env` pada direktori kerja saat dijalankan.

Catatan: Skrip menentukan arsitektur OS dan mengunduh binary rilis terbaru dari GitHub sesuai platform Anda.

### 2) Siapkan konfigurasi `.env`
- Salin file contoh: `cp env .env` lalu edit nilainya.
- Minimal isi yang perlu disesuaikan:
  - `DB_TYPE` dan URL koneksinya (contoh cepat: `DB_TYPE=sqlite` dan `SQLITE_URL=sqlite://data.db`).
  - `SECRET_KEY` untuk tanda tangan JWT, dan `ENCRYPT_KEY` untuk enkripsi kolom.
  - `LOC_CONFIG` menunjuk ke profil konfigurasi, misalnya `config/example`, `config/pos`, atau `config/tms`.
- Hindari duplikasi kunci di `.env` (pilih satu `DB_TYPE` saja dan satu URL yang sesuai).

Contoh minimal (SQLite, cepat coba jalan):

```
DB_TYPE=sqlite
SQLITE_URL=sqlite://data.db
LOC_CONFIG="config/example"
SECRET_KEY=ganti_dengan_rahasia_panjang
ENCRYPT_KEY=ganti_juga_rahasia
PORT=8080
DEBUG=True
LOGGING=True
LOC_STATIC=static
LOC_IMAGE=images
LOC_LOGGING=static/log
```

### 3) Jalankan server
Pilih salah satu cara:
- Dengan installer: masuk ke folder proyek yang berisi `.env`, lalu jalankan `flexurio`.
- Dengan binary rilis: pakai file di folder `release/` yang sesuai OS Anda dan jalankan.
- Build dari source: `cargo build --release` lalu jalankan `./target/release/flexurio-api-nocode-v2`.

Pada start pertama:
- Aplikasi memastikan tabel inti (`flx_users`, `flx_roles`) ada. Jika belum, akan dibuat.
- Jika admin belum ada, sistem akan membuat user admin default dengan email `admin` dan password acak 4 digit, yang dicetak di console: "Your admin Password: 1234". Simpan password ini.

### 4) Menjalankan via Docker (opsional)
Gunakan `docker-compose.yaml` sebagai acuan. Sesuaikan volume untuk path lokal Anda. Contoh untuk macOS dari root proyek:

```
services:
  rust-app:
    build: .
    container_name: flexurio-api-nocode-v2
    restart: always
    ports:
      - "2121:8080"    # akses di http://localhost:2121
    volumes:
      - "./static:/app/static"
      - "./config:/app/config"
      - "./.env:/app/.env"
```

### 5) Struktur konfigurasi (LOC_CONFIG)
- `routes.json` berisi daftar route yang diaktifkan dan yang bersifat publik.
- Folder `entity/` berisi file `<route>.json` yang mendeskripsikan skema tabel dan perilaku CRUD.
- Pilih profil contoh di `config/example`, `config/pos`, atau `config/tms`, atau buat sendiri.

Langkah umum menambah route baru:
1. Tambahkan nama route ke array `routes` di `LOC_CONFIG/routes.json`.
2. Buat `LOC_CONFIG/entity/<route>.json` sesuai tabel yang diinginkan.
3. (Opsional) Panggil `POST /generate/table/<route>` bila tabel fisik belum ada.
4. Validasi skema dengan `GET /validate/<route>`.

### 6) Login dan otorisasi
- Endpoint login: `POST /login` memakai header Authorization Basic (`Basic base64(email:password)`).
- Admin default: email `admin`, password dicetak saat start pertama (lihat log console).
- Setelah login, Anda akan menerima JWT. Sertakan pada request selanjutnya: `Authorization: Bearer <token>`.
- Endpoint publik dapat diatur di `route_publics` pada `routes.json`. IP whitelist via `WHITE_LIST_IP`.

### 7) Akses endpoint data
Untuk setiap `route` yang aktif:
- `GET /<route>`: baca data (mendukung parameter query sesuai definisi skema).
- `POST /<route>`: tambah data (mendukung multipart/form‑data untuk upload file).
- `PUT /<route>/:id` dan `DELETE /<route>/:id`.
- `PATCH /<route>` dan `TRACE /<route>` untuk alur khusus (stored procedure/pipeline) sesuai skema.
- `GET /validate/<route>` untuk validasi skema.

Tips:
- Untuk operasi tambahan sebelum/sesudah INSERT/UPDATE, gunakan `post.before`, `post.after`, `put.before`, `put.after` di file skema dengan awalan `SQL:`.
- Gunakan placeholder seperti `{request.field}` agar nilai di‑binding aman (anti SQL injection).

### 8) Integrasi dengan Flexurio
- Repository ini adalah engine No‑Code API milik Flexurio. Untuk contoh konfigurasi nyata, lihat profil di `config/pos`, `config/tms`, atau `configmftl`.
- Gunakan `CUSTOME_JWT_QUERY` di `.env` untuk menambah klaim kustom pada JWT setelah login.

Jika butuh bantuan, buka issue di GitHub atau hubungi tim Flexurio.

## 1. Overview

Flexurio No‑Code API lets you stand up secure, multi‑database REST endpoints by describing each entity (table) in a JSON schema. At startup the engine:

1. Loads the enabled route names from `LOC_CONFIG/routes.json`.
2. Loads the entity schema JSON files from `LOC_CONFIG/entity/*.json` matching each route name.
3. (Optionally) generates required tables (`/generate/table/<route>`) except for built‑ins (`flx_users`, `flx_roles`).
4. Exposes a uniform REST surface for every route: `GET`, `POST`, `PUT /:id`, `DELETE /:id`, `PATCH` (stored procedure / custom processing), `TRACE` (custom select pipeline), plus `GET /validate/<route>`.
5. Applies JWT auth for all non‑public routes (with public overrides from `route_publics` in `routes.json` and optional IP whitelist).

Core use cases:
* Rapid prototyping of admin/data panels
* Multi‑tenant internal tooling
* API scaffolding over existing MySQL/Postgres/SQLite data
* Adding computed / formula‑driven flows (TRACE & PATCH)

---

## 2. Features

* Multi‑DB support: MySQL, PostgreSQL, SQLite (select via `DB_TYPE`).
* JSON‑driven entity schema defining columns, primary key, indexes, Redis cache keys, CRUD behavior, joins, grouping, ordering, and formula expressions.
* Auto table creation endpoint (`/generate/table/<route>`).
* Schema validation endpoint (`/validate/<route>`).
* Role + bitwise permission model embedded in JWT claims (`rl` field, e.g. `products/127,*/*`).
* Configurable additional JWT claim via SQL (`CUSTOME_JWT_QUERY`).
* IP whitelisting override (`WHITE_LIST_IP`).
* Redis caching blueprint (keys + TTL) in schema (implementation hooks in place; extend as needed).
* Pluggable pre / post hooks for POST & PUT (string SQL markers in schema) and PATCH stored procedure like flows.
* Structured logging with optional file output path.
* Static file serving under `/static` (e.g. images, logs).

---

## 3. Quick Start

### 3.1 Clone
```bash
git clone https://github.com/flexurio/flx-nocode-api.git
cd flx-nocode-api
```

### 3.2 Prepare Environment
Copy the example environment file and adapt values.
```bash
cp .env_example .env
```
Minimum required adjustments:
* `PORT` (default 8080)
* Database connection (set `DB_TYPE` and corresponding URL var)
* `SECRET_KEY` (JWT signing) & `ENCRYPT_KEY` (column encryption logic)
* `LOC_CONFIG` path to a config profile (e.g. `config/pos`)

### 3.3 Run (Pre‑built Binaries)
Pick the binary matching your OS / architecture in `release/` and run:
```bash
./flx-nocode-aarch64-apple-darwin   # Apple Silicon
# or
./flx-nocode-x86_64-apple-darwin    # Intel macOS
# or (on Windows)
flx-nocode-x86_64-pc-windows-gnu.exe
```

On first start the engine ensures `flx_users` & `flx_roles` exist and seeds an administrator account (`Admin Flexurio`). Check console logs to retrieve initial credentials if emitted.

### 3.4 (Optional) Build from Source
Requires Rust toolchain.
```bash
cargo build --release
./target/release/flx-nocode-api
```

---

## 4. Environment Variables

Below is the authoritative list derived from `.env_example` and source code.

| Variable | Required | Description |
|----------|----------|-------------|
| PORT | Yes | HTTP listen port. |
| DB_TYPE | Yes | One of `mysql`, `postgres`, `sqlite`. |
| MYSQL_URL / POSTGRES_URL / SQLITE_URL | Cond. | Connection string for selected DB type. (SQLite: `sqlite://data.db`). |
| REDIS_HOST | No | Redis host (future caching usage / extension). |
| REDIS_PORT | No | Redis port. |
| REDIS_PASSWORD | No | Redis auth password. |
| REDIS_DB | No | Redis DB index. |
| SECRET_KEY | Yes | JWT HMAC secret. |
| ENCRYPT_KEY | Yes | Symmetric encryption key for protected columns. |
| BASE_URL | No | External base URL (used in logs / links). |
| DEBUG | No | Set `True` for verbose debug flow. |
| LOGGING | No | Set `True` to enable extended log output. |
| LOC_LOGGING | No | Directory for log files (default `logs`). |
| LOC_CONFIG | Yes | Path to active configuration profile (contains `routes.json` + `entity/`). |
| LOC_IMAGE | No | Static image directory (served under `/static`). |
| CUSTOME_JWT_QUERY | No | SQL template run at login to enrich JWT claim `cs` (use `{:?}` placeholder for user id). |
| WHITE_LIST_IP | No | Comma separated IPs that bypass JWT validation. |

Example snippet:
```env
DB_TYPE=mysql
MYSQL_URL=mysql://user:pass@localhost:3306/your_db
LOC_CONFIG="config/pos"
SECRET_KEY=change_me
ENCRYPT_KEY=change_me_too
PORT=8080
```

---

## 5. Configuration Structure

```
LOC_CONFIG/
  routes.json        # Lists enabled routes + public routes
  entity/
    <route>.json     # Schema per route (must match route name)
```

### 5.1 routes.json
```json
{
  "routes": ["flx_users", "flx_roles", "banks"],
  "route_publics": ["login", "register"]
}
```

### 5.2 Entity Schema (Summary)
Each `<route>.json` → `TableSchema` (see `src/model.rs`). Key sections:
* `table`: Physical table name.
* `primary_key.columns`: Array of PK columns (supports composite).
* `columns`: Column definitions: `name`, `type_data`, `auto_increment`, `nullable`, `function` (DB function override), `encrypt` (bool).
* `indexes`: Named index sets (with `unique`).
* `redis`: `{ keys: [], ttl }` — blueprint for future caching.
* `get`: Read pipeline (projected `columns`, accepted `parameters`, `join_tables`, `column_groups`, `having`, `order_by`).
* `post` / `put`: `{ before, columns, after }` — `before` / `after` can embed SQL formulas prefixed with `SQL:`. Column list defines allowed insert/update fields.
* `del`: `{ columns, type_delete }` where `type_delete` = `soft` or `hard`.
* `patch`: For invoking pre‑processing / stored procedure logic: `{ pre_process_sp, parameters }`.
* `trace`: Advanced pipeline (insert + select + grouping + conflict handling) for data journaling / change capture.

### 5.3 Formula Placeholders
Expressions like `{request.field}` inside `before/after` or formulas are replaced with request JSON values. Patterns like `{products[1].price}` produce sub‑queries `(SELECT price FROM products WHERE id = 1)`.

### 5.4 POST/PUT hooks: using before and after

The fields `post.before`, `post.after`, `put.before`, and `put.after` in `entity/<route>.json` let you run extra SQL before or after the main INSERT/UPDATE operation.

- Where they run:
  - `post.before` runs before the INSERT.
  - `post.after` runs after a successful INSERT.
  - `put.before` runs before the UPDATE.
  - `put.after` runs after a successful UPDATE.
- Must start with the `SQL:` prefix. Empty strings or values without `SQL:` are ignored.
- Placeholders are automatically bound as parameters (SQL‑injection safe). You don’t need to place `?` manually.
- Hooks are not executed in a single transaction with the main operation. If you need atomicity/rollback across all steps, use DB triggers or move logic into a stored procedure.
- Hook errors are logged but do not roll back the main operation.

Minimal example (entity `menus.json`):

```json
{
  "post": {
    "before": "SQL:UPDATE counters SET val = val + 1 WHERE name = 'menus'",
    "after":  "SQL:INSERT INTO audit_logs(entity, action, user_id) VALUES('menus','CREATE',{request.created_by_id})",
    "columns": ["name"]
  },
  "put": {
    "before": "SQL:INSERT INTO audit_logs(entity, action, ref_id) VALUES('menus','BEFORE_UPDATE',{request.id})",
    "after":  "SQL:INSERT INTO audit_logs(entity, action, ref_id) VALUES('menus','AFTER_UPDATE',{request.id})",
    "columns": ["name"]
  }
}
```

Supported placeholders inside before/after:
- `{request.field}` – value from the request body. Multipart/form‑data is supported. Text fields are read as strings; if the text contains valid JSON, it is parsed. Dotted paths are supported, e.g. `{request.user.id}` or `{request.items.0.price}`.
- `{table[123].col}` – becomes a subquery: `(SELECT col FROM table WHERE id = 123)`.
- `{table[{request.id}].col}` – subquery with a dynamic id from the request.

More complete example:

```json
{
  "post": {
    "before": "SQL:UPDATE products SET stock = stock - {request.qty} WHERE id = {request.product_id}",
    "after":  "SQL:INSERT INTO order_items(order_id, product_id, price) VALUES({request.order_id}, {request.product_id}, {products[{request.product_id}].price})",
    "columns": ["order_id", "product_id", "qty"]
  }
}
```

Important notes:
- For PUT, the path parameter `/:id` is not automatically available in hooks. If you need it in formulas, include `id` in the request body.
- For POST with auto‑increment IDs, the newly generated ID is not available via `{request.*}`. If you need to reference the ID in `after`, use a custom ID pattern via `columns[].function` (POST) or provide the `id` yourself in the body.
- The engine auto‑infers numeric vs string bindings. On PostgreSQL, `?` placeholders are converted to `$1,$2,...` internally.
- Each hook is executed as a single statement. For multi‑step logic, prefer a stored procedure or DB‑side routine that encapsulates multiple statements.


---

## 6. Runtime Endpoints

For a route name `<route>` listed in `routes.json`:
* `GET /<route>?param=value` – filtered select.
* `POST /<route>` – multipart/form-data create (supports file + fields).
* `PUT /<route>/:id` – update by id.
* `DELETE /<route>/:id` – delete (soft/hard per schema `del.type_delete`).
* `PATCH /<route>?...` – custom stored procedure / parameterized op (`patch`).
* `TRACE /<route>?...` – custom select + insert/orchestrated flow (`trace`).
* `GET /validate/<route>` – validate entity JSON matches engine expectations.
* `POST /generate/table/<route>` – create underlying table (except reserved core tables).

Authentication:
* Public endpoints: `/login`, `/register`, plus any in `route_publics`.
* Protected endpoints require `Authorization: Bearer <token>`.
* IPs in `WHITE_LIST_IP` bypass token checks.

Permissions (bitwise):
```
1=delete 2=write 4=read 8=execute 16=open/close 32=export 64=approve/reject
```
`rl` claim contains comma‑separated `<route>/<value>` pairs (or `*` wildcard). Engine resolves bit flags to allowed verbs.

---

## 7. Adding a New API Route (Example: banks)

1. Add the name to `routes` array inside `LOC_CONFIG/routes.json`.
2. Create `LOC_CONFIG/entity/banks.json` based on an existing sample (e.g. `flx_users.json`). Ensure `table` and file name align.
3. (Optional) Call `POST /generate/table/banks` if the physical table does not exist.
4. Hit `GET /validate/banks` to confirm schema integrity (returns OK / NOT OK summary).
5. Start using CRUD endpoints: `GET /banks`, `POST /banks`, etc.

Illustrations:
![route](routes.png)
![service](services.png)
![running](running%20project.png)
![validate format](validate%20format.png)

---

## 8. Authentication Flow

1. `POST /login` with credentials → JWT issued (`id`, `nm`, `rl`, optional `cs`).
2. Include token in `Authorization` header for subsequent calls.
3. Optional enrichment via `CUSTOME_JWT_QUERY` (e.g. add user email) using `{:?}` placeholder for user id.

---

## 9. Logging

Structured log entries display endpoint registration and query execution. Control levels via:
* `DEBUG=True` – enables extra diagnostic printing.
* `LOGGING=True` – extended logging; destination path from `LOC_LOGGING`.

Static assets & logs can be served under `/static` (e.g. `GET /static/log/`).

---

## 10. Development Notes

* Worker count is currently fixed to 1 (`.workers(1)`). Adjust in `main.rs` for concurrency.
* Redis objects defined in schema are scaffolds—implement actual cache get/set in DB adapters or select layer as an extension.
* Add rate limiting / audit trails by wrapping Actix middleware before app configuration.
* Extend permission mapping in `auth.rs` if you need more bit flags.

---

## 11. Roadmap Ideas

* Auto‑generate OpenAPI spec from schemas
* Built‑in Redis caching for GET queries
* Web UI for managing schemas + routes
* Row level security / attribute‑based access controls
* Soft delete recovery endpoint
* Query explain / performance metrics

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Exit with `LOC_CONFIG must be set` | Missing env var | Set `LOC_CONFIG` to one of the config profiles. |
| `ROUTES NOT VALID !` | Empty / invalid `routes.json` | Validate JSON syntax & ensure at least one route. |
| Duplicate table name error | Two schemas share same `table` value | Rename one or consolidate. |
| Unauthorized responses | Missing / invalid Authorization header | Re‑login and attach Bearer token. |
| Table not found | Did not generate or create manually | Use `POST /generate/table/<route>` or create table yourself. |

---

## 13. Security Checklist

* Use a long random `SECRET_KEY`.
* Rotate keys regularly (redeploy + reissue tokens).
* Keep `ENCRYPT_KEY` secret; rotate with data re‑encrypt process if used.
* Restrict DB user privileges to least required.
* Sanitize any dynamic formula inputs (engine applies a basic sanitizer; validate further if exposing external clients).
* Enable HTTPS at the reverse proxy (nginx / traefik) level.

---

## 14. Contributing

1. Fork & branch (`feat/<name>`).
2. Make changes with focused commits.
3. Ensure schemas & example configs remain valid.
4. Submit PR with clear description & test notes.

---

## 15. License

See `LICENSE` file.

---

## 16. Credits

Flexurio Engineering Team. Built with Rust + Actix Web.

---

## 17. At a Glance (TL;DR)

1. Configure `.env` & `LOC_CONFIG`.
2. List routes in `routes.json`.
3. Create matching `entity/<route>.json` schemas.
4. Run binary.
5. (Optional) `POST /generate/table/<route>`.
6. Validate with `GET /validate/<route>`.
7. Use REST endpoints with JWT auth.

Happy building.

