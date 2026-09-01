<div align="center">

# Flexurio No‑Code API Engine

**Declarative, database‑driven REST endpoints generated from JSON configuration.**

Ship full CRUD plus advanced data operations (GET / POST / PUT / DELETE / PATCH / TRACE, import & export) for any table — without writing boilerplate code. Built with Rust + Actix Web.

`flx-nocode-api` · supports MySQL · PostgreSQL · SQLite · MSSQL · MongoDB

</div>

---

## Table of Contents

1. [What is Flexurio?](#1-what-is-flexurio)
2. [How it works](#2-how-it-works)
3. [Quick start (SQLite)](#3-quick-start-sqlite)
4. [Installation](#4-installation)
5. [Environment variables (`.env`)](#5-environment-variables-env)
6. [Configuration layout (`LOC_CONFIG`)](#6-configuration-layout-loc_config)
7. [Entity schema reference](#7-entity-schema-reference)
8. [Custom ID generation (`function`)](#8-custom-id-generation-function)
9. [Master‑Detail (Header‑Detail) transactional orchestration](#9-master-detail-header-detail-transactional-orchestration)
10. [Database seeding (`seed`)](#10-database-seeding-seed)
11. [Hooks & validation](#11-hooks--validation)
12. [Formula placeholders](#12-formula-placeholders)
13. [Endpoint reference](#13-endpoint-reference)
14. [Authentication & authorization](#14-authentication--authorization)
15. [Import & export](#15-import--export)
16. [Column encryption](#16-column-encryption)
17. [Logging & observability](#17-logging--observability)
18. [Database feature flags (compile‑time)](#18-database-feature-flags-compile-time)
19. [Multi‑target build script (`build.sh`)](#19-multi-target-build-script-buildsh)
20. [Troubleshooting](#20-troubleshooting)
21. [Security checklist](#21-security-checklist)
22. [Contributing & license](#22-contributing--license)

---

## 1. What is Flexurio?

Flexurio No‑Code API lets you stand up secure, multi‑database REST endpoints by **describing each entity (table) in a JSON file**. You write configuration, not code; the engine generates the HTTP surface, validates requests, talks to the database, and enforces authentication.

Typical use cases:

* Rapid prototyping of admin / data panels.
* Internal tooling and back‑office APIs.
* Putting a clean REST API over an existing MySQL / Postgres / SQLite / MSSQL / MongoDB database.
* Computed / formula‑driven flows and change‑capture journaling (PATCH & TRACE).

---

## 2. How it works

At startup the engine:

1. Reads the active config profile path from the `LOC_CONFIG` environment variable.
2. Loads enabled route names from `LOC_CONFIG/routes.json`.
3. Loads one entity schema per route from `LOC_CONFIG/entity/<route>.json`.
4. Ensures the core tables (`flx_users`, `flx_roles`) exist and seeds a default admin if none exists.
5. Registers a uniform REST surface for every route (only the HTTP methods you enable per schema).
6. Applies JWT authentication to all non‑public routes (with public overrides and an optional IP allow‑list).

```
                ┌────────────────────┐
   routes.json  │  enabled routes    │
                └─────────┬──────────┘
                          │  for each route
                          ▼
        LOC_CONFIG/entity/<route>.json  ──►  TableSchema
                          │
                          ▼
   ┌──────────────────────────────────────────────────────────┐
   │  Actix‑web router                                          │
   │   GET /<route>     POST /<route>    PUT /<route>/{id}      │
   │   DELETE /<route>/{id}   PATCH /<route>   TRACE /<route>   │
   │   POST /import/<route>   GET /export/<route>               │
   │   GET /validate/<route>  POST /generate/table/<route>      │
   └──────────────────────────────────────────────────────────┘
                          │
                          ▼
        DB adapter (MySQL · Postgres · SQLite · MSSQL · MongoDB)
```

---

## 3. Quick start (SQLite)

The fastest way to try Flexurio — no external database needed.

```bash
# 1. Get a binary (build from source shown here; see §4 for installers)
cargo build --release

# 2. Create a minimal .env
cat > .env <<'EOF'
DB_TYPE=sqlite
SQLITE_URL=sqlite://data.db
LOC_CONFIG=config
SECRET_KEY=replace_with_a_long_random_secret
ENCRYPT_KEY=replace_with_another_random_secret
PORT=8080
REQUIRE_AUTH=True
DEBUG=True
LOGGING=True
EOF

# 3. Run it
./target/release/flx-nocode-api
```

On first start the engine creates `flx_users` / `flx_roles` if missing and, if there is no admin yet, prints a generated password to the console:

```
Your admin Password: 1234
```

Log in with email `admin` and that password (see [§14](#14-authentication--authorization)).

---

## 4. Installation

### Requirements

* macOS or Linux (Windows works natively or via WSL).
* A database. SQLite needs nothing extra and is ideal for a first run.
* Rust toolchain **only** if you build from source.

### 4.1 Installer script (macOS / Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/flexurio/flx-nocode-api/main/install-flexurio.sh | bash
```

The script detects your OS/architecture, downloads the matching asset from the latest GitHub release, installs the binary plus a convenient `flexurio` wrapper into `~/.local/bin`, and adds it to your `PATH`. The `flexurio` command automatically reads the `.env` in the current working directory.

Reload your shell afterwards (e.g. `source ~/.zshrc`), then run `flexurio` from any folder containing a `.env`.

### 4.2 Windows

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\install-flexurio.ps1
# open a new terminal, then:
flexurio
```

### 4.3 Manual download (latest release)

```bash
# macOS (Apple Silicon)
curl -fsSL -o flx-nocode-aarch64-apple-darwin.pkg \
  https://github.com/flexurio/flx-nocode-api/releases/latest/download/flx-nocode-aarch64-apple-darwin.pkg
sudo installer -pkg flx-nocode-aarch64-apple-darwin.pkg -target /

# Linux (x86_64)
curl -fsSL -o flx-nocode-x86_64-unknown-linux-gnu \
  https://github.com/flexurio/flx-nocode-api/releases/latest/download/flx-nocode-x86_64-unknown-linux-gnu
chmod +x flx-nocode-x86_64-unknown-linux-gnu
install -m 0755 flx-nocode-x86_64-unknown-linux-gnu "$HOME/.local/bin/flx-nocode"
```

### 4.4 Build from source

```bash
git clone https://github.com/flexurio/flx-nocode-api.git
cd flx-nocode-api
cargo build --release
./target/release/flx-nocode-api
```

To build a smaller binary with only the database backend(s) you need, see [§18](#18-database-feature-flags-compile-time).

### 4.5 Docker

`docker-compose.yaml` is included as a reference. Mount your `static/`, `config/`, and `.env`:

```yaml
services:
  rust-app:
    build: .
    container_name: flx-nocode-api
    restart: always
    ports:
      - "2121:8080"        # access at http://localhost:2121
    volumes:
      - "./static:/app/static"
      - "./config:/app/config"
      - "./.env:/app/.env"
```

---

## 5. Environment variables (`.env`)

Copy the bundled example and edit it:

```bash
cp env .env
```

> Pick exactly one `DB_TYPE` and its matching URL. Avoid duplicate keys.

### Core

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | Yes | HTTP listen port (e.g. `8080`). |
| `DB_TYPE` | Yes | One of `mysql`, `postgres`, `sqlite`, `mssql`, `mongodb`. |
| `MYSQL_URL` / `POSTGRES_URL` / `SQLITE_URL` / `MSSQL_URL` / `MONGODB_URI` | Cond. | Connection string for the selected backend. SQLite quick start: `sqlite://data.db`. |
| `MONGODB_DB` | Cond. | Database name when `DB_TYPE=mongodb`. |
| `SECRET_KEY` | Yes | HMAC secret used to sign Flexurio‑issued JWTs. |
| `ENCRYPT_KEY` | Yes | Symmetric key for encrypted columns (`encrypt: true`). |
| `LOC_CONFIG` | Yes | Path to the active config profile (contains `routes.json` + `entity/`). |
| `REQUIRE_AUTH` | No | `True` (default) to enforce JWT auth; `False` exposes routes without auth. |
| `BASE_URL` | No | External base URL used in logs / links. |

### Locations

| Variable | Default | Description |
|----------|---------|-------------|
| `LOC_STATIC` | `static` | Directory served under `/static`. |
| `LOC_IMAGE` | `images` | Image upload directory (inside the static directory). |
| `LOC_LOGGING` | `logs` | Directory for log files. |
| `LOC_AUDIT` | — | Path to the audit event log (keep outside `static/` to avoid exposing it). |
| `LOC_SEED` | `seed` | Directory containing seed data files (`.json`, `.csv`, `.sql`). |

### Authorization & external JWT (converter token)

| Variable | Description |
|----------|-------------|
| `CUSTOME_JWT_QUERY` | SQL run at login to enrich the JWT `cs` claim. Use `{:?}` as the user‑id placeholder, e.g. `SELECT email FROM flx_users WHERE id = {:?}`. (`CUSTOM_JWT_QUERY` is also accepted.) |
| `WHITE_LIST_IP` | Comma‑separated IPs / CIDR ranges that bypass JWT validation. |
| `CONVERTER_JWT_SECRET` | HMAC secret to verify externally‑issued JWTs (converter‑token mode). |
| `CONVERTER_JWT_PUBLIC_KEY` | PEM public key (RS*/ES*/EdDSA) to verify external JWTs. Use literal `\n` for newlines. |
| `CONVERTER_JWT_ALG` | Algorithm for external JWT verification (e.g. `HS256`, `RS256`). |
| `CONVERTER_JWT_ISSUER` / `CONVERTER_JWT_AUDIENCE` | Optional issuer / audience claim checks (comma‑separated). |
| `CONVERTER_JWT_INSECURE_SKIP_VERIFY` | `true` to accept external JWTs **without** signature verification (only if an upstream gateway already validates them). |

> Converter‑token mode is **fail‑closed**: if it is active and none of the verification variables are set, all converter‑token requests are rejected. See [§14](#14-authentication--authorization).

### Limits, uploads & rate limiting

| Variable | Default | Description |
|----------|---------|-------------|
| `LIMIT_DEFAULT` | `100` | Default page size for `GET`. |
| `LIMIT_MAX` | `1000` | Maximum page size a client may request. |
| `JSON_LIMIT_KB` | `512` | Max JSON request body size. |
| `UPLOAD_LIMIT_MB` | `5` | Max upload size per file. |
| `UPLOAD_TEXT_LIMIT_KB` | `512` | Max size of a text form field. |
| `UPLOAD_MAX_FILES` / `UPLOAD_MAX_FIELDS` | `5` / `100` | Multipart limits. |
| `UPLOAD_EXT_ALLOW` | — | Comma‑separated allow‑list of upload file extensions. |
| `IMPORT_BATCH_SIZE` | — | Rows per batch during import. |
| `RATE_LIMIT_LOGIN_PER_MIN` | `3` | Login attempts per minute. |
| `RATE_LIMIT_MUTATE_PER_SEC` | `20` | Per‑second limit for mutating methods. |
| `RATE_LIMIT_GET_PER_SEC` | `50` | Per‑second limit for `GET`. |
| `RATE_LIMIT_LOGIN_FAIL_USER` / `RATE_LIMIT_LOGIN_FAIL_IP` | — | Failed‑login limits over a 5‑minute window. |

### Performance & server tuning

| Variable | Description |
|----------|-------------|
| `ACTIX_WORKERS` | Number of worker threads (match CPU cores). |
| `HTTP_KEEPALIVE_SECS` / `HTTP_BACKLOG` / `HTTP_MAX_CONN_RATE` / `HTTP_MAX_CONNECTIONS` | HTTP server tuning. |
| `MAX_POOL` / `MIN_POOL` / `CONNECT_TIMEOUT` / `POOL_MAX_LIFETIME_SECS` / `POOL_IDLE_TIMEOUT_SECS` | DB connection‑pool tuning. |
| `WRITE_QUEUE_ENABLED` / `WRITE_CONCURRENCY` / `WRITE_QUEUE_MAX_LEN` / `WRITE_EXEC_RETRY_MAX` | Write‑queue / concurrency controls for high write throughput. |
| `DEFAULT_COLLATE` | Default collation for MySQL/MariaDB (e.g. `utf8mb4_bin`). |
| `MSSQL_ENCRYPTION` / `MSSQL_TRUST_CERT` | TLS options for MSSQL. |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` / `REDIS_DB` | Redis connection (caching / extension). |

### Logging

| Variable | Description |
|----------|-------------|
| `DEBUG` | `1`/`true`/`yes` enables verbose debug logging. |
| `LOGGING` | `True` enables extended log output to `LOC_LOGGING`. |
| `LOG_MIN_LEVEL` | `error\|warn\|info\|debug\|trace` (default `info`). Drops messages below the level. |
| `LOG_MAX_BODY_BYTES` | Max bytes printed per log body (default `8192`); longer bodies are truncated. |
| `LOG_SAMPLE_DEBUG_N` | Sample every N debug/trace logs (default `1` = no sampling). |
| `LOG_QUEUE_CAP` | Bounded logger queue capacity (default `2048`). |
| `LOG_COLOR` | `0`/`false`/`no` disables ANSI colors. |

Logging runs on a non‑blocking background thread, so it never blocks the request path. In production prefer `LOG_MIN_LEVEL=info` (most SQL/param logs are at `debug`) and consider sampling under high load.

---

## 6. Configuration layout (`LOC_CONFIG`)

```
LOC_CONFIG/
  routes.json          # enabled routes + public routes
  rules.json           # (optional) role / endpoint authorization rules
  entity/
    <route>.json       # one schema per route — file name must match the route
```

Several sample profiles are included: `config`, `config/example`, `config/pos`, `config/tms`, `configmftl`.

### `routes.json`

```json
{
  "routes": ["flx_users", "flx_roles", "banks", "bank_types"],
  "route_publics": ["login", "register"]
}
```

* `routes` — entities to expose. Each must have a matching `entity/<name>.json`.
* `route_publics` — routes reachable without a JWT.

### Adding a new route

1. Add the name to `routes` in `routes.json`.
2. Create `entity/<route>.json` (copy an existing one; ensure `table` and the file name align).
3. *(Optional)* `POST /generate/table/<route>` to create the physical table (requires `auto_generate: true`).
4. `GET /validate/<route>` to confirm the schema matches the database.
5. Use the CRUD endpoints.

---

## 7. Entity schema reference

Each `entity/<route>.json` deserializes into a `TableSchema` (see [src/model.rs](src/model.rs)). Sections:

| Key | Purpose |
|-----|---------|
| `table` | Physical table / collection name. |
| `primary_key.columns` | Array of PK columns (supports composite keys). |
| `columns[]` | Column definitions (see below). |
| `foreign_keys[]` | `{ column, reference_table, reference_column, on_delete, on_update }`. Actions: `cascade`, `restrict`, `set null`, `no action`. |
| `details[]` | Array of `DetailSchema` for transactional master‑detail orchestration (see [§9](#9-master-detail-header-detail-transactional-orchestration)). |
| `indexes[]` | `{ name, columns[], unique }`. Unique indexes are enforced on insert/update. |
| `redis` | `{ keys[], ttl }` — cache blueprint. |
| `get` | Read pipeline (see below). |
| `post` / `put` | Create / update behavior + hooks (see [§11](#11-hooks--validation)). Suffix a name in `columns` with `*` to make that field required — e.g. `"columns": ["name*", "phone"]`. |
| `del` | `{ enable_method, columns, type_delete, pre_process, post_process }`; `type_delete` = `soft` or `hard`. |
| `patch` | Stored‑procedure / parameterized op: `{ enable_method, pre_process_sp, parameters[], return_mode }`. `return_mode` = `""` / `rows` / `affected`. |
| `trace` | Advanced insert + select / upsert pipeline for journaling & change capture. |
| `seed` | If `true`, registers `POST /seed/<route>` and `POST /generate/seed/<route>` for database seeding (see [§10](#10-database-seeding-seed)). |
| `auto_generate` | If `true`, the `POST /generate/table/<route>` endpoint is exposed. |
| `collate` | Per‑table collation override. |

Each HTTP method is only registered when its section sets `"enable_method": true`.

### `columns[]`

```json
{
  "name": "id",
  "type_data": "varchar(15)",
  "auto_increment": false,
  "nullable": false,
  "function": "{request.id_trans}/%Y/%m/000ID",
  "function_endpoint": "",
  "function_endpoint_path": "data",
  "encrypt": false,
  "default": null
}
```

| Field | Description |
|-------|-------------|
| `name` | Column name. |
| `type_data` | SQL type, e.g. `varchar(255)`, `bigint`, `timestamp`. |
| `auto_increment` | Auto‑increment integer PK. |
| `nullable` | Whether `NULL` is allowed. |
| `function` | *(optional)* A pattern that builds the column value automatically on **insert** — e.g. `"{request.id_trans}/%Y/%m/000ID"` produces `SO/2026/01/0001`. Empty string = no generation (the client supplies the value). Full token list in [§8](#8-custom-id-generation-function). |
| `function_endpoint` | *(optional)* When `function` contains a numeric `…ID` token, fetch the running number from this HTTP endpoint instead of computing `MAX(id)+1`. Empty string = use the built‑in `MAX(id)+1`. Supports `{request.field}` in the URL. Detail in [§8](#8-custom-id-generation-function). |
| `function_endpoint_path` | *(optional)* Dotted JSON path to the number inside the `function_endpoint` response. Defaults to `data`, i.e. a response of `{ "data": 1 }`. Ignored when `function_endpoint` is empty. |
| `encrypt` | If `true`, the value is stored encrypted with `ENCRYPT_KEY` — see [§16](#16-column-encryption). |
| `default` | Default value used by `generate/table`. |

#### ID generation fields at a glance

The three `function*` fields work together to auto‑generate an id; they only apply to **POST/insert**:

* **`function`** — the *format*. Split on `/`; tokens like `%Y`/`%m`/`%d` (date), `{request.field}` (request value), and `NNNID` (zero‑padded running number) are resolved, everything else is literal.
* **`function_endpoint`** — *where the running number comes from*. Leave empty → the engine uses `MAX(id)+1` for ids sharing the same prefix. Set it → the engine `GET`s that URL (with the built prefix appended as `?prefix=…` and the request's `Authorization` header forwarded) and uses the returned number. There is **no fallback**: if the call fails the insert is aborted.
* **`function_endpoint_path`** — *how to read the number* out of the endpoint's JSON response (default `data`).

> A column with no id generation simply sets `"function": ""` and omits the two endpoint fields. See [§8](#8-custom-id-generation-function) for worked examples.

### `get` (read pipeline)

```json
"get": {
  "enable_method": true,
  "columns": ["banks.id", "banks.name", "bank_types.name"],
  "parameters": ["name.eq", "bank_type_id.eq"],
  "join_tables": [
    { "table": "bank_types", "columns": ["name"], "logical": "banks.bank_type_id = bank_types.id", "type_join": "left" }
  ],
  "column_groups": [],
  "having": [],
  "order_by": ["banks.id"],
  "where_clause": []
}
```

* `parameters` declares which query‑string filters are accepted, in the form `column.operator` (e.g. `name.eq`, `created_at.gte`). Clients then call `GET /banks?name.eq=BCA`.
* Pagination is controlled by `LIMIT_DEFAULT` / `LIMIT_MAX`.

---

## 8. Custom ID generation (`function`)

Set `function` on a column (typically `id`) to build a formatted identifier on insert — e.g. `SO/2026/01/0001`. The pattern is **split on `/`** and each token is resolved:

| Token | Result |
|-------|--------|
| `%Y` | Current 4‑digit year |
| `%m` | Current 2‑digit month |
| `%d` | Current 2‑digit day |
| `{request.field}` | A value from the request body (supports dotted paths) |
| `000ID` (any digits + `ID`) | The running/sequence number, zero‑padded to the number of leading digits (`000ID` → width 3 → `001`) |
| anything else | Used literally |

Example pattern and the value it produces:

```json
{ "name": "id", "type_data": "varchar(15)", "function": "{request.id_trans}/%Y/%m/000ID" }
```

```
request.id_trans = "SO"   →   SO/2026/01/0001
```

### Where the running number comes from

By default the `…ID` token is computed as **`MAX(id) + 1`** for ids sharing the same prefix, inside the same transaction as the insert. You can instead fetch it from an **external endpoint** — useful when sequence numbers are owned by another service.

Add `function_endpoint` (and optionally `function_endpoint_path`) to the same column:

```json
{
  "name": "id",
  "type_data": "varchar(15)",
  "function": "{request.id_trans}/%Y/%m/000ID",
  "function_endpoint": "http://localhost:8080/api/next-sequence",
  "function_endpoint_path": "data"
}
```

Behavior when `function_endpoint` is set:

1. The URL is interpolated (`{request.field}` placeholders are filled from the request body).
2. The already‑built prefix is appended as a query param, URL‑encoded — e.g. `?prefix=SO%2F2026%2F01` — so the endpoint can scope the sequence per prefix.
3. A `GET` is sent; the inbound request's `Authorization` header is forwarded.
4. The response must be JSON. The number is read from `function_endpoint_path` (dotted path, default `data`) and coerced to an integer.
5. The number is zero‑padded to the token width and spliced into the id.

Expected response shape (with the default path `data`):

```json
{ "data": 1 }
```

→ produces `SO/2026/01/0001`.

> **No fallback:** if the endpoint times out, returns a non‑2xx status, or the field is missing/non‑numeric, the insert is aborted with an error. Leave `function_endpoint` empty to use the built‑in `MAX(id)+1` strategy.

 Implementation: [src/nocode/repositories/data_create_repo.rs](src/nocode/repositories/data_create_repo.rs) (`fetch_next_number_from_endpoint` and `query_next_number_from_max`).

---

## 9. Master‑Detail (Header‑Detail) transactional orchestration

Flexurio provides first-class, **atomic transactional orchestration** for Master‑Detail (Header‑Detail / Parent‑Child) business workflows — such as Purchase Orders with line items, Invoices with tax charges, or Sales Orders with products.

Instead of writing multiple manual API requests and managing partial failure rollbacks on the frontend, clients send a single payload with nested items. The engine orchestrates parent generation, foreign key injection, and child batching within a **single ACID database transaction**.

```
                ┌────────────────────────────────────────────────────────┐
   Single POST  │  { id_trans: "PO", customer: "ACME", items: [...] }   │
                └───────────────────────────┬────────────────────────────┘
                                            │
                                            ▼
                       ┌────────────────────────────────────────┐
                       │  Atomic Database Transaction (ACID)     │
                       │                                        │
                       │  1. INSERT Header (e.g. PO/2026/01/001)│
                       │  2. Extract/Auto-gen Parent PK         │
                       │  3. Inject po_id into each child item  │
                       │  4. Bulk INSERT Detail Items           │
                       │  5. COMMIT (or ROLLBACK all on error)  │
                       └────────────────────────────────────────┘
```

### 9.1 Schema Configuration (`details[]`)

Configure one or more detail relationships inside `LOC_CONFIG/entity/<parent_route>.json`:

```json
{
  "table": "transaction_purchase_orders",
  "primary_key": {
    "columns": ["id"]
  },
  "columns": [
    { "name": "id", "type_data": "varchar(20)", "function": "{request.id_trans}/%Y/%m/000ID" },
    { "name": "customer", "type_data": "varchar(100)" },
    { "name": "total_amount", "type_data": "decimal(15,2)" }
  ],
  "details": [
    {
      "field": "items",
      "target_table": "transaction_purchase_order_items",
      "foreign_key_column": "po_id",
      "parent_key_column": "id",
      "columns": ["item_code", "description", "qty", "unit_price", "subtotal"],
      "update_strategy": "replace",
      "cascade_delete": true
    }
  ],
  "post": { "enable_method": true, "columns": ["id_trans", "customer", "total_amount"] },
  "put": { "enable_method": true, "columns": ["customer", "total_amount"] },
  "get": { "enable_method": true, "columns": ["id", "customer", "total_amount"] },
  "del": { "enable_method": true, "type_delete": "hard" }
}
```

| `DetailSchema` Field | Default | Description |
|----------------------|---------|-------------|
| `field` | *(required)* | Key name in the JSON request payload containing the array of child records (e.g. `"items"`, `"details"`, `"lines"`). |
| `target_table` | *(required)* | Physical table name of the detail/child entity. |
| `foreign_key_column` | *(required)* | Column in the child table referencing the parent header's primary key (e.g. `"po_id"`). |
| `parent_key_column` | `"id"` | Column on the parent table whose value is injected into child records. |
| `columns` | `[]` | *(optional)* Column whitelist for child records. If specified, any extra keys in detail items are safely ignored. |
| `update_strategy` | `"replace"` | Strategy on `PUT /<route>/{id}`: `"replace"` (delete old & insert new), `"upsert"` (update existing / insert new), or `"append"` (keep existing & insert new). |
| `cascade_delete` | `true` | When `true`, deleting the parent via `DELETE /<route>/{id}` automatically deletes child records in the same transaction. |

### 9.2 Creating Master‑Detail Records (`POST`)

Send a `POST /<parent_route>` with multipart/form-data or JSON containing the nested items array:

```json
{
  "id_trans": "PO",
  "customer": "PT Maju Bersama",
  "total_amount": 1500000,
  "items": [
    {
      "item_code": "ITM-001",
      "description": "Mechanical Keyboard",
      "qty": 2,
      "unit_price": 500000,
      "subtotal": 1000000
    },
    {
      "item_code": "ITM-002",
      "description": "Ergonomic Mouse",
      "qty": 1,
      "unit_price": 500000,
      "subtotal": 500000
    }
  ]
}
```

**Execution Lifecycle:**
1. The engine generates or assigns the parent primary key (e.g. `PO/2026/01/0001` via `function` pattern or auto-increment).
2. The parent header is inserted into `transaction_purchase_orders`.
3. The generated `id` is automatically injected as `po_id: "PO/2026/01/0001"` into each item in `items`.
4. All child items are bulk-inserted into `transaction_purchase_order_items`.
5. The entire operation is committed atomically. If any detail record fails validation or DB constraint, the parent record is automatically rolled back.

### 9.3 Updating Master‑Detail Records (`PUT`)

Send `PUT /<parent_route>/{id}` with the updated header fields and new/modified detail items:

```json
{
  "customer": "PT Maju Bersama Perkasa",
  "total_amount": 2000000,
  "items": [
    { "item_code": "ITM-001", "description": "Mechanical Keyboard", "qty": 4, "unit_price": 500000, "subtotal": 2000000 }
  ]
}
```

Under `"update_strategy": "replace"` (default), existing child items for that parent are deleted and the new list is inserted within the transaction.

### 9.4 Reading Master‑Detail Records (`GET`)

When calling `GET /<parent_route>` or `GET /<parent_route>/{id}`, Flexurio automatically queries and embeds matching child records inside each parent item under the declared `field` name:

```json
{
  "success": true,
  "data": [
    {
      "id": "PO/2026/01/0001",
      "customer": "PT Maju Bersama",
      "total_amount": 1500000,
      "items": [
        { "id": 1, "po_id": "PO/2026/01/0001", "item_code": "ITM-001", "qty": 2, "subtotal": 1000000 },
        { "id": 2, "po_id": "PO/2026/01/0001", "item_code": "ITM-002", "qty": 1, "subtotal": 500000 }
      ]
    }
  ],
  "total_data": 1
}
```

---

## 10. Database seeding (`seed`)

Flexurio supports declarative database seeding for initial master data, lookup tables, and test fixtures.

```
   LOC_SEED/ (or seed/)
     ├── banks.json          # JSON seed
     ├── bank_types.csv      # CSV seed with schema-aware type casting
     └── init_roles.sql      # Multi-statement raw SQL script
```

### 10.1 Enabling Seeding in Entity Schema

Add `"seed": true` to `LOC_CONFIG/entity/<route>.json`:

```json
{
  "table": "banks",
  "seed": true,
  "columns": [
    { "name": "id", "type_data": "int", "auto_increment": true },
    { "name": "name", "type_data": "varchar(50)", "nullable": false },
    { "name": "code", "type_data": "varchar(10)", "nullable": false }
  ]
}
```

When `"seed": true` is enabled, the engine registers two administrative endpoints:
* `POST /seed/<route>`
* `POST /generate/seed/<route>`

> **Security**: Seed endpoints require an **Admin** role token (`admin`, `administrator`, or bitmask `127` / `*/127`). Non-admin requests receive `403 Forbidden`.

### 10.2 Supported Seed File Formats

Seed files are stored in the directory configured by `LOC_SEED` (default: `seed/`). The engine automatically detects and loads files named `<route>.*` or `<table_name>.*`:

#### 1. JSON (`<LOC_SEED>/<route>.json`)
An array of objects matching column names:
```json
[
  { "name": "Bank Central Asia", "code": "BCA" },
  { "name": "Bank Mandiri", "code": "MANDIRI" },
  { "name": "Bank Rakyat Indonesia", "code": "BRI" }
]
```

#### 2. CSV (`<LOC_SEED>/<route>.csv`)
Comma-separated values with a header row matching column names. Flexurio uses the entity schema to perform **schema-aware type casting** (converting integers, decimals, booleans, dates, timestamps, and JSON strings, while omitting empty auto-increment PKs):
```csv
name,code
Bank Central Asia,BCA
Bank Mandiri,MANDIRI
Bank Rakyat Indonesia,BRI
```

#### 3. SQL Script (`<LOC_SEED>/<route>.sql`)
Raw multi-statement DDL/DML script. The engine parses and splits statements safely (preserving semicolons within quotes and ignoring line/block comments) and executes them inside a transaction:
```sql
-- Initial seed for banks
INSERT INTO banks (name, code) VALUES ('Bank Central Asia', 'BCA');
INSERT INTO banks (name, code) VALUES ('Bank Mandiri', 'MANDIRI');
INSERT INTO banks (name, code) VALUES ('Bank Rakyat Indonesia', 'BRI');
```

### 10.3 Triggering a Seed Run

Trigger seeding by sending an authenticated POST request:

```bash
curl -X POST http://localhost:8080/seed/banks \
  -H "Authorization: Bearer <ADMIN_JWT_TOKEN>"
```

Response:
```json
{
  "success": true,
  "message": "Seeding for 'banks' completed successfully from 'seed/banks.json' (3 records inserted)",
  "total_data": 3,
  "data": null
}
```

---

## 11. Hooks & validation

`post`, `put`, and `del` can run extra logic around the main database operation. All hook strings are **prefixed** to indicate their kind; an empty string or a value without a recognized prefix is ignored.

| Schema field | Runs | Prefixes |
|--------------|------|----------|
| `post.validate_data` | Before INSERT (reject on failure) | `SQL:` or `API:` |
| `post.pre_process` | Before INSERT | `SQL:` |
| `post.post_process` | After a successful INSERT | `SQL:` |
| `put.validate_data` | Before UPDATE (reject on failure) | `SQL:` or `API:` |
| `put.pre_process` | Before UPDATE | `SQL:` |
| `put.post_process` | After a successful UPDATE | `SQL:` |
| `del.pre_process` | Before DELETE | `SQL:` |
| `del.post_process` | After DELETE | `SQL:` |

> These are the actual field names the engine reads. (Older drafts of this README referred to `before`/`after`; those keys are **not** used.)

### `SQL:` hooks

```json
"post": {
  "enable_method": true,
  "pre_process":  "SQL:UPDATE counters SET val = val + 1 WHERE name = 'menus'",
  "post_process": "SQL:INSERT INTO audit_logs(entity, action, user_id) VALUES('menus','CREATE',{request.created_by_id})",
  "columns": ["name"]
}
```

* Placeholders are bound as parameters automatically (SQL‑injection safe) — you do not write `?` yourself.
* Each hook is a **single statement**. For multi‑step transactional logic, use a stored procedure (`patch.pre_process_sp`) or DB triggers.
* `pre_process` / `post_process` run in the operation's transaction on SQL backends; `validate_data` runs first and can reject the request.

### `API:` validation (`validate_data`)

Call an external endpoint and assert on its response before allowing the write. Format:

```
API:<METHOD>:<URL>|<operator>:<response_path>:<request_path>
```

Example — only allow a `role` that the `/roles` endpoint returns:

```json
"validate_data": "API:GET:http://127.0.0.1:8080/roles|in:data:request.role"
```

`SQL:` validation expects a query returning an `is_valid` boolean:

```json
"validate_data": "SQL:SELECT CASE WHEN email NOT LIKE '%@%' THEN FALSE ELSE TRUE END AS is_valid FROM customers WHERE email = {request.email}"
```

### Required fields (`*` suffix)

The `post.columns` array lists the fields the **POST** endpoint accepts. To make a
field **mandatory**, append a `*` to its name. The `*` is only a marker — the engine
strips it and uses the real column name everywhere.

```json
"post": {
  "enable_method": true,
  "columns": ["name*", "email*", "phone"]
}
```

Here `name` and `email` are required; `phone` is optional. A required field is
rejected when it is **absent, `null`, an empty string, or the literal string
`"null"`**, returning:

```
400 Bad Request — Missing required field: name
```

Notes:

* A field is **also** treated as required (even without `*`) when its `columns[]`
  definition has `"nullable": false` and `"auto_increment": false`. The `*` suffix
  is the explicit way to enforce it for any column.
* Empty datetime/timestamp values (`""` or `"null"`) are coerced to SQL `NULL`
  before the required check runs.
* **PUT** (`put.columns`) uses the exact same `*` convention for updates.

---

## 12. Formula placeholders

Placeholders are available inside hooks and formula values:

| Placeholder | Expands to |
|-------------|-----------|
| `{request.field}` | A value from the request body. Multipart form fields are supported; text that is valid JSON is parsed. Dotted paths work: `{request.user.id}`, `{request.items.0.price}`. |
| `{table[123].col}` | Subquery `(SELECT col FROM table WHERE id = 123)`. |
| `{table[{request.id}].col}` | Subquery with a dynamic id taken from the request. |

Notes:

* For `PUT`, the path parameter `/{id}` is **not** auto‑injected into hooks — include `id` in the request body if a formula needs it.
* For `POST` with auto‑increment ids, the new id is not available via `{request.*}`. To reference it afterwards, use a custom id (`columns[].function`) or supply `id` yourself.
* Bindings are numeric/string‑inferred automatically. On PostgreSQL, `?` placeholders are rewritten to `$1, $2, …` internally.

---

## 13. Endpoint reference

For a route `<route>` listed in `routes.json` (each method requires `enable_method: true` in its schema section):

| Method & path | Schema section | Description |
|---------------|----------------|-------------|
| `GET /<route>?col.op=value` | `get` | Filtered read (automatically embeds child records if `details[]` configured). |
| `POST /<route>` | `post` | Create (multipart/form‑data; supports file uploads & atomic master‑detail items). Required fields use `*` suffix — see [§11](#required-fields--suffix). |
| `PUT /<route>/{id}` | `put` | Update by id (synchronizes child detail records per `update_strategy`). |
| `DELETE /<route>/{id}` | `del` | Delete (soft or hard per `del.type_delete`; cascade deletes details if enabled). |
| `PATCH /<route>` | `patch` | Stored‑procedure / parameterized operation. |
| `TRACE /<route>` | `trace` | Custom select + insert / upsert pipeline. |
| `POST /seed/<route>` | `seed` | Trigger database seeding from `<LOC_SEED>/<route>.*` (Admin only; requires `"seed": true`). |
| `POST /generate/seed/<route>` | `seed` | Alternative seed endpoint (Admin only; requires `"seed": true`). |
| `POST /import/<route>` | `post` | Bulk import (CSV / XLSX). See [§15](#15-import--export). |
| `GET /export/<route>` | `get` | Export (CSV / XLSX). See [§15](#15-import--export). |
| `GET /validate/<route>` | — | Validate the entity JSON against the database. |
| `POST /generate/table/<route>` | — | Create the physical table (requires `auto_generate: true`; not for core tables). |

Core / system endpoints:

| Method & path | Description |
|---------------|-------------|
| `POST /login` | Authenticate, returns a JWT (see [§14](#14-authentication--authorization)). |
| `POST /register` | Register a user (multipart). |
| `GET /roles` | List roles. |
| `GET /healthz` | Health check: `{ "status": "ok", "db": "up\|down", "db_type": "…" }`. Returns `503` if the DB is unreachable. |
| `GET /metrics` | Prometheus‑format metrics. |
| `GET /static/...` | Static files from `LOC_STATIC` (directory listing in debug mode). |

---

## 14. Authentication & authorization

### Login

```bash
curl -X POST http://localhost:8080/login \
  -H "Authorization: Basic $(printf 'admin:1234' | base64)"
```

* Credentials are passed via HTTP Basic auth (`Basic base64(email:password)`).
* Success returns a JWT with claims such as `id`, `nm` (name), `rl` (roles), and optional `cs` (custom claim from `CUSTOME_JWT_QUERY`).
* Send it on every protected request: `Authorization: Bearer <token>`.

The default admin (email `admin`) is seeded on first start; its generated password is printed to the console.

### Authorization

* Routes in `route_publics` (plus `/login`, `/register`) are public.
* All other routes require a valid `Bearer` token.
* IPs/CIDRs in `WHITE_LIST_IP` bypass token checks.
* Fine‑grained, role‑/endpoint‑based rules can be defined in `LOC_CONFIG/rules.json` (per‑method `permission_id`, `allowed_fields`, and `if` conditions such as `$user.role` / `$user.id`).

### Converter‑token mode (external IdP)

When `routes.json` defines a non‑default `converter_token` mapping, JWTs are issued by an external identity provider and Flexurio **verifies their signature** using the `CONVERTER_JWT_*` variables ([§5](#5-environment-variables-env)). This is fail‑closed: with no verification key configured, converter‑token requests are rejected (unless `CONVERTER_JWT_INSECURE_SKIP_VERIFY=true`).

---

## 15. Import & export

* **Import** — `POST /import/<route>` with a multipart file. CSV and XLSX are supported; column headers must match the entity's insertable columns. Rows are inserted in batches (`IMPORT_BATCH_SIZE`).
* **Export** — `GET /export/<route>?type=csv|xlsx` returns the route's data in the requested format (defaults to CSV; falls back to CSV if XLSX generation fails). The same filtering as `GET /<route>` applies.

---

## 16. Column encryption

Set `"encrypt": true` on a column to store its value encrypted at rest using `ENCRYPT_KEY` (AES‑GCM). The engine encrypts on write and decrypts on read transparently. Keep `ENCRYPT_KEY` secret and stable — rotating it requires re‑encrypting existing data.

---

## 17. Logging & observability

* Structured logs cover endpoint registration and query execution; control verbosity with the `LOG_*`, `DEBUG`, and `LOGGING` variables ([§5](#5-environment-variables-env)).
* `GET /healthz` for liveness/readiness probes.
* `GET /metrics` exposes Prometheus metrics.
* An audit trail is written to `LOC_AUDIT`.
* Logs and static assets are served under `/static` (e.g. `GET /static/log/`). Keep the audit log outside `static/` so it is not publicly served.

---

## 18. Database feature flags (compile‑time)

Database backends are gated behind Cargo features so you can build a lean binary with only what you need.

```toml
[features]
default  = ["mysql", "postgres", "sqlite", "mssql", "mongodb"]
mysql    = ["sqlx/mysql", "sqlx/chrono"]
postgres = ["sqlx/postgres", "sqlx/chrono"]
sqlite   = ["sqlx/sqlite", "sqlx/chrono"]
mssql    = ["tiberius/chrono", "bb8"]
mongodb  = ["dep:mongodb"]
```

If you disable a backend but set `DB_TYPE` to it at runtime, the app exits with an error (e.g. `mysql feature disabled`).

```bash
# Only MySQL
cargo build --release --no-default-features --features mysql

# MySQL + SQLite
cargo build --release --no-default-features --features "mysql sqlite"

# Everything (default)
cargo build --release

# Build & run in one step
cargo run --release --no-default-features --features mysql
```

Smaller builds compile faster, produce smaller binaries, and remove unused code paths from production.

---

## 19. Multi‑target build script (`build.sh`)

`build.sh` produces per‑database, per‑OS binaries with feature‑gated builds, and optionally signs/notarizes macOS artifacts when Apple credentials are present.

> Use `./build.sh` or `bash build.sh` (not `sh build.sh`) — macOS ships Bash 3.2.

```bash
./build.sh [--db <list>] [--os <list>] [--arch <list>] [--help]
```

* `--db` — `mysql,postgres,sqlite,all` (default `all`).
* `--os` — `macos,windows,linux,all` (default `all`).
* `--arch` — `x86_64,aarch64,all` (filters after OS expansion, default `all`).

OS group expansion:

* `macos` → `x86_64-apple-darwin`, `aarch64-apple-darwin`
* `windows` → `x86_64-pc-windows-gnu`
* `linux` → `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`

Examples:

```bash
./build.sh                                   # all DBs, all OS targets
./build.sh --db mysql                        # MySQL only, all OS
./build.sh --db mysql,sqlite --os macos      # MySQL + SQLite for macOS (both arches)
./build.sh --db postgres --os macos --arch aarch64
./build.sh --db mysql --os windows,linux --arch x86_64
```

Artifacts land in `release/` as `flx-nocode-<driver>-<target>` (Windows adds `.exe`; signed macOS produces `.pkg`). With `--db all` (default) a single combined multi‑driver binary is emitted per target as `flx-nocode-<target>` for installer compatibility.

For each driver the script runs `cargo build --release --target <triple> --no-default-features --features <driver>`. macOS signing/notarization activates when `APPLE_ID`, `APPLE_TEAM_ID`, an app‑specific password, `APPLE_IDENTITY`, `APPLE_IDENTITY_INS`, `PRIMARY_BUNDLE_ID`, and `KEYCHAIN_PROFILE` are set; otherwise it just copies the binary.

---

## 20. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Exit: `LOC_CONFIG not set` | Missing env var | Set `LOC_CONFIG` to a config profile path. |
| Panic: `Invalid routes.json` / `ROUTES NOT VALID` | Malformed `routes.json` | Validate JSON; ensure at least one route. |
| Panic: `Cannot read entity file` | Route listed but `entity/<route>.json` missing | Create the file or remove the route. |
| Duplicate table error | Two schemas share the same `table` value | Rename one. |
| `401 Unauthorized` | Missing/invalid `Authorization` header | Re‑login and send `Bearer <token>`. |
| Table not found | Table never created | `POST /generate/table/<route>` (needs `auto_generate: true`) or create it manually. |
| `<backend> feature disabled` | `DB_TYPE` points to a backend not compiled in | Rebuild with that feature, or change `DB_TYPE` ([§18](#18-database-feature-flags-compile-time)). |
| Hooks not running | Used `before`/`after` keys | Use `pre_process` / `post_process` with the `SQL:` prefix ([§11](#11-hooks--validation)). |
| Custom id insert fails | `function_endpoint` unreachable / bad response | Endpoint must return 2xx JSON with the configured field; or clear `function_endpoint` to use `MAX(id)+1` ([§8](#8-custom-id-generation-function)). |

---

## 21. Security checklist

* Use long, random `SECRET_KEY` and `ENCRYPT_KEY`; keep them out of version control.
* Rotate keys periodically (reissue tokens; re‑encrypt data if `ENCRYPT_KEY` changes).
* Grant the database user least privilege.
* Keep the audit log (`LOC_AUDIT`) outside the publicly served `static/` directory.
* Terminate TLS at a reverse proxy (nginx / traefik / Caddy).
* In converter‑token mode, always configure signature verification — avoid `CONVERTER_JWT_INSECURE_SKIP_VERIFY=true` in production.
* Validate any externally‑supplied formula inputs.

---

## 22. Contributing & license

**Contributing**

1. Fork and branch (`feat/<name>`).
2. Make focused commits; keep example configs valid.
3. Run `cargo build` / `cargo test`.
4. Open a PR with a clear description and test notes.

**License** — see [LICENSE](LICENSE) (and [LICENSE-AGPL](LICENSE-AGPL)).

**Credits** — Flexurio Engineering Team. Built with Rust + Actix Web.

---

<div align="center">

### TL;DR

1. Configure `.env` (`DB_TYPE`, URL, `SECRET_KEY`, `ENCRYPT_KEY`, `LOC_CONFIG`).
2. List routes in `routes.json`; add `entity/<route>.json` schemas.
3. Run the binary. 4. *(Optional)* `POST /generate/table/<route>`. 5. `GET /validate/<route>`.
6. Log in, then call the REST endpoints with `Authorization: Bearer <token>`.

Happy building. 🚀

</div>
