<div align="center">

# Flexurio No‑Code API Engine

Declarative, database‑driven REST endpoints generated from JSON configuration. Ship CRUD + advanced data operations (GET / POST / PUT / DELETE / PATCH / TRACE) without writing boilerplate code.

</div>

---

## Installation & Usage

Quick guide to install and run the Flexurio No‑Code API on macOS/Linux using the built‑in installer script or Docker, plus how to log in and access endpoints.

### Requirements
- macOS or Linux (Windows works via WSL).
- Database: MySQL/PostgreSQL/SQLite (recommended to start with SQLite for a quick try).
- Rust toolchain only if you want to build from source (optional).

### 1) Install Flexurio
#### LINUX or MAC
- Run `install-flexurio.sh` to install the binary into `~/.local/bin` and create the `flexurio` command.
- After it finishes, reload your shell (e.g., `source ~/.zshrc`).
- The `flexurio` command automatically reads the `.env` file in the current working directory when executed.

#### WINDOWS
- Open PowerShell in the repo folder and run:
- Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
- Run .\install-flexurio.ps1
- Open a new terminal, then run: flexurio


Note: The script detects your OS architecture and downloads the latest release binary from GitHub for your platform.

### 2) Prepare the `.env` configuration
- Copy the example file: `cp env .env` and edit the values.
- Minimum values to set:
  - `DB_TYPE` and its connection URL (quick start: `DB_TYPE=sqlite` and `SQLITE_URL=sqlite://data.db`).
  - `SECRET_KEY` for JWT signing, and `ENCRYPT_KEY` for column encryption.
  - `LOC_CONFIG` pointing to a config profile, e.g. `config/example`, `config/pos`, or `config/tms`.
- Avoid duplicate keys in `.env` (choose only one `DB_TYPE` and one matching URL).

Minimal example (SQLite, quick start):

```
DB_TYPE=sqlite
SQLITE_URL=sqlite://data.db
LOC_CONFIG="config/example"
SECRET_KEY=replace_with_a_long_secret
ENCRYPT_KEY=replace_with_another_secret
PORT=8080
DEBUG=True
LOGGING=True
LOC_STATIC=static
LOC_IMAGE=images
LOC_LOGGING=static/log
```

### 3) Run the server
Pick one method:
- Using the installer: go to the project folder that contains `.env`, then run `flexurio`.
- Using a release binary: pick the file under `release/` that matches your OS and run it.
- Build from source: `cargo build --release` then run `./target/release/flx-nocode-api`.

On the first start:
- The app ensures core tables (`flx_users`, `flx_roles`) exist. If not, they will be created.
- If no admin exists, the system creates a default admin user with email `admin` and a random 4‑digit password printed to the console: "Your admin Password: 1234". Save this password.

### 4) Run via Docker (optional)
Use `docker-compose.yaml` as a reference. Adjust volumes for your local paths. Example for macOS from the project root:

```
services:
  rust-app:
    build: .
    container_name: flx-nocode-api
    restart: always
    ports:
      - "2121:8080"    # access at http://localhost:2121
    volumes:
      - "./static:/app/static"
      - "./config:/app/config"
      - "./.env:/app/.env"
```

### 5) Configuration structure (LOC_CONFIG)
- `routes.json` contains the list of enabled routes and those that are public.
- The `entity/` folder contains `<route>.json` files describing the table schema and CRUD behavior.
- Choose a sample profile under `config/example`, `config/pos`, or `config/tms`, or create your own.

Steps to add a new route:
1. Add the route name to the `routes` array in `LOC_CONFIG/routes.json`.
2. Create `LOC_CONFIG/entity/<route>.json` matching the target table and desired behavior.
3. (Optional) Call `POST /generate/table/<route>` if the physical table does not exist.
4. Validate the schema with `GET /validate/<route>`.

### 6) Login and authorization
- Login endpoint: `POST /login` using the Basic Authorization header (`Basic base64(email:password)`).
- Default admin: email `admin`, password is printed on first start (see console logs).
- After login you will receive a JWT. Include it in subsequent requests: `Authorization: Bearer <token>`.
- Public endpoints are defined in `route_publics` inside `routes.json`. IP whitelist via `WHITE_LIST_IP`.

### 7) Accessing data endpoints
For each active `route`:
- `GET /<route>`: read data (supports query parameters per schema).
- `POST /<route>`: create data (supports multipart/form‑data for file uploads).
- `PUT /<route>/:id` and `DELETE /<route>/:id`.
- `PATCH /<route>` and `TRACE /<route>` for custom flows (stored procedure/pipeline) as defined in the schema.
- `GET /validate/<route>` to validate the schema.

Tips:
- For extra steps before/after INSERT/UPDATE, use `post.before`, `post.after`, `put.before`, `put.after` in the schema with the `SQL:` prefix.
- Use placeholders like `{request.field}` so values are safely bound (SQL injection safe).

### 8) Integration with Flexurio
- This repository is Flexurio’s No‑Code API engine. For real‑world config examples, see profiles under `config/pos`, `config/tms`, or `configmftl`.
- Use `CUSTOME_JWT_QUERY` in `.env` to add custom claims to the JWT after login.

If you need help, open an issue on GitHub or contact the Flexurio team.

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
* Static file serving under `/static` (e.g. images, logs).
* Rate Limit per request per minutes

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
| PRIMARY_BUNDLE_ID | No | Optional primary bundle identifier for notarization. |

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

---

## 18. Database Feature Flags (Compile-Time)

Starting from version 0.1.18+, database backends are controlled via Cargo features so you can build a lighter binary containing only what you need.

### 18.1 Available Features

Enabled by default (all three):
```
mysql
postgres
sqlite
```

`Cargo.toml` excerpt:
```toml
[features]
default = ["mysql", "postgres", "sqlite"]
mysql   = ["sqlx/mysql", "sqlx/chrono"]
postgres= ["sqlx/postgres", "sqlx/chrono"]
sqlite  = ["sqlx/sqlite", "sqlx/chrono"]
```

If you disable a feature and still set `DB_TYPE` at runtime to that backend, the application will exit with an error like `mysql feature disabled`.

### 18.2 Build / Run Examples

Build with only MySQL:
```bash
cargo build --release --no-default-features --features mysql
```

Run (after build):
```bash
DB_TYPE=mysql MYSQL_URL="mysql://user:pass@localhost/db" \
SECRET_KEY=change_me ENCRYPT_KEY=change_me \
LOC_CONFIG=config/example \
./target/release/flx-nocode-api
```

Build with MySQL + SQLite:
```bash
cargo build --release --no-default-features --features "mysql sqlite"
```

Build with only PostgreSQL:
```bash
cargo build --release --no-default-features --features postgres
```

Build with everything (default):
```bash
cargo build --release
```

### 18.3 Run Directly via cargo run

You can also compile & run in one step:
```bash
cargo run --no-default-features --features mysql --release
```

Or with multiple:
```bash
cargo run --no-default-features --features "mysql sqlite" --release
```

If you omit `--no-default-features`, all backends are included.

### 18.4 Why Use Feature Flags?
* Faster compile time when you only target one backend.
* Smaller binary footprint.
* Reduced supply surface (fewer unused code paths in production).

### 18.5 Troubleshooting
| Symptom | Cause | Fix |
|---------|-------|-----|
| `feature 'postgres' not enabled at compile time` | You set `DB_TYPE=postgres` but didn't enable the feature | Rebuild with `--features postgres` or change `DB_TYPE`. |
| `could not find sqlx::mysql` compile error | Removed mysql feature from default and code still referencing it | Ensure the `#[cfg(feature="mysql")]` guards are present (already applied) and rebuild with proper features. |
| Runtime exit `Unsupported DB_TYPE` | Typo in `DB_TYPE` or feature disabled | Verify `.env` and enabled feature list. |

---

## 19. Multi-Target Build Script (`build.sh`)

The `build.sh` script produces per-database, per-OS binaries with feature-gated builds so each artifact contains only the selected backend. It also optionally signs/notarizes macOS binaries if Apple credentials are present.

Note: Use `./build.sh` or `bash build.sh` (not `sh build.sh`). macOS ships an older Bash (3.2) that lacks associative arrays, so the script avoids them for portability. Export any Apple signing credentials in your shell or CI environment rather than editing the script.

### 19.1 New Flag Syntax

Usage:
```bash
./build.sh [--db <list>] [--os <list>] [--arch <list>] [--help]
```

Flags:
* `--db <list>`: Comma-separated database drivers: `mysql,postgres,sqlite,all` (default: `all`).
* `--os <list>`: Comma-separated OS groups: `macos,windows,linux,all` (default: `all`).
* `--arch <list>`: Comma-separated architectures: `x86_64,aarch64,all` (filters after OS expansion, default: `all`).
* `--help`: Show help text.

OS group expansion:
* `macos` -> `x86_64-apple-darwin`, `aarch64-apple-darwin`
* `windows` -> `x86_64-pc-windows-gnu`
* `linux` -> `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`

Architecture filtering (optional):
After expanding OS groups you can further restrict with `--arch`.

Examples:
```bash
# All DBs for all OS targets
./build.sh

# Only MySQL (all OS targets)
./build.sh --db mysql

# MySQL + SQLite only for macOS (both architectures)
./build.sh --db mysql,sqlite --os macos

# PostgreSQL only for macOS on Apple Silicon
./build.sh --db postgres --os macos --arch aarch64

# All DBs for Linux only (both arches)
./build.sh --db all --os linux

# Only MySQL for Windows + Linux x86_64
./build.sh --db mysql --os windows,linux --arch x86_64

# PostgreSQL only for Linux
./build.sh --db postgres --os linux

# All DBs for macOS + Windows
./build.sh --db all --os macos,windows
```

### 19.2 Output Artifacts

Artifacts are written to `release/` with the pattern:
```
flx-nocode-<driver>-<target>
```
Windows executables have `.exe`. macOS installer packages (if signing succeeds) follow:
```
flx-nocode-<driver>-<target>.pkg
```

When you use `--db all` (default) the script produces a single combined binary per target containing all database drivers, using the legacy naming without the driver segment:
```
flx-nocode-<target>
```
and for macOS pkg (if signed):
```
flx-nocode-<target>.pkg
```
This preserves compatibility with existing installer scripts expecting a generic multi-driver binary.

### 19.3 Feature Selection Logic

For each chosen driver the script builds with:
```bash
cargo build --release --target <triple> --no-default-features --features <driver>
```

### 19.4 macOS Signing & Notarization (Optional)

If these environment variables are set the script will attempt to sign and notarize the binary and produce a signed `.pkg`:
* `APPLE_ID`, `APPLE_TEAM_ID`, `PASSWORD` or `APPLE_APP_SPECIFIC_PASSWORD`
* `APPLE_IDENTITY` (codesign identity)
* `APPLE_IDENTITY_INS` (installer identity)
* `PRIMARY_BUNDLE_ID`
* `KEYCHAIN_PROFILE` (for `notarytool` stored credentials)

If signing variables are absent the script simply copies/renames the compiled binary.

### 19.5 Backward Compatibility

Previous versions accepted a positional driver list (e.g. `./build.sh mysql,sqlite`). This still works if you pass it as `--db mysql,sqlite`. Pure positional usage has been deprecated in favor of explicit flags for clarity and future extensibility (e.g. adding `--arch`).

### 19.6 Troubleshooting
| Symptom | Cause | Fix |
|---------|-------|-----|
| `Unknown driver` | Typo in `--db` list | Use only `mysql,postgres,sqlite` |
| `Unknown OS` | Typo in `--os` list | Use only `macos,windows,linux` |
| `No targets resolved` | Empty expansion due to bad `--os` value | Correct the OS names |
| Build fails linking for target | Missing target toolchain | `rustup target add <triple>` |
| macOS signing errors | Missing/invalid Apple credentials | Export required env vars or skip signing |

---

