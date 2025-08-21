<div align="center">

# Flexurio No‑Code API Engine

Declarative, database‑driven REST endpoints generated from JSON configuration. Ship CRUD + advanced data operations (GET / POST / PUT / DELETE / PATCH / TRACE) without writing boilerplate code.

</div>

---

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

