# Performance scenarios (k6)

Baseline and regression checks for the API. Every scenario carries thresholds
that encode the targets from `docs/PERFORMANCE_ROBUSTNESS_PLAN.md`; a failed
threshold makes k6 exit non-zero, so these can gate CI.

## Run

```bash
# server running on :8080 with REQUIRE_AUTH=false, or export TOKEN=<jwt>
make perf-baseline                # all scenarios, results in docs/perf/<date>/
k6 run perf/k6/get-list-cached.js # one scenario
BASE_URL=http://host:2121 ROUTE=master_employee RPS=1000 k6 run perf/k6/get-list-cached.js
```

Environment: `BASE_URL`, `ROUTE`, `TOKEN` (or `LOGIN_USER`/`LOGIN_PASS`), `RPS`, `DURATION`, `BODY_FILE`.

| Scenario | What it measures | Threshold |
|----------|------------------|-----------|
| `get-list-cached.js` | L1/L2 hit path: middleware, ETag, body copy | p50 < 3 ms, p95 < 10 ms, p99 < 25 ms |
| `get-list-uncached.js` | planner + SQL + row→JSON for a 200-row page | p95 < 25 ms, p99 < 60 ms |
| `post-master-detail.js` | validation, custom id, details, triggers, cache bump | p95 < 60 ms, p99 < 150 ms |
| `login-burst.js` | Argon2 off the async workers: GET probe stays fast during a login storm | probe p95 < 15 ms |
| `mixed.js` | 80/20 read/write ramp to 500 rps | read p95 < 30 ms, write p95 < 80 ms |

## Reading the server side

`GET /metrics` exposes `flx_http_request_duration_seconds{route,method,status}`,
`flx_db_query_duration_seconds{kind}`, `flx_db_pool_{size,idle,max,active}`,
`flx_cache_hits/misses`, `flx_db_timeouts`. Compare the histogram with k6's
client-side numbers to separate server time from network time.
