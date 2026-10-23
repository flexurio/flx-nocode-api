// Unique query string per request so every call misses the cache and hits the
// database: measures planner + SQL + row conversion + serialization.
import http from "k6/http";
import { BASE_URL, ROUTE, headers, ok } from "./lib.js";

export const options = {
  scenarios: {
    uncached: { executor: "constant-arrival-rate", rate: Number(__ENV.RPS || 200), timeUnit: "1s", duration: __ENV.DURATION || "60s", preAllocatedVUs: 50, maxVUs: 400 },
  },
  thresholds: {
    http_req_failed: ["rate<0.001"],
    "http_req_duration{scenario:uncached}": ["p(95)<25", "p(99)<60"],
  },
};

export default function () {
  // `page` varies the key; `limit=200` is the typical ERP grid page.
  const page = 1 + (__ITER % 50);
  const res = http.get(`${BASE_URL}/${ROUTE}?limit=200&page=${page}&redis=false`, { headers: headers() });
  ok(res, "get uncached");
}
