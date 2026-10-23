// Hot page served from L1/L2: measures the cache-hit path (serialization,
// middleware, ETag). Expect sub-millisecond p50 after Phase 1.
import http from "k6/http";
import { BASE_URL, ROUTE, headers, ok } from "./lib.js";

export const options = {
  scenarios: {
    cached: { executor: "constant-arrival-rate", rate: Number(__ENV.RPS || 500), timeUnit: "1s", duration: __ENV.DURATION || "60s", preAllocatedVUs: 50, maxVUs: 400 },
  },
  thresholds: {
    http_req_failed: ["rate<0.001"],
    "http_req_duration{scenario:cached}": ["p(50)<3", "p(95)<10", "p(99)<25"],
  },
};

export default function () {
  const res = http.get(`${BASE_URL}/${ROUTE}?limit=50&redis=true`, { headers: headers() });
  ok(res, "get cached");
}
