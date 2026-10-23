// 80 % reads / 20 % writes, the shape of a normal ERP working hour.
import http from "k6/http";
import { BASE_URL, ROUTE, headers, ok } from "./lib.js";

export const options = {
  scenarios: {
    mixed: { executor: "ramping-arrival-rate", startRate: 50, timeUnit: "1s", preAllocatedVUs: 50, maxVUs: 500,
      stages: [ { target: 200, duration: "30s" }, { target: 500, duration: "60s" }, { target: 0, duration: "15s" } ] },
  },
  thresholds: {
    http_req_failed: ["rate<0.005"],
    "http_req_duration{kind:read}": ["p(95)<30"],
    "http_req_duration{kind:write}": ["p(95)<80"],
  },
};

export default function () {
  if (Math.random() < 0.8) {
    const res = http.get(`${BASE_URL}/${ROUTE}?limit=50&page=${1 + (__ITER % 5)}`, { headers: headers(), tags: { kind: "read" } });
    ok(res, "read");
  } else {
    const body = JSON.stringify({ name: `Dept ${__VU}-${__ITER}`, code: `D${__VU}${__ITER}` });
    const res = http.post(`${BASE_URL}/${ROUTE}`, body, { headers: headers({ "Content-Type": "application/json" }), tags: { kind: "write" } });
    ok(res, "write");
  }
}
