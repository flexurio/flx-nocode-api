// 08:00 login storm: Argon2 must run on the blocking pool so GETs stay fast.
// Runs a login burst and a background GET probe at the same time; the probe's
// latency shows whether logins stall the async workers.
import http from "k6/http";
import encoding from "k6/encoding";
import { BASE_URL, ROUTE, headers, ok } from "./lib.js";

const user = __ENV.LOGIN_USER || "admin";
const pass = __ENV.LOGIN_PASS || "wrong-password";

export const options = {
  scenarios: {
    logins: { executor: "constant-arrival-rate", rate: Number(__ENV.RPS || 20), timeUnit: "1s", duration: "30s", preAllocatedVUs: 20, maxVUs: 100, exec: "login" },
    probe: { executor: "constant-arrival-rate", rate: 50, timeUnit: "1s", duration: "30s", preAllocatedVUs: 10, maxVUs: 50, exec: "probe" },
  },
  thresholds: {
    "http_req_duration{scenario:probe}": ["p(95)<15"],
  },
};

export function login() {
  http.post(`${BASE_URL}/login`, null, { headers: { Authorization: `Basic ${encoding.b64encode(`${user}:${pass}`)}` } });
}

export function probe() {
  const res = http.get(`${BASE_URL}/${ROUTE}?limit=1&redis=true`, { headers: headers() });
  ok(res, "probe");
}
