// Write path with validation, custom id, details and triggers. Set BODY_FILE to
// a JSON file matching your entity (default targets master_department).
import http from "k6/http";
import { BASE_URL, headers, ok } from "./lib.js";

const route = __ENV.ROUTE || "master_department";
const bodyTemplate = __ENV.BODY_FILE ? JSON.parse(open(__ENV.BODY_FILE)) : { name: "Dept", code: "D" };

export const options = {
  scenarios: {
    writes: { executor: "constant-arrival-rate", rate: Number(__ENV.RPS || 50), timeUnit: "1s", duration: __ENV.DURATION || "60s", preAllocatedVUs: 20, maxVUs: 200 },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    "http_req_duration{scenario:writes}": ["p(95)<60", "p(99)<150"],
  },
};

export default function () {
  const body = JSON.parse(JSON.stringify(bodyTemplate));
  const n = `${__VU}-${__ITER}-${Date.now()}`;
  for (const k of Object.keys(body)) if (typeof body[k] === "string") body[k] = `${body[k]} ${n}`;
  const res = http.post(`${BASE_URL}/${route}`, JSON.stringify(body), { headers: headers({ "Content-Type": "application/json" }) });
  ok(res, "post");
}
