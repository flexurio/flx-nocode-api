// Shared helpers for the Flexurio k6 scenarios.
import http from "k6/http";
import { check } from "k6";

export const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
export const ROUTE = __ENV.ROUTE || "master_department";
export const TOKEN = __ENV.TOKEN || "";

export function headers(extra = {}) {
  const h = { Accept: "application/json", ...extra };
  if (TOKEN) h.Authorization = `Bearer ${TOKEN}`;
  return h;
}

// Logs in with Basic auth once per VU when TOKEN is not provided.
export function login() {
  if (TOKEN) return TOKEN;
  const user = __ENV.LOGIN_USER || "admin";
  const pass = __ENV.LOGIN_PASS || "";
  if (!pass) return "";
  const res = http.post(`${BASE_URL}/login`, null, {
    headers: { Authorization: `Basic ${encoding.b64encode(`${user}:${pass}`)}` },
  });
  check(res, { "login 200": (r) => r.status === 200 });
  try {
    return res.json("data");
  } catch (_) {
    return "";
  }
}

export function ok(res, name) {
  return check(res, {
    [`${name} status 2xx`]: (r) => r.status >= 200 && r.status < 300,
  });
}
