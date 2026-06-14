// k6 ingestion load test for Hookline.
//
//   k6 run -e BASE=http://localhost:8080 -e API_KEY=dev-key \
//          -e TARGET=http://localhost:9100/hook loadtest/k6/ingest.js
//
// Ramps to 50 virtual users submitting events to POST /v1/events and asserts
// p95 latency and a near-zero error rate. The Go program in loadtest/loadgen is
// an equivalent that needs no k6 install.
import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

const errors = new Rate("errors");

const BASE = __ENV.BASE || "http://localhost:8080";
const API_KEY = __ENV.API_KEY || "dev-key";
const TARGET = __ENV.TARGET || "http://localhost:9100/hook";

export const options = {
  stages: [
    { duration: "10s", target: 50 },
    { duration: "30s", target: 50 },
    { duration: "10s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<50"],
    errors: ["rate<0.01"],
  },
};

export default function () {
  const res = http.post(
    `${BASE}/v1/events`,
    JSON.stringify({ endpoint: TARGET, payload: { hello: "load" } }),
    { headers: { "Content-Type": "application/json", Authorization: `Bearer ${API_KEY}` } }
  );
  const ok = check(res, { "status is 202": (r) => r.status === 202 });
  errors.add(!ok);
}
