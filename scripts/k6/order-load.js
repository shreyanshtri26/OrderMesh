// Load test for the Order API. Measures the two numbers SKILL.md section 6 asks for:
//   http_req_duration{name:create_order}   POST /orders latency (p50/p95/p99)
//   time_to_terminal_ms                    POST to CONFIRMED or CANCELLED, through the whole saga
//
// Run (k6 installed):
//   k6 run scripts/k6/order-load.js
// Run (no install, Docker; BASE_URL must be reachable from the container):
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:3001 grafana/k6 run - < scripts/k6/order-load.js
//
// Tunables (environment variables):
//   BASE_URL  default http://localhost:3001
//   SKU       default SKU-TEA-250. Use SKU-SAFFRON-1G with a high VUS to check overselling:
//             exactly one order may end CONFIRMED (stock is 1); `stock.available` must never go below 0.
//   RATE      orders per second, default 20
//   DURATION  default 30s
import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Counter } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const BASE_URL = __ENV.BASE_URL || "http://localhost:3001";
const SKU = __ENV.SKU || "SKU-TEA-250";

const timeToTerminal = new Trend("time_to_terminal_ms", true);
const confirmed = new Counter("orders_confirmed");
const cancelled = new Counter("orders_cancelled");
const unfinished = new Counter("orders_unfinished");

export const options = {
  scenarios: {
    orders: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 20),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    "http_req_duration{name:create_order}": ["p(95)<500"],
    checks: ["rate>0.99"],
  },
};

export default function () {
  const started = Date.now();
  const res = http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({ customerId: `k6-${__VU}`, items: [{ sku: SKU, quantity: 1 }] }),
    {
      headers: { "Content-Type": "application/json", "Idempotency-Key": uuidv4() },
      tags: { name: "create_order" },
    },
  );
  const accepted = check(res, { "order accepted (202)": (r) => r.status === 202 });
  if (!accepted) return;

  const id = res.json("id");
  // Poll until the saga finishes (about 2 s with default latency), up to 15 s.
  for (let i = 0; i < 30; i++) {
    sleep(0.5);
    const order = http.get(`${BASE_URL}/orders/${id}`, { tags: { name: "get_order" } });
    const status = order.status === 200 ? order.json("status") : null;
    if (status === "CONFIRMED" || status === "CANCELLED") {
      timeToTerminal.add(Date.now() - started);
      (status === "CONFIRMED" ? confirmed : cancelled).add(1);
      return;
    }
  }
  unfinished.add(1);
}
