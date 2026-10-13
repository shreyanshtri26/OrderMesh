# OrderMesh

**Event-driven order processing: four microservices, Kafka, the transactional outbox and a saga that undoes its own work when payment fails.**

OrderMesh splits an order pipeline into Order, Inventory, Payment and Notification services that talk only through Kafka events. It is built to answer the hard questions of distributed systems in working code: what happens when a service crashes between two writes, when Kafka delivers the same event twice, or when payment fails after stock was already reserved.

[![CI](https://github.com/shreyanshtri26/OrderMesh/actions/workflows/ci.yml/badge.svg)](https://github.com/shreyanshtri26/OrderMesh/actions/workflows/ci.yml)
&nbsp;Live demo: `https://ordermesh.vercel.app` _(Week 5)_

---

## Contents

- [What it demonstrates](#what-it-demonstrates)
- [Features and status](#features-and-status)
- [Architecture](#architecture)
- [The saga](#the-saga)
- [Reliability patterns](#reliability-patterns)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [API](#api)
- [Testing](#testing)
- [Free deployment](#free-deployment)
- [Roadmap](#roadmap)
- [Honest limitations](#honest-limitations)
- [Interview questions this project answers](#interview-questions-this-project-answers)

---

## What it demonstrates

| Concept                                                               | Where to look                                                                                               |
| --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Transactional outbox (the dual-write problem)                         | [`packages/shared/src/outbox.ts`](packages/shared/src/outbox.ts)                                            |
| Idempotent consumers (exactly-once effects on at-least-once delivery) | [`packages/shared/src/consumer.ts`](packages/shared/src/consumer.ts)                                        |
| Retries with backoff and a dead-letter queue                          | [`packages/shared/src/consumer.ts`](packages/shared/src/consumer.ts)                                        |
| Saga orchestration with compensating transactions                     | [`services/order/src/saga.ts`](services/order/src/saga.ts), [`handlers.ts`](services/order/src/handlers.ts) |
| Idempotent HTTP API (`Idempotency-Key`)                               | [`services/order/src/routes.ts`](services/order/src/routes.ts)                                              |
| Database per service                                                  | [`services/*/migrations`](services)                                                                         |
| Distributed tracing across Kafka hops                                 | [`packages/shared/src/tracing.ts`](packages/shared/src/tracing.ts)                                          |
| Graceful degradation when Kafka is down                               | [`packages/shared/src/service.ts`](packages/shared/src/service.ts)                                          |

## Features and status

✅ built and tested in the foundation · 🧑‍💻 yours to build next (spec in the code) · 🚧 on the [roadmap](#roadmap)

| Feature                                                                            | Status                                                                                           |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Shared event contract with zod validation, 4 domain topics                         | ✅                                                                                               |
| Transactional outbox + relay with `FOR UPDATE SKIP LOCKED`                         | ✅ integration-tested                                                                            |
| Idempotent consumer, 3 retries with exponential backoff, DLQ with error headers    | ✅ integration-tested                                                                            |
| Order API: create (202 + `Idempotency-Key`), get with history, list                | ✅                                                                                               |
| Saga orchestrator: state machine, compensation, late events ignored                | ✅ unit- and integration-tested                                                                  |
| Notification service                                                               | ✅                                                                                               |
| Order API keeps accepting orders while Kafka is down                               | ✅ verified                                                                                      |
| OpenTelemetry: HTTP + Postgres auto-instrumented, Kafka context propagated by hand | ✅                                                                                               |
| Docker Compose (Kafka 4.1 KRaft, Postgres, Jaeger), single multi-service image     | ✅ config validated; first full run is part of Week 1                                            |
| Inventory: reserve stock with row locks; release on compensation                   | ✅ integration-tested; live-verified ([code](services/inventory/src/handlers.ts))                |
| Payment: charge through the mock gateway, record the result                        | ✅ idempotent charge (order id as key); live-verified ([code](services/payment/src/handlers.ts)) |
| DLQ inspect-and-replay endpoint (`/admin/dlq`, token-protected)                    | ✅ live-verified                                                                                 |
| Kubernetes manifests (probes, resources) in [`k8s/`](k8s)                          | ✅ written and structure-checked; 🚧 not yet run on kind                                         |
| Storefront page (Vercel, with labelled sample-data mode) and k6 load test script   | ✅ page live; 🚧 k6 numbers not yet measured                                                     |
| Hosted deployment (Aiven + Neon + Render)                                          | 🚧 prepared: `render.yaml`, `scripts/deploy-site.mjs`; needs your Aiven and Render accounts      |

## Architecture

```
                         POST /orders (Idempotency-Key)
                                   │
                                   ▼
┌──────────────────────────── Order service ────────────────────────────┐
│  ① INSERT order  +  INSERT outbox(OrderCreated)   ← one transaction   │
│  ② relay: outbox ──► Kafka                                            │
│  ③ saga: reacts to inventory/payment events, emits the next command   │
└───────────────┬───────────────────────────────────────▲───────────────┘
                │ topic: orders                         │ topics: inventory, payments
      ┌─────────┼──────────────────┬─────────────┐      │
      ▼         ▼                  ▼             │      │
┌───────────┐ ┌───────────┐ ┌──────────────┐     │      │
│ Inventory │ │  Payment  │ │ Notification │     │      │
│  stock DB │ │ payments  │ │ notifications│     │      │
└─────┬─────┘ └─────┬─────┘ └──────────────┘     │      │
      │ inventory   │ payments                   │      │
      └─────────────┴────────────────────────────┴──────┘

Each service: own Postgres database · own outbox · own processed_events · own consumer group
Every hop carries a W3C traceparent → one order = one trace in Jaeger
```

**Topics.** Aiven's free Kafka allows 5 topics, so there is one topic per domain (`orders`, `inventory`, `payments`) plus `dlq`, with the event type inside the envelope. Each message is keyed by order id, so every event for one order lands on the same partition and stays in order.

## The saga

The Order service orchestrates. It is a pure state machine ([`saga.ts`](services/order/src/saga.ts)) applied inside a database transaction ([`handlers.ts`](services/order/src/handlers.ts)):

```
PENDING ──InventoryReserved──► INVENTORY_RESERVED ──PaymentCompleted──► CONFIRMED
   │                                   │
   └─InventoryRejected─► CANCELLED     └─PaymentFailed─► CANCELLING ──InventoryReleased──► CANCELLED
                                                         (emits ReleaseInventoryRequested:
                                                          the compensating transaction)
```

Any other combination of state and event is ignored and logged. That makes late or out-of-order events harmless: a `PaymentCompleted` that arrives after an order was cancelled cannot resurrect it.

## Reliability patterns

| Failure                                                              | What happens                                               | Mechanism                                                                                  |
| -------------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Service crashes after writing the order, before publishing           | The event is still published once the service restarts     | Order row and outbox row commit in one transaction                                         |
| Relay crashes after publishing, before marking the row               | The event is published again                               | At-least-once relay; consumers dedupe                                                      |
| Kafka delivers the same event twice                                  | The second delivery does nothing                           | `processed_events (event_id, consumer)` primary key, inserted in the handler's transaction |
| A handler fails                                                      | Rolled back and retried 3 times with backoff               | Marker and effects roll back together                                                      |
| A message can never succeed (bad JSON, bad schema, persistent error) | Sent to `dlq` with the error, partition keeps moving       | DLQ headers: original topic, partition, offset, consumer, attempts, error                  |
| Kafka is down                                                        | Orders are still accepted (202); events wait in the outbox | HTTP starts before Kafka; Kafka connects in the background with retries                    |
| Client retries `POST /orders`                                        | Same order returned, no duplicate                          | Unique `idempotency_key` column                                                            |
| Two events for one order arrive together                             | Processed one after the other                              | `SELECT … FOR UPDATE` on the order row                                                     |

## Tech stack

Every hosted service has a free tier that needs **no credit card**.

| Layer        | Choice                                                                       | Why                                                  |
| ------------ | ---------------------------------------------------------------------------- | ---------------------------------------------------- |
| Runtime      | Node.js 22 running TypeScript directly (type stripping)                      | No build step, no `dist/` to drift from `src/`       |
| HTTP         | Fastify 5                                                                    | Fast, schema-friendly, built-in structured logging   |
| Messaging    | Kafka 4.1 (KRaft); [Aiven free tier](https://aiven.io/free-tier) when hosted | Real Kafka, managed, no card                         |
| Kafka client | `@confluentinc/kafka-javascript` (librdkafka, KafkaJS-compatible API)        | KafkaJS is unmaintained and cannot talk to Kafka 4.x |
| Database     | PostgreSQL (one database per service); [Neon](https://neon.com) when hosted  | Transactions and constraints carry the guarantees    |
| Validation   | zod                                                                          | One schema per event, checked on both ends           |
| Tracing      | OpenTelemetry → Jaeger                                                       | Follow one order across four services                |
| Tests        | Vitest; embedded Postgres for integration tests                              | Integration tests run without Docker                 |
| Packaging    | pnpm workspaces, one Docker image, Docker Compose                            | One `docker compose up` for the whole stack          |

## Project structure

```
packages/shared/src/
  events.ts        Event envelope, payload schemas, topic map
  outbox.ts        appendToOutbox() and the relay
  consumer.ts      Idempotent consumer, retries, DLQ
  dlq.ts           Read and replay dead-letter entries
  db.ts            Pool, transactions, migration runner (+ outbox/processed_events tables)
  kafka.ts         Kafka client, idempotent producer
  tracing.ts       Trace context across Kafka
  service.ts       Boot sequence and graceful shutdown shared by all services
  http.ts          Fastify with /health and /ready
  catalog.ts       Demo products and prices
services/
  order/           Public API + saga orchestrator   (port 3001)
  inventory/       Stock reservation                (port 3002)
  payment/         Mock gateway + charging          (port 3003)
  notification/    Customer notifications           (port 3004)
  */migrations/    SQL owned by that service only
  */test/          Unit (*.test.ts) and integration (*.int.test.ts) tests
scripts/start-all.ts   Runs all four services in one container (Render free tier)
scripts/k6/            Load test for the Order API
k8s/                   Kubernetes manifests (infra.yaml, services.yaml)
infra/postgres/        Local database init
docker-compose.yml     Kafka, Postgres, Jaeger (+ services with --profile app)
Dockerfile             One image; SERVICE=order|inventory|payment|notification|all
SKILL.md               Engineering log: decisions, numbers, bugs, interview answers
```

## Getting started

**Prerequisites:** Node.js 22.18+, pnpm 10+ (`npm i -g pnpm`), Docker Desktop.

```bash
git clone https://github.com/shreyanshtri26/OrderMesh.git
cd OrderMesh
pnpm install
cp .env.example .env          # defaults already match docker compose
docker compose up -d          # Kafka, Postgres, Jaeger
pnpm dev                      # all four services with auto-reload
```

Then place an order and watch it move:

```bash
curl -s -X POST localhost:3001/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: demo-1' \
  -d '{"customerId":"c-42","items":[{"sku":"SKU-TEA-250","quantity":2}]}'

curl -s localhost:3001/orders/<id>     # status + history
```

Open Jaeger at <http://localhost:16686> to see the trace across services.

> Orders run the full saga: stock is reserved, the payment is charged, and the order ends `CONFIRMED`, or `CANCELLED` with its stock released when the card is declined.

**Run everything in containers instead:** `docker compose --profile app up --build`.

**Use Neon instead of the local Postgres:** put each service's Neon connection string in `.env` (`ORDER_DATABASE_URL`, `INVENTORY_DATABASE_URL`, `PAYMENT_DATABASE_URL`, `NOTIFICATION_DATABASE_URL`; one database per service, see `.env.example`), then:

```bash
docker compose -f docker-compose.yml -f docker-compose.neon.yml --profile app up --build -d
```

The services run their migrations against Neon on start. Expect a slower saga than locally (several seconds, not one or two) when the services run far from the database: every hop makes database round trips, and the first request after Neon scales to zero includes a cold start. Running the services in the same region as the Neon project removes most of that.

> On Windows, Postgres is published on host port `15432` (Windows often reserves 5430-5529). Containers still reach it as `postgres:5432`.

**Inspect and replay dead letters** (the token defaults to `dev-admin-token` in compose; the routes are off when `ADMIN_TOKEN` is unset):

```bash
curl -H "Authorization: Bearer dev-admin-token" http://localhost:3001/admin/dlq
curl -X POST -H "Authorization: Bearer dev-admin-token" -H "Content-Type: application/json"   -d '{"partition":0,"offset":0}' http://localhost:3001/admin/dlq/replay
```

Replay is safe because consumers are idempotent. A message that can never succeed (bad JSON) lands in the DLQ again.

**Kubernetes (kind):**

```bash
docker build -t ordermesh:latest .
kind create cluster && kind load docker-image ordermesh:latest
kubectl create namespace ordermesh
kubectl -n ordermesh create configmap postgres-init --from-file=infra/postgres/init.sql
kubectl apply -f k8s/infra.yaml && kubectl apply -f k8s/services.yaml
kubectl -n ordermesh port-forward svc/order 3001:3001
```

**Load test** (k6, or the Docker form in the script header): `k6 run scripts/k6/order-load.js`.

**Try the guarantees yourself:**

- Stop Kafka (`docker compose stop kafka`), place orders (still `202`), start it again and watch the outbox drain.
- Send the same `POST` twice with one `Idempotency-Key`: the second returns `200` with the same order.
- Set `PAYMENT_FAILURE_RATE=1` and watch every order compensate to `CANCELLED` with its stock restored.

## API

| Method | Path                  | Description                                                                             |
| ------ | --------------------- | --------------------------------------------------------------------------------------- |
| `POST` | `/orders`             | Create an order. `202` with the order (`PENDING`). Optional `Idempotency-Key` header.   |
| `GET`  | `/orders/:id`         | Order with status, cancel reason and saga history                                       |
| `GET`  | `/orders`             | 20 most recent orders                                                                   |
| `GET`  | `/catalog`            | Demo products and prices (paise)                                                        |
| `GET`  | `:3002/stock`         | Current stock levels                                                                    |
| `GET`  | `:3004/notifications` | Recent customer notifications                                                           |
| `GET`  | `/health`, `/ready`   | Liveness; readiness (database) on every service                                         |
| `GET`  | `/admin/dlq`          | Dead-letter entries with origin, consumer, attempts and error. Needs `ADMIN_TOKEN`.     |
| `POST` | `/admin/dlq/replay`   | Body `{partition, offset}`: republish one entry to its original topic. Needs the token. |

## Testing

```bash
pnpm test               # unit: events, saga, backoff, DLQ, gateway
pnpm test:integration   # real Postgres: outbox atomicity, SKIP LOCKED, dedupe, saga end to end
pnpm typecheck
pnpm format:check
```

Integration tests start an **embedded Postgres** from npm, so they need no Docker. To use your own database instead, set `TEST_DATABASE_URL`.

## Free deployment

| Piece     | Where                                  | Notes                                                                                                     |
| --------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Kafka     | Aiven free tier                        | 5 topics × 2 partitions, ~125 KiB/s each way; powers off after long inactivity (restart from the console) |
| Databases | Neon, one database per service         | Scale to zero when idle                                                                                   |
| Services  | Render free web service, `SERVICE=all` | All four run in one container (`scripts/start-all.ts`); sleeps after 15 min idle, ~1 min to wake          |
| Demo page | Vercel                                 | Proxies `/catalog` and `/orders` to the Order API with rewrites; sample-data mode when it is unreachable  |

Running four services in one container is a deployment compromise for the free tier, not an architecture change: each service is still its own process, with its own database and consumer group. Locally and in Kubernetes they run separately.

**Go live (about 20 minutes, no card needed):**

1. **Kafka (Aiven).** Create a free Kafka service. Create four topics with 2 partitions each: `orders`, `inventory`, `payments`, `dlq`. Note the service URI (host:port), the `avnadmin` password and the CA certificate.
2. **Databases (Neon).** Already covered above: one database per service, pooled connection strings.
3. **Services (Render).** Push this repo to GitHub, then New > Blueprint and pick it. Render reads [`render.yaml`](render.yaml) and asks for the values it marks as secret: the Kafka URI, SASL username and password, CA certificate (paste with real line breaks), and the four database URLs.
4. **Demo page (Vercel).** Point the page at the Render URL:

   ```bash
   API_URL=https://<your-service>.onrender.com node scripts/deploy-site.mjs
   ```

   Only `/catalog`, `/orders` and `/orders/:id` are forwarded, so `/admin/dlq` is never reachable through the site.

Measured locally with the real image at Render's limits (details in `SKILL.md`): four services fit in **297 MiB of 512**, and cold start takes about **70 s at 0.1 CPU** (15-25 s at 0.5 CPU). The saga is also slow there (tens of seconds per order, minutes for a burst) because of the CPU share and database distance. Not exercised: the Render and Aiven steps themselves.

## Roadmap

| Week | Goal                     | Done when                                                                                      |
| ---- | ------------------------ | ---------------------------------------------------------------------------------------------- |
| 1    | Foundation (this commit) | CI green; unit + integration tests pass; stack starts with `docker compose up`                 |
| 2    | Inventory                | Two concurrent orders for the last saffron (stock 1): exactly one reserved, one rejected       |
| 3    | Payment + compensation   | `PAYMENT_FAILURE_RATE=1` → every order ends `CANCELLED` with stock restored                    |
| 4    | Operations               | DLQ inspect/replay endpoint; Kubernetes manifests running on kind; Jaeger screenshot in README |
| 5    | Ship                     | Aiven + Neon + Render + Vercel demo; k6 numbers (p50/p95/p99 order latency) in SKILL.md        |

## Honest limitations

- **Polling relay.** The outbox is polled every 500 ms. Change data capture (Debezium) would cut latency but needs Kafka Connect, which the free tier does not include.
- **The payment gateway call is outside the database transaction.** A crash between charging and committing is the classic gap; real gateways close it with an idempotency key per order (see the Week 3 spec).
- **Single region, single broker locally.** No cross-region replication or multi-broker failover is exercised.
- **Free-tier cold starts.** The hosted demo can take about a minute to wake.

## Interview questions this project answers

1. Your service writes to Postgres and publishes to Kafka. The process crashes between the two. What happens, and how does the outbox fix it?
2. Payment succeeded but Inventory has no stock. Describe the compensating flow in order.
3. What did you lose by moving from a monolith to microservices? Be honest.
4. Kafka delivers the same event three times. How many times is the customer notified, and why?

Rehearsed answers live in [`SKILL.md`](SKILL.md#9-interview-answers-i-have-rehearsed).

## Credits

Project brief adapted from _The Resume Project Vault 2026_ (B3, Event-Driven Order Processing with Microservices) by @pratham.codes.

## License

[MIT](LICENSE)
