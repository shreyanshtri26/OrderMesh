---
project: OrderMesh
track: backend
level: advanced
started: 2026-10-12
shipped: <YYYY-MM-DD>
repo: https://github.com/shreyanshtri26/OrderMesh
live: https://ordermesh.vercel.app
---

> **How to use this file.** This is the engineering log from _The Resume Project
> Vault_ method. Sections 1 to 4 were drafted during setup. **Rewrite them in
> your own words**, then fill in sections 5 to 10 _while_ you build, the same day
> something happens. If you cannot explain a line here out loud, it does not go
> on your resume yet.

# 1. What this project is

**To a friend:** when you order something online, several systems have to agree:
the shop, the warehouse, the payment provider, the email sender. OrderMesh is
that pipeline, built so nothing gets lost or done twice even when one of those
systems crashes, and so a failed payment puts the reserved stock back.

**To an engineer:** four Node.js services with a database each, communicating
over Kafka. Writes use the transactional outbox; consumers are idempotent with
retries and a DLQ; the Order service orchestrates a saga with compensation;
OpenTelemetry carries one trace across every Kafka hop.

# 2. Problem it solves

Splitting a system into services trades one database transaction for many
independent ones. That creates three problems a monolith never had: a write and
its event can diverge (dual write), messages arrive more than once (at-least-once
delivery), and a multi-step business process can fail halfway (no distributed
transaction to roll back). OrderMesh solves each with a standard pattern and
proves it with tests against a real database.

# 3. Architecture

See the diagrams in [README.md](README.md#architecture).

Components:

- Order service → public API and saga orchestrator → orchestration keeps the whole flow readable in one state machine
- Inventory service → stock reservation and release → stock must be locked where it lives
- Payment service → charging through a mock gateway → failures can be produced on demand
- Notification service → tells the customer the outcome → the simplest consumer, and the easiest place to verify idempotency
- Kafka → event transport → durable, ordered per key, replayable; consumer groups give each service its own cursor
- Postgres per service → state + outbox + processed_events → the guarantees come from transactions and unique keys
- Jaeger → trace viewer → shows one order's path across all four services

# 4. Key decisions and trade-offs

| Decision           | Options I considered                                         | What I chose                                    | Why                                                        | What I gave up                          |
| ------------------ | ------------------------------------------------------------ | ----------------------------------------------- | ---------------------------------------------------------- | --------------------------------------- |
| Saga style         | Choreography; orchestration                                  | Orchestration in Order                          | The flow lives in one state machine I can test and explain | Order is coupled to every step          |
| Publishing events  | Publish after commit; outbox + polling relay; CDC (Debezium) | Outbox + polling relay                          | Removes the dual write with no extra infrastructure        | ~500 ms latency; polling load on the DB |
| Duplicate handling | Rely on Kafka exactly-once; dedupe table                     | `processed_events` in the handler's transaction | Works for side effects in Postgres; simple to reason about | One extra write per event               |
| Topics             | One per event type; one per domain                           | One per domain + `dlq` (4 of 5)                 | Fits Aiven's free-tier limit of 5 topics                   | Consumers see events they ignore        |
| Kafka client       | KafkaJS; Confluent's librdkafka client                       | Confluent client                                | KafkaJS is unmaintained and incompatible with Kafka 4.x    | Native dependency; glibc base image     |
| Build              | `tsc` to `dist/`; Node type stripping                        | Type stripping                                  | No build step; source is what runs                         | Erasable syntax only (no enums)         |
| Startup            | Wait for Kafka; start HTTP first                             | HTTP first, Kafka in background                 | Orders are accepted during a Kafka outage                  | `/ready` must not depend on Kafka       |
|                    |                                                              |                                                 |                                                            |                                         |

# 5. Skills demonstrated

- [ ] Event-driven architecture with Kafka, evidence: `packages/shared/src/events.ts`, `docker-compose.yml`
- [ ] Transactional outbox, evidence: `packages/shared/src/outbox.ts` + `outbox-consumer.int.test.ts`
- [ ] Idempotent consumers, retries and DLQ, evidence: `packages/shared/src/consumer.ts`
- [ ] Saga with compensating transactions, evidence: `services/order/src/saga.ts` + `saga.int.test.ts`
- [ ] Pessimistic locking for stock, evidence: `services/inventory/src/handlers.ts` (Week 2)
- [ ] Distributed tracing, evidence: Jaeger screenshot in README (Week 4)
- [ ] Docker and Kubernetes, evidence: `Dockerfile`, `k8s/` (Week 4)

# 6. Numbers I measured

Fill these in only with numbers you measured yourself.

| Metric                                                 | Before | After | How I measured it                     |
| ------------------------------------------------------ | ------ | ----- | ------------------------------------- |
| Duplicate notifications after replaying 100 events 3×  |        | 0     | SQL count after replay                |
| Orders accepted while Kafka was down for 60 s          |        |       | k6 during `docker compose stop kafka` |
| Oversold units with 50 concurrent orders for stock = 1 |        | 0     | k6 burst, then check `stock`          |
| p95 POST /orders latency at 100 RPS                    |        |       | k6                                    |
| Time from POST to CONFIRMED (p95)                      |        |       | timestamps in `order_status_history`  |

# 7. Things that broke and how I fixed them

1. Symptom: the producer threw `Cannot read properties of undefined (reading 'mechanism')` against a local broker.
   Cause: the Kafka client checks whether the `sasl` key exists, so `sasl: undefined` still turned SASL on.
   Fix: only add the `sasl` key when SASL is configured (`packages/shared/src/kafka.ts`).
   Lesson: for config objects, an absent key and an `undefined` value are not the same thing.

2. Symptom:
   Cause:
   Fix:
   Lesson:

# Verification log (Weeks 2 and 3)

Run on the local Docker stack, `docker compose --profile app up --build`:

- **Two concurrent orders for the last saffron (stock 1):** one `CANCELLED` with `out_of_stock:SKU-SAFFRON-1G`, one `CONFIRMED`. Final stock: `available 0, reserved 1`. Also covered by `services/inventory/test/reservation.int.test.ts`, which runs the two reservations in parallel against Postgres.
- **`PAYMENT_FAILURE_RATE=1`, three orders of 2 kg rice:** all three `CANCELLED` with `card_declined`. Stock before and after: `20 / 0`. The history for one order reads `INVENTORY_RESERVED → CANCELLING (PaymentFailed) → CANCELLED (InventoryReleased)`.
- **Default failure rate (0.2), six orders:** 3 `CONFIRMED`, 3 `CANCELLED` (declined at random).

## Deployment-shape benchmark

The real image, all four services in one container (`SERVICE=all`, as on Render's free tier), capped at 512 MB, against Neon in `aws-ap-southeast-1`, with Kafka local, driven from a Windows laptop in India. Measured, not estimated; n is small and the network was noisy, so read these as orders of magnitude.

| What                                           | Result                                                                                                                                         |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Memory, idle (4 services + runner)             | 297 MiB of 512 (about 123 MB per service)                                                                                                      |
| Memory, peak under a 20-order burst            | 297 MiB; no OOM kill, no restarts                                                                                                              |
| Cold start to all four services connected      | 4 CPUs 9.6 s (n=1) · 2 CPUs 20.4 s (n=1) · 0.5 CPU 16-24 s (n=4) · **0.1 CPU 69-72 s (n=2)**; first-ever runs on cold disk caches took 60-90 s |
| Per-query round trip to Neon (open connection) | about 80 ms                                                                                                                                    |
| Opening a new connection to Neon (TCP + TLS)   | 0.1 s to 4.9 s, highly variable                                                                                                                |
| POST to terminal status, 0.1 CPU, sequential   | p50 22 s, p95 37 s                                                                                                                             |
| POST to terminal status, 0.1 CPU, burst of 20  | p50 158 s, p95 182 s                                                                                                                           |
| POST `/orders` latency, 0.1 CPU                | sequential p50 0.8 s; burst p50 4.8 s                                                                                                          |

What this says:

- **Memory is not the problem.** Four Node processes fit with room to spare.
- **CPU and network distance are.** At 0.1 CPU the container is starved, and every saga hop makes several round trips to a database about 80 ms away. A burst of 20 orders took minutes to settle. The saga is correct under this load (no lost or duplicated orders), just slow.
- **Disabling the OpenTelemetry SDK (`OTEL_SDK_DISABLED=true`) showed no reliable gain** at 0.1 CPU, so it is not set.
- **The pool idle timeout mattered.** Orders 15 s apart at 0.5 CPU, four per arm, two rounds with the order swapped: median POST-to-terminal 76 s and 32 s with a 10 s idle timeout, 30 s and 17 s with 60 s. The timeout is now 60 s. Small n, so the factor is approximate; the direction held in both orderings.
- **Caveats.** `docker --cpus 0.1` is a hard quota; Render's free tier is a burstable share, so real behaviour may differ. Running the services in the same region as Neon should remove most of the network cost. The k6 script exists but was not used for these numbers.

# 8. What I would do differently at 100x scale

-
-
-

# 9. Interview answers I have rehearsed

Q: Your service writes to Postgres and publishes to Kafka. The process crashes between the two. What happens, and how does the outbox pattern fix it?
A:

Q: Payment succeeded but Inventory has no stock. Describe the compensating flow in order.
A:

Q: What did you lose by moving from the monolith to microservices? Be honest.
A:

Q: The Payment service charges the card, then crashes before committing. What happens on redelivery?
A:

# 10. Honest limitations

See [README.md](README.md#honest-limitations). Add anything new you discover.

# 11. How to run it

See [README.md](README.md#getting-started).

# 12. Credits

- Project brief: _The Resume Project Vault 2026_, card B3 (Event-Driven Order Processing with Microservices), @pratham.codes
- Kafka client: Confluent's `@confluentinc/kafka-javascript`
- Add every tutorial, repository or article you learn from here.
