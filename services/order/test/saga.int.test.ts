import {
  createEvent,
  createPool,
  migrate,
  processOnce,
  type Envelope,
  type Handler,
  type Pool,
} from "@ordermesh/shared";
import { pino } from "pino";
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from "vitest";
import { MIGRATIONS_DIR } from "../src/defaults.ts";
import { sagaHandlers } from "../src/handlers.ts";

// Runs the saga orchestrator against a real Postgres and checks, after each
// event, the order's status and the next command waiting in the outbox.

let pool: Pool;
const handlers = sagaHandlers(pino({ level: "silent" }));

beforeAll(async () => {
  pool = createPool(inject("databaseUrl"));
  await migrate(pool, MIGRATIONS_DIR, pino({ level: "silent" }));
});

afterAll(async () => {
  await pool.end();
});

beforeEach(async () => {
  await pool.query("TRUNCATE orders, order_status_history, outbox, processed_events CASCADE");
});

async function newOrder(): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO orders (customer_id, status, items, total_paise) VALUES ('c1', 'PENDING', '[]', 24900) RETURNING id`,
  );
  return rows[0]!.id;
}

async function deliver(event: Envelope) {
  return processOnce(pool, "order", event, handlers[event.type] as Handler);
}

async function state(orderId: string) {
  const order = await pool.query("SELECT status, cancel_reason FROM orders WHERE id = $1", [orderId]);
  const outbox = await pool.query<{ event_type: string; envelope: Envelope }>(
    "SELECT event_type, envelope FROM outbox ORDER BY created_at DESC LIMIT 1",
  );
  return { ...order.rows[0], lastCommand: outbox.rows[0]?.event_type, lastPayload: outbox.rows[0]?.envelope.payload };
}

describe("saga orchestrator (Postgres)", () => {
  it("drives the happy path to CONFIRMED", async () => {
    const id = await newOrder();

    await deliver(createEvent("InventoryReserved", id, { orderId: id }));
    expect(await state(id)).toMatchObject({ status: "INVENTORY_RESERVED", lastCommand: "PaymentRequested" });

    await deliver(createEvent("PaymentCompleted", id, { orderId: id, paymentId: "pay_1" }));
    expect(await state(id)).toMatchObject({ status: "CONFIRMED", lastCommand: "OrderConfirmed" });
  });

  it("compensates a failed payment and keeps the original reason", async () => {
    const id = await newOrder();
    await deliver(createEvent("InventoryReserved", id, { orderId: id }));

    await deliver(createEvent("PaymentFailed", id, { orderId: id, reason: "card_declined" }));
    expect(await state(id)).toMatchObject({
      status: "CANCELLING",
      lastCommand: "ReleaseInventoryRequested",
      lastPayload: { orderId: id, reason: "card_declined" },
    });

    await deliver(createEvent("InventoryReleased", id, { orderId: id }));
    expect(await state(id)).toMatchObject({
      status: "CANCELLED",
      cancel_reason: "card_declined",
      lastCommand: "OrderCancelled",
      lastPayload: { orderId: id, reason: "card_declined" },
    });
  });

  it("applies a redelivered event only once", async () => {
    const id = await newOrder();
    const reserved = createEvent("InventoryReserved", id, { orderId: id });

    expect(await deliver(reserved)).toBe("processed");
    expect(await deliver(reserved)).toBe("duplicate");

    const commands = await pool.query("SELECT count(*)::int AS n FROM outbox WHERE event_type = 'PaymentRequested'");
    expect(commands.rows[0].n).toBe(1);
  });

  it("ignores an event that arrives in the wrong state", async () => {
    const id = await newOrder();
    await deliver(createEvent("PaymentCompleted", id, { orderId: id, paymentId: "pay_late" }));
    expect(await state(id)).toMatchObject({ status: "PENDING", lastCommand: undefined });
  });
});
