import {
  createEvent,
  createLogger,
  createPool,
  migrate,
  processOnce,
  type Handler,
  type Pool,
} from "@ordermesh/shared";
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from "vitest";
import { MIGRATIONS_DIR } from "../src/defaults.ts";
import { inventoryHandlers } from "../src/handlers.ts";
import { seedStock } from "../src/seed.ts";

// Runs the Inventory handlers against a real Postgres.
// Roadmap week 2 "done when": two concurrent orders for the last saffron
// (stock 1) produce exactly one reservation and one rejection.

let pool: Pool;
const logger = createLogger("inventory-test", "silent");
const handlers = inventoryHandlers(logger);

beforeAll(async () => {
  pool = createPool(inject("databaseUrl"));
  await migrate(pool, MIGRATIONS_DIR, logger);
});

afterAll(async () => {
  await pool.end();
});

beforeEach(async () => {
  await pool.query("TRUNCATE stock, reservations, outbox, processed_events CASCADE");
  await seedStock(pool);
  await pool.query("UPDATE stock SET available = 1, reserved = 0 WHERE sku = 'SKU-SAFFRON-1G'");
});

async function stock(sku: string) {
  const { rows } = await pool.query<{ available: number; reserved: number }>(
    "SELECT available, reserved FROM stock WHERE sku = $1",
    [sku],
  );
  return rows[0]!;
}

async function outboxTypes() {
  const { rows } = await pool.query<{ event_type: string }>("SELECT event_type FROM outbox ORDER BY created_at");
  return rows.map((r) => r.event_type);
}

function orderCreated(orderId: string, items: { sku: string; quantity: number }[]) {
  return createEvent("OrderCreated", orderId, {
    orderId,
    customerId: "c1",
    items: items.map((i) => ({ ...i, unitPricePaise: 1 })),
    totalPaise: 1,
  });
}

async function deliver(orderId: string, items: { sku: string; quantity: number }[]) {
  const event = orderCreated(orderId, items);
  return processOnce(pool, "inventory", event, handlers.OrderCreated as Handler);
}

describe("stock reservation (Postgres)", () => {
  it("lets exactly one of two concurrent orders reserve the last unit", async () => {
    const a = crypto.randomUUID();
    const b = crypto.randomUUID();
    const item = [{ sku: "SKU-SAFFRON-1G", quantity: 1 }];

    await Promise.all([deliver(a, item), deliver(b, item)]);

    const types = await outboxTypes();
    expect(types.filter((t) => t === "InventoryReserved")).toHaveLength(1);
    expect(types.filter((t) => t === "InventoryRejected")).toHaveLength(1);
    expect(await stock("SKU-SAFFRON-1G")).toEqual({ available: 0, reserved: 1 });
  });

  it("releases a reservation, restores stock, and is safe to repeat", async () => {
    const id = crypto.randomUUID();
    await deliver(id, [{ sku: "SKU-RICE-5KG", quantity: 3 }]);
    expect(await stock("SKU-RICE-5KG")).toEqual({ available: 17, reserved: 3 });

    const release = createEvent("ReleaseInventoryRequested", id, { orderId: id, reason: "payment_failed" });
    await processOnce(pool, "inventory", release, handlers.ReleaseInventoryRequested as Handler);
    expect(await stock("SKU-RICE-5KG")).toEqual({ available: 20, reserved: 0 });

    // A second release (a redelivered compensation) must not add stock back twice.
    const again = createEvent("ReleaseInventoryRequested", id, { orderId: id, reason: "payment_failed" });
    await processOnce(pool, "inventory", again, handlers.ReleaseInventoryRequested as Handler);
    expect(await stock("SKU-RICE-5KG")).toEqual({ available: 20, reserved: 0 });
  });

  it("counts a SKU listed twice against the total it needs", async () => {
    const id = crypto.randomUUID();
    // Two lines for the last saffron: 1 + 1 = 2 needed, only 1 available.
    await deliver(id, [
      { sku: "SKU-SAFFRON-1G", quantity: 1 },
      { sku: "SKU-SAFFRON-1G", quantity: 1 },
    ]);
    expect(await outboxTypes()).toEqual(["InventoryRejected"]);
    expect(await stock("SKU-SAFFRON-1G")).toEqual({ available: 1, reserved: 0 });
  });
});
