import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pino } from "pino";
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it, vi } from "vitest";
import { processOnce } from "../src/consumer.ts";
import { createPool, migrate, withTransaction, type Pool } from "../src/db.ts";
import { createEvent } from "../src/events.ts";
import type { Producer } from "../src/kafka.ts";
import { appendToOutbox, publishOutboxBatch } from "../src/outbox.ts";

// These tests run against a real Postgres, because the guarantees being tested
// (atomic commits, row locks, unique constraints) live in the database.

let pool: Pool;

beforeAll(async () => {
  pool = createPool(inject("databaseUrl"));
  const dir = await mkdtemp(join(tmpdir(), "ordermesh-migrations-"));
  await writeFile(join(dir, "001_things.sql"), "CREATE TABLE things (id int PRIMARY KEY, note text NOT NULL);");
  await migrate(pool, dir, pino({ level: "silent" }));
});

afterAll(async () => {
  await pool.end();
});

beforeEach(async () => {
  await pool.query("TRUNCATE outbox, processed_events, things");
});

function fakeProducer(send = vi.fn().mockResolvedValue([])) {
  return { producer: { send } as unknown as Producer, send };
}

const event = (n: number) => createEvent("OrderConfirmed", `order-${n}`, { orderId: `order-${n}` });

describe("transactional outbox", () => {
  it("commits the business row and its event together", async () => {
    await withTransaction(pool, async (tx) => {
      await tx.query("INSERT INTO things (id, note) VALUES (1, 'ok')");
      await appendToOutbox(tx, event(1));
    });
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL");
    expect(rows[0].n).toBe(1);
  });

  it("rolls back both when the transaction fails", async () => {
    await expect(
      withTransaction(pool, async (tx) => {
        await tx.query("INSERT INTO things (id, note) VALUES (2, 'will vanish')");
        await appendToOutbox(tx, event(2));
        throw new Error("crash after both writes, before commit");
      }),
    ).rejects.toThrow();
    const things = await pool.query("SELECT count(*)::int AS n FROM things");
    const outbox = await pool.query("SELECT count(*)::int AS n FROM outbox");
    expect([things.rows[0].n, outbox.rows[0].n]).toEqual([0, 0]);
  });

  it("publishes pending rows in order and marks them published", async () => {
    for (const n of [1, 2, 3]) {
      await withTransaction(pool, (tx) => appendToOutbox(tx, event(n)));
    }
    const { producer, send } = fakeProducer();

    expect(await publishOutboxBatch(pool, producer)).toBe(3);
    expect(send.mock.calls.map(([record]) => record.messages[0].key)).toEqual(["order-1", "order-2", "order-3"]);
    expect(await publishOutboxBatch(pool, producer)).toBe(0); // nothing left
  });

  it("keeps rows unpublished when Kafka is down, so they are retried", async () => {
    await withTransaction(pool, (tx) => appendToOutbox(tx, event(1)));
    const { producer } = fakeProducer(vi.fn().mockRejectedValue(new Error("broker unavailable")));

    await expect(publishOutboxBatch(pool, producer)).rejects.toThrow("broker unavailable");
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL");
    expect(rows[0].n).toBe(1);
  });

  it("lets two relays run at once without publishing the same row twice", async () => {
    await withTransaction(pool, (tx) => appendToOutbox(tx, event(1)));

    // Relay A claims the row and holds the lock...
    const relayA = await pool.connect();
    await relayA.query("BEGIN");
    await relayA.query("SELECT id FROM outbox WHERE published_at IS NULL FOR UPDATE SKIP LOCKED");

    // ...so relay B skips it instead of waiting or double-publishing.
    const { producer, send } = fakeProducer();
    expect(await publishOutboxBatch(pool, producer)).toBe(0);
    expect(send).not.toHaveBeenCalled();

    await relayA.query("ROLLBACK");
    relayA.release();
  });
});

describe("idempotent consumer", () => {
  it("runs the handler once for a redelivered event", async () => {
    const handler = vi.fn(async (_e, tx) => {
      await tx.query("INSERT INTO things (id, note) VALUES (10, 'handled')");
    });
    const e = event(10);

    expect(await processOnce(pool, "notification", e, handler)).toBe("processed");
    expect(await processOnce(pool, "notification", e, handler)).toBe("duplicate");
    expect(await processOnce(pool, "notification", e, handler)).toBe("duplicate");
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("treats the same event as new for a different consumer", async () => {
    const e = event(11);
    expect(await processOnce(pool, "notification", e, async () => {})).toBe("processed");
    expect(await processOnce(pool, "order", e, async () => {})).toBe("processed");
  });

  it("does not mark an event processed when its handler fails, so a retry runs it", async () => {
    const e = event(12);
    await expect(
      processOnce(pool, "payment", e, async () => {
        throw new Error("gateway timeout");
      }),
    ).rejects.toThrow("gateway timeout");

    const retry = vi.fn(async () => {});
    expect(await processOnce(pool, "payment", e, retry)).toBe("processed");
    expect(retry).toHaveBeenCalledTimes(1);
  });
});
