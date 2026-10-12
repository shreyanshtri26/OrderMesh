import type { Pool, Tx } from "./db.ts";
import { EVENT_TOPIC, type Envelope } from "./events.ts";
import type { Producer } from "./kafka.ts";
import type { Logger } from "./logger.ts";
import { contextFromHeaders, currentTraceHeaders, withSpan } from "./tracing.ts";

// Transactional outbox.
//
// The dual-write problem: a service that writes to Postgres and then publishes
// to Kafka can crash between the two, leaving the database and the event
// stream disagreeing. The fix is to write the event into an `outbox` table in
// the SAME transaction as the business change. Either both commit or neither
// does. A relay then publishes outbox rows to Kafka and marks them published.
//
// Delivery is at-least-once: if the relay crashes after publishing but before
// marking the row, the event is published again. Consumers are idempotent
// (see consumer.ts), so a duplicate has no effect.

/** Writes an event to the outbox. Must be called inside the business transaction. */
export async function appendToOutbox(tx: Tx, event: Envelope): Promise<void> {
  await tx.query(
    `INSERT INTO outbox (id, topic, message_key, event_type, envelope, headers)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      event.id,
      EVENT_TOPIC[event.type],
      event.aggregateId,
      event.type,
      JSON.stringify(event),
      JSON.stringify(currentTraceHeaders()),
    ],
  );
}

type OutboxRow = {
  id: string;
  topic: string;
  message_key: string;
  event_type: string;
  envelope: Envelope;
  headers: Record<string, string>;
};

/**
 * Publishes one batch of unpublished rows. Returns how many were published.
 *
 * `FOR UPDATE SKIP LOCKED` lets several relay instances run safely: each one
 * claims different rows instead of publishing the same row twice.
 */
export async function publishOutboxBatch(pool: Pool, producer: Producer, batchSize = 50): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query<OutboxRow>(
      `SELECT id, topic, message_key, event_type, envelope, headers
         FROM outbox
        WHERE published_at IS NULL
        ORDER BY created_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [batchSize],
    );
    if (rows.length === 0) {
      await client.query("COMMIT");
      return 0;
    }

    // Rows are sent in created_at order, one at a time, so events for the
    // same order keep their order on the partition.
    for (const row of rows) {
      await withSpan(
        `publish ${row.event_type}`,
        "producer",
        contextFromHeaders(row.headers),
        { "messaging.system": "kafka", "messaging.destination.name": row.topic, "event.id": row.id },
        () =>
          producer.send({
            topic: row.topic,
            messages: [
              {
                key: row.message_key,
                value: JSON.stringify(row.envelope),
                headers: { ...row.headers, "event-type": row.event_type, "event-id": row.id },
              },
            ],
          }),
      );
    }

    await client.query("UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])", [rows.map((r) => r.id)]);
    await client.query("COMMIT");
    return rows.length;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/** Polls the outbox until `stop()` is called. Drains quickly when there is a backlog. */
export function startOutboxRelay(opts: { pool: Pool; producer: Producer; logger: Logger; intervalMs: number }): {
  stop: () => Promise<void>;
} {
  let running = true;
  let loop: Promise<void> = Promise.resolve();

  const tick = async () => {
    while (running) {
      try {
        const published = await publishOutboxBatch(opts.pool, opts.producer);
        if (published > 0) {
          opts.logger.debug({ published }, "outbox batch published");
          continue; // backlog: go again immediately
        }
      } catch (error) {
        opts.logger.error({ err: error }, "outbox relay failed; will retry");
      }
      await new Promise((resolve) => setTimeout(resolve, opts.intervalMs));
    }
  };
  loop = tick();

  return {
    stop: async () => {
      running = false;
      await loop;
    },
  };
}
