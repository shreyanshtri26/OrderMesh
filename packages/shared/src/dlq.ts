import { randomUUID } from "node:crypto";
import { TOPICS } from "./events.ts";
import { headersToStrings, type Kafka, type Producer } from "./kafka.ts";

// Inspect and replay the dead-letter queue.
//
// A message lands in `dlq` with headers that say where it came from and why it
// failed (see dlqMessage() in consumer.ts). Replaying publishes the original
// value back to its original topic. This is safe because every consumer is
// idempotent: an event that already succeeded is skipped as a duplicate, and one
// that failed (and so never wrote its processed_events marker) is handled afresh.
//
// The DLQ itself is an append-only log, so a replayed entry stays listed.

export type DlqEntry = {
  partition: number;
  offset: number;
  key: string | null;
  value: string | null;
  originalTopic: string | null;
  originalPartition: number | null;
  originalOffset: number | null;
  consumer: string | null;
  attempts: number;
  error: string | null;
  failedAt: string | null;
};

type RawMessage = {
  offset: string;
  key: Buffer | string | null;
  value: Buffer | string | null;
  headers?: Parameters<typeof headersToStrings>[0];
};

const toNumber = (value: string | undefined): number | null => (value === undefined ? null : Number(value));

/** Turns a raw DLQ message into a plain, JSON-safe entry. */
export function toDlqEntry(partition: number, message: RawMessage): DlqEntry {
  const h = headersToStrings(message.headers);
  return {
    partition,
    offset: Number(message.offset),
    key: message.key?.toString() ?? null,
    value: message.value?.toString() ?? null,
    originalTopic: h["dlq-original-topic"] ?? null,
    originalPartition: toNumber(h["dlq-original-partition"]),
    originalOffset: toNumber(h["dlq-original-offset"]),
    consumer: h["dlq-consumer"] ?? null,
    attempts: Number(h["dlq-attempts"] ?? 0),
    error: h["dlq-error"] ?? null,
    failedAt: h["dlq-failed-at"] ?? null,
  };
}

/** Reads the DLQ from the beginning without joining a real consumer group. Newest first. */
export async function readDlq(kafka: Kafka, opts: { limit?: number; timeoutMs?: number } = {}): Promise<DlqEntry[]> {
  const limit = opts.limit ?? 100;
  const timeoutMs = opts.timeoutMs ?? 5_000;

  // How many messages are there? Reading until the end offsets avoids guessing with idle timers.
  const admin = kafka.admin();
  await admin.connect();
  let expected = 0;
  try {
    const offsets = await admin.fetchTopicOffsets(TOPICS.dlq);
    expected = offsets.reduce((sum, o) => sum + (Number(o.high) - Number(o.low)), 0);
  } finally {
    await admin.disconnect().catch(() => {});
  }
  if (expected === 0) return [];

  const want = Math.min(expected, limit);
  const entries: DlqEntry[] = [];
  const consumer = kafka.consumer({
    kafkaJS: { groupId: `dlq-inspect-${randomUUID()}`, fromBeginning: true, autoCommit: false },
  });
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [TOPICS.dlq] });
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, timeoutMs);
      consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            entries.push(toDlqEntry(partition, message as RawMessage));
            if (entries.length >= want) {
              clearTimeout(timer);
              resolve();
            }
          },
        })
        .catch(() => resolve());
    });
  } finally {
    await consumer.disconnect().catch(() => {});
  }

  return entries.sort((a, b) => (b.failedAt ?? "").localeCompare(a.failedAt ?? ""));
}

/** Republishes one DLQ entry to its original topic. Returns the entry, or null if it cannot be replayed. */
export async function replayFromDlq(
  kafka: Kafka,
  producer: Producer,
  target: { partition: number; offset: number },
): Promise<DlqEntry | null> {
  const entries = await readDlq(kafka, { limit: 1_000 });
  const entry = entries.find((e) => e.partition === target.partition && e.offset === target.offset);
  if (!entry?.originalTopic || entry.value === null) return null;

  await producer.send({
    topic: entry.originalTopic,
    messages: [
      {
        key: entry.key,
        value: entry.value,
        headers: { "dlq-replayed-from": `${entry.partition}:${entry.offset}` },
      },
    ],
  });
  return entry;
}
