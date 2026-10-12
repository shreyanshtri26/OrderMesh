import { withTransaction, type Pool, type Tx } from "./db.ts";
import { InvalidEventError, parseEnvelope, TOPICS, type Envelope, type EventType } from "./events.ts";
import { headersToStrings, type Consumer, type EachMessagePayload, type Kafka, type Producer } from "./kafka.ts";
import type { Logger } from "./logger.ts";
import { contextFromHeaders, withSpan } from "./tracing.ts";

// Idempotent consumer with retries and a dead-letter queue.
//
// For each message:
//   1. Parse and validate the envelope. A malformed message can never succeed,
//      so it goes straight to the DLQ.
//   2. In ONE transaction: insert (event_id, consumer) into processed_events,
//      then run the handler. If the insert hits the primary key, this event was
//      already handled: skip it. Because the marker and the handler's writes
//      commit together, a crash can never record one without the other.
//   3. On failure, roll back and retry with exponential backoff. After
//      `maxAttempts`, publish the message to the DLQ with the error attached,
//      so one bad message cannot block the partition forever.

export type Handler<T extends EventType = EventType> = (event: Envelope<T>, tx: Tx) => Promise<void>;
export type Handlers = { [K in EventType]?: Handler<K> };

export type ProcessResult = "processed" | "duplicate" | "ignored";

/** Runs the handler exactly once per (event id, consumer). */
export async function processOnce(
  pool: Pool,
  consumerName: string,
  event: Envelope,
  handler: Handler,
): Promise<ProcessResult> {
  return withTransaction(pool, async (tx) => {
    const marker = await tx.query(
      `INSERT INTO processed_events (event_id, consumer) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
      [event.id, consumerName],
    );
    if (marker.rowCount === 0) return "duplicate";
    await handler(event, tx);
    return "processed";
  });
}

export function backoffMs(attempt: number, baseMs = 200, maxMs = 5_000): number {
  return Math.min(maxMs, baseMs * 2 ** (attempt - 1));
}

export function dlqMessage(
  payload: Pick<EachMessagePayload, "topic" | "partition" | "message">,
  consumerName: string,
  error: unknown,
  attempts: number,
) {
  const { message } = payload;
  return {
    key: message.key,
    value: message.value,
    headers: {
      ...headersToStrings("headers" in message ? message.headers : undefined),
      "dlq-original-topic": payload.topic,
      "dlq-original-partition": String(payload.partition),
      "dlq-original-offset": message.offset,
      "dlq-consumer": consumerName,
      "dlq-attempts": String(attempts),
      "dlq-error": error instanceof Error ? error.message.slice(0, 500) : String(error),
      "dlq-failed-at": new Date().toISOString(),
    },
  };
}

export async function startConsumer(opts: {
  kafka: Kafka;
  pool: Pool;
  producer: Producer;
  logger: Logger;
  consumerName: string;
  topics: string[];
  handlers: Handlers;
  maxAttempts?: number;
}): Promise<Consumer> {
  const maxAttempts = opts.maxAttempts ?? 3;
  const consumer = opts.kafka.consumer({
    kafkaJS: { groupId: opts.consumerName, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topics: opts.topics });

  await consumer.run({
    eachMessage: async (payload) => {
      const { message } = payload;
      const headers = headersToStrings("headers" in message ? message.headers : undefined);
      const log = opts.logger.child({ topic: payload.topic, offset: message.offset });

      let event: Envelope;
      try {
        event = parseEnvelope(message.value);
      } catch (error) {
        log.error({ err: error }, "invalid message, sending to DLQ");
        await opts.producer.send({ topic: TOPICS.dlq, messages: [dlqMessage(payload, opts.consumerName, error, 0)] });
        return;
      }

      const handler = opts.handlers[event.type] as Handler | undefined;
      if (!handler) return; // Not this service's event; topics are shared.

      for (let attempt = 1; ; attempt++) {
        try {
          const result = await withSpan(
            `process ${event.type}`,
            "consumer",
            contextFromHeaders(headers),
            { "messaging.system": "kafka", "messaging.consumer.group.name": opts.consumerName, "event.id": event.id },
            () => processOnce(opts.pool, opts.consumerName, event, handler),
          );
          log.info({ eventId: event.id, type: event.type, orderId: event.aggregateId, result }, "event handled");
          return;
        } catch (error) {
          if (error instanceof InvalidEventError || attempt >= maxAttempts) {
            log.error({ err: error, eventId: event.id, attempt }, "giving up, sending to DLQ");
            await opts.producer.send({
              topic: TOPICS.dlq,
              messages: [dlqMessage(payload, opts.consumerName, error, attempt)],
            });
            return;
          }
          log.warn({ err: error, eventId: event.id, attempt }, "handler failed, retrying");
          await new Promise((resolve) => setTimeout(resolve, backoffMs(attempt)));
        }
      }
    },
  });

  return consumer;
}
