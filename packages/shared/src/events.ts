import { randomUUID } from "node:crypto";
import { z } from "zod";

// The event contract between services.
//
// Every message on Kafka is an Envelope: a stable id (used for idempotency),
// a type, the aggregate it belongs to (used as the Kafka key, so all events
// for one order land on the same partition and stay in order) and a payload.
//
// Free-tier constraint: Aiven's free Kafka allows 5 topics. Instead of one
// topic per event type, there is one topic per domain and the event type
// travels in the envelope. 4 topics are used; 1 is spare.

export const TOPICS = {
  orders: "orders",
  inventory: "inventory",
  payments: "payments",
  dlq: "dlq",
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];

const money = z.number().int().nonnegative(); // amounts are integers in paise

export const PAYLOADS = {
  // Published by Order
  OrderCreated: z.object({
    orderId: z.string(),
    customerId: z.string(),
    items: z.array(z.object({ sku: z.string(), quantity: z.number().int().positive(), unitPricePaise: money })),
    totalPaise: money,
  }),
  PaymentRequested: z.object({ orderId: z.string(), amountPaise: money }),
  ReleaseInventoryRequested: z.object({ orderId: z.string(), reason: z.string() }),
  OrderConfirmed: z.object({ orderId: z.string() }),
  OrderCancelled: z.object({ orderId: z.string(), reason: z.string() }),

  // Published by Inventory
  InventoryReserved: z.object({ orderId: z.string() }),
  InventoryRejected: z.object({ orderId: z.string(), reason: z.string() }),
  InventoryReleased: z.object({ orderId: z.string() }),

  // Published by Payment
  PaymentCompleted: z.object({ orderId: z.string(), paymentId: z.string() }),
  PaymentFailed: z.object({ orderId: z.string(), reason: z.string() }),
} as const;

export type EventType = keyof typeof PAYLOADS;
export type PayloadOf<T extends EventType> = z.infer<(typeof PAYLOADS)[T]>;

export const EVENT_TOPIC: Record<EventType, Topic> = {
  OrderCreated: "orders",
  PaymentRequested: "orders",
  ReleaseInventoryRequested: "orders",
  OrderConfirmed: "orders",
  OrderCancelled: "orders",
  InventoryReserved: "inventory",
  InventoryRejected: "inventory",
  InventoryReleased: "inventory",
  PaymentCompleted: "payments",
  PaymentFailed: "payments",
};

export type Envelope<T extends EventType = EventType> = {
  id: string;
  type: T;
  aggregateId: string;
  occurredAt: string;
  version: 1;
  payload: PayloadOf<T>;
};

const envelopeShape = z.object({
  id: z.string().uuid(),
  type: z.enum(Object.keys(PAYLOADS) as [EventType, ...EventType[]]),
  aggregateId: z.string().min(1),
  occurredAt: z.string().datetime(),
  version: z.literal(1),
  payload: z.unknown(),
});

export function createEvent<T extends EventType>(type: T, aggregateId: string, payload: PayloadOf<T>): Envelope<T> {
  return {
    id: randomUUID(),
    type,
    aggregateId,
    occurredAt: new Date().toISOString(),
    version: 1,
    payload: PAYLOADS[type].parse(payload) as PayloadOf<T>,
  };
}

export class InvalidEventError extends Error {}

/** Parses and validates a raw Kafka message value. Throws InvalidEventError. */
export function parseEnvelope(raw: string | Buffer | null): Envelope {
  if (raw === null) throw new InvalidEventError("Empty message");
  let json: unknown;
  try {
    json = JSON.parse(raw.toString());
  } catch {
    throw new InvalidEventError("Message is not valid JSON");
  }
  const shape = envelopeShape.safeParse(json);
  if (!shape.success) throw new InvalidEventError(`Bad envelope: ${shape.error.issues[0]?.message}`);
  const payload = PAYLOADS[shape.data.type].safeParse(shape.data.payload);
  if (!payload.success) {
    throw new InvalidEventError(`Bad ${shape.data.type} payload: ${payload.error.issues[0]?.message}`);
  }
  return { ...shape.data, payload: payload.data } as Envelope;
}
