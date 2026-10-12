import { describe, expect, it } from "vitest";
import { createEvent, EVENT_TOPIC, InvalidEventError, parseEnvelope, PAYLOADS, TOPICS } from "../src/events.ts";

describe("event envelope", () => {
  it("round-trips through JSON", () => {
    const event = createEvent("PaymentFailed", "order-1", { orderId: "order-1", reason: "card_declined" });
    expect(parseEnvelope(JSON.stringify(event))).toEqual(event);
  });

  it("rejects payloads that do not match the event type", () => {
    expect(() =>
      // @ts-expect-error deliberately wrong payload
      createEvent("PaymentCompleted", "order-1", { orderId: "order-1" }),
    ).toThrow();
  });

  it.each([
    ["null", null],
    ["not JSON", "{oops"],
    ["missing id", JSON.stringify({ type: "OrderConfirmed" })],
    [
      "unknown type",
      JSON.stringify({
        id: crypto.randomUUID(),
        type: "Nope",
        aggregateId: "o",
        occurredAt: new Date().toISOString(),
        version: 1,
        payload: {},
      }),
    ],
    [
      "bad payload",
      JSON.stringify({
        id: crypto.randomUUID(),
        type: "OrderConfirmed",
        aggregateId: "o",
        occurredAt: new Date().toISOString(),
        version: 1,
        payload: { orderId: 42 },
      }),
    ],
  ])("throws InvalidEventError for %s", (_label, raw) => {
    expect(() => parseEnvelope(raw)).toThrow(InvalidEventError);
  });

  it("maps every event type to a topic, using at most 5 topics (Aiven free tier)", () => {
    for (const type of Object.keys(PAYLOADS)) expect(EVENT_TOPIC[type as keyof typeof EVENT_TOPIC]).toBeDefined();
    expect(Object.keys(TOPICS).length).toBeLessThanOrEqual(5);
  });
});
