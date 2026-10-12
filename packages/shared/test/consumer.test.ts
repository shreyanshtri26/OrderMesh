import { describe, expect, it } from "vitest";
import { backoffMs, dlqMessage } from "../src/consumer.ts";

describe("retry backoff", () => {
  it("doubles each attempt and caps at the maximum", () => {
    expect([1, 2, 3, 4].map((a) => backoffMs(a, 100, 500))).toEqual([100, 200, 400, 500]);
  });
});

describe("dead-letter message", () => {
  it("keeps the original message and records why it failed", () => {
    const dlq = dlqMessage(
      {
        topic: "payments",
        partition: 2,
        message: {
          key: Buffer.from("order-1"),
          value: Buffer.from("{}"),
          headers: { traceparent: "00-abc-def-01" },
          offset: "42",
          timestamp: "0",
          attributes: 0,
          deserializedKey: { key: null, error: null },
          deserializedValue: { value: null, error: null },
        },
      },
      "order",
      new Error("boom"),
      3,
    );
    expect(dlq.key?.toString()).toBe("order-1");
    expect(dlq.headers).toMatchObject({
      traceparent: "00-abc-def-01",
      "dlq-original-topic": "payments",
      "dlq-original-partition": "2",
      "dlq-original-offset": "42",
      "dlq-consumer": "order",
      "dlq-attempts": "3",
      "dlq-error": "boom",
    });
  });
});
