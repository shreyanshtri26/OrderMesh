import { describe, expect, it } from "vitest";
import { dlqMessage } from "../src/consumer.ts";
import { toDlqEntry } from "../src/dlq.ts";

describe("DLQ entries", () => {
  it("round-trips what dlqMessage writes into a typed entry", () => {
    const written = dlqMessage(
      {
        topic: "orders",
        partition: 2,
        message: {
          key: Buffer.from("order-1"),
          value: Buffer.from("not json"),
          offset: "41",
          timestamp: "0",
          size: 8,
          attributes: 0,
        } as never,
      },
      "inventory",
      new Error("Message is not valid JSON"),
      0,
    );

    const entry = toDlqEntry(0, { offset: "7", key: written.key, value: written.value, headers: written.headers });

    expect(entry).toMatchObject({
      partition: 0,
      offset: 7,
      key: "order-1",
      value: "not json",
      originalTopic: "orders",
      originalPartition: 2,
      originalOffset: 41,
      consumer: "inventory",
      attempts: 0,
      error: "Message is not valid JSON",
    });
    expect(entry.failedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("tolerates a message with no DLQ headers", () => {
    expect(toDlqEntry(1, { offset: "3", key: null, value: null })).toMatchObject({
      key: null,
      value: null,
      originalTopic: null,
      attempts: 0,
    });
  });
});
