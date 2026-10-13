import { describe, expect, it } from "vitest";
import { createMockGateway } from "../src/gateway.ts";

describe("mock payment gateway", () => {
  it("declines when the random draw is under the failure rate", async () => {
    const gateway = createMockGateway({ failureRate: 0.5, latencyMs: 0, random: () => 0.1 });
    expect(await gateway.charge(1000, "order-1")).toEqual({ ok: false, reason: "card_declined" });
  });

  it("succeeds otherwise and returns a payment id", async () => {
    const gateway = createMockGateway({ failureRate: 0.5, latencyMs: 0, random: () => 0.9 });
    const result = await gateway.charge(1000, "order-1");
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.paymentId).toMatch(/^pay_/);
  });

  it("rejects a zero amount", async () => {
    const gateway = createMockGateway({ failureRate: 0, latencyMs: 0 });
    expect(await gateway.charge(0, "order-1")).toEqual({ ok: false, reason: "invalid_amount" });
  });
});

describe("idempotency key", () => {
  it("returns the first result for a repeated key and does not charge again", async () => {
    let calls = 0;
    const gateway = createMockGateway({ failureRate: 0, latencyMs: 0, random: () => (calls++, 0.9) });
    const first = await gateway.charge(1000, "order-42");
    const retry = await gateway.charge(1000, "order-42");
    expect(retry).toEqual(first);
    expect(calls).toBe(1);
  });
});
