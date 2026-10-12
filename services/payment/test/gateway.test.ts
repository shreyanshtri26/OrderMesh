import { describe, expect, it } from "vitest";
import { createMockGateway } from "../src/gateway.ts";

describe("mock payment gateway", () => {
  it("declines when the random draw is under the failure rate", async () => {
    const gateway = createMockGateway({ failureRate: 0.5, latencyMs: 0, random: () => 0.1 });
    expect(await gateway.charge(1000)).toEqual({ ok: false, reason: "card_declined" });
  });

  it("succeeds otherwise and returns a payment id", async () => {
    const gateway = createMockGateway({ failureRate: 0.5, latencyMs: 0, random: () => 0.9 });
    const result = await gateway.charge(1000);
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.paymentId).toMatch(/^pay_/);
  });

  it("rejects a zero amount", async () => {
    const gateway = createMockGateway({ failureRate: 0, latencyMs: 0 });
    expect(await gateway.charge(0)).toEqual({ ok: false, reason: "invalid_amount" });
  });
});
