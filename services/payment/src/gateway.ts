import { randomUUID } from "node:crypto";

// A fake payment gateway, so failures can be produced on demand.
//
//   PAYMENT_FAILURE_RATE=0.3    30% of charges are declined
//   PAYMENT_LATENCY_MS=200      every charge takes this long
//
// A declined charge is a business outcome (PaymentFailed), not an error.
// An exception from the gateway (timeout, outage) IS an error: the consumer
// retries it and, after 3 attempts, sends it to the dead-letter queue.

export type ChargeResult = { ok: true; paymentId: string } | { ok: false; reason: string };

export function createMockGateway(opts: { failureRate: number; latencyMs: number; random?: () => number }) {
  const random = opts.random ?? Math.random;
  return {
    async charge(amountPaise: number): Promise<ChargeResult> {
      await new Promise((resolve) => setTimeout(resolve, opts.latencyMs));
      if (amountPaise <= 0) return { ok: false, reason: "invalid_amount" };
      if (random() < opts.failureRate) return { ok: false, reason: "card_declined" };
      return { ok: true, paymentId: `pay_${randomUUID()}` };
    },
  };
}

export type Gateway = ReturnType<typeof createMockGateway>;
