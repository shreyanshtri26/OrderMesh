import { randomUUID } from "node:crypto";

// A fake payment gateway, so failures can be produced on demand.
//
//   PAYMENT_FAILURE_RATE=0.3    30% of charges are declined
//   PAYMENT_LATENCY_MS=200      every charge takes this long
//
// A declined charge is a business outcome (PaymentFailed), not an error.
// An exception from the gateway (timeout, outage) IS an error: the consumer
// retries it and, after 3 attempts, sends it to the dead-letter queue.
//
// Every charge carries an idempotency key (the orderId). A real gateway returns
// the first result for a repeated key instead of charging again. That is what
// makes a redelivered PaymentRequested safe after a crash between the charge
// and the COMMIT. This mock keeps the results in memory, so it only shows the
// behaviour inside one process; a real gateway keeps them on its side.

export type ChargeResult = { ok: true; paymentId: string } | { ok: false; reason: string };

export function createMockGateway(opts: { failureRate: number; latencyMs: number; random?: () => number }) {
  const random = opts.random ?? Math.random;
  const results = new Map<string, ChargeResult>();
  return {
    async charge(amountPaise: number, idempotencyKey: string): Promise<ChargeResult> {
      const previous = results.get(idempotencyKey);
      if (previous) return previous;

      await new Promise((resolve) => setTimeout(resolve, opts.latencyMs));
      const result = decide(amountPaise);
      results.set(idempotencyKey, result);
      return result;
    },
  };

  function decide(amountPaise: number): ChargeResult {
    if (amountPaise <= 0) return { ok: false, reason: "invalid_amount" };
    if (random() < opts.failureRate) return { ok: false, reason: "card_declined" };
    return { ok: true, paymentId: `pay_${randomUUID()}` };
  }
}

export type Gateway = ReturnType<typeof createMockGateway>;
