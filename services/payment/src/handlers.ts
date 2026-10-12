import type { Handlers, Logger } from "@ordermesh/shared";
import type { Gateway } from "./gateway.ts";

// ─────────────────────────────────────────────────────────────────────────────
// TODO (Roadmap week 3): implement charging.
//
// PaymentRequested
//   1. Call gateway.charge(amountPaise).
//   2. Insert a payments row (COMPLETED or FAILED) in the handler's transaction.
//   3. Emit PaymentCompleted { orderId, paymentId } or PaymentFailed { orderId, reason }
//      with appendToOutbox(tx, createEvent(...)).
//
// Think about: the gateway call is NOT inside the database transaction. What
// happens if the process crashes after the charge succeeds but before COMMIT?
// (Hint: real gateways accept an idempotency key; use the orderId.)
// Write your answer in SKILL.md section 9.
// ─────────────────────────────────────────────────────────────────────────────

export function paymentHandlers(logger: Logger, _gateway: Gateway): Handlers {
  return {
    PaymentRequested: async (event) => {
      logger.warn({ orderId: event.payload.orderId }, "TODO: charge payment (roadmap week 3)");
    },
  };
}
