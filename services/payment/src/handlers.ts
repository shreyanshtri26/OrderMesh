import { appendToOutbox, createEvent, type Handlers, type Logger } from "@ordermesh/shared";
import type { Gateway } from "./gateway.ts";

// PaymentRequested: charge the order once and record the outcome.
//
// The gateway call runs inside the consumer's transaction. If the process
// crashes after the charge but before COMMIT, nothing is recorded and the
// event is redelivered. The retry calls the gateway with the same idempotency
// key (the orderId), so the gateway returns the first result and does not
// charge again. The UNIQUE(order_id) constraint is the second guard: even a
// buggy retry cannot write two payment rows for one order.
//
// A declined card is a business outcome (PaymentFailed), which starts the
// compensation in the Order saga. A gateway exception is not handled here: it
// propagates, the consumer retries, and after 3 attempts the message goes to the DLQ.

export function paymentHandlers(logger: Logger, gateway: Gateway): Handlers {
  return {
    PaymentRequested: async (event, tx) => {
      const { orderId, amountPaise } = event.payload;
      const result = await gateway.charge(amountPaise, orderId);

      if (result.ok) {
        await tx.query("INSERT INTO payments (order_id, amount_paise, status) VALUES ($1, $2, 'COMPLETED')", [
          orderId,
          amountPaise,
        ]);
        await appendToOutbox(tx, createEvent("PaymentCompleted", orderId, { orderId, paymentId: result.paymentId }));
        logger.info({ orderId, paymentId: result.paymentId }, "payment completed");
        return;
      }

      await tx.query(
        "INSERT INTO payments (order_id, amount_paise, status, failure_reason) VALUES ($1, $2, 'FAILED', $3)",
        [orderId, amountPaise, result.reason],
      );
      await appendToOutbox(tx, createEvent("PaymentFailed", orderId, { orderId, reason: result.reason }));
      logger.info({ orderId, reason: result.reason }, "payment failed");
    },
  };
}
