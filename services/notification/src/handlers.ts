import type { Handlers, Logger } from "@ordermesh/shared";

// Records a customer notification for each finished order. A real system
// would send email or SMS here; storing the message keeps the demo free and
// lets the idempotency guarantee be checked with one SQL query:
//   SELECT order_id, count(*) FROM notifications GROUP BY 1 HAVING count(*) > 1;
// must always return zero rows, even after replaying events.

export function notificationHandlers(logger: Logger): Handlers {
  return {
    OrderConfirmed: async (event, tx) => {
      const message = `Your order ${event.payload.orderId.slice(0, 8)} is confirmed.`;
      await tx.query("INSERT INTO notifications (order_id, kind, message) VALUES ($1, 'ORDER_CONFIRMED', $2)", [
        event.payload.orderId,
        message,
      ]);
      logger.info({ orderId: event.payload.orderId }, message);
    },
    OrderCancelled: async (event, tx) => {
      const message = `Your order ${event.payload.orderId.slice(0, 8)} was cancelled (${event.payload.reason}).`;
      await tx.query("INSERT INTO notifications (order_id, kind, message) VALUES ($1, 'ORDER_CANCELLED', $2)", [
        event.payload.orderId,
        message,
      ]);
      logger.info({ orderId: event.payload.orderId }, message);
    },
  };
}
