import {
  appendToOutbox,
  createEvent,
  type Envelope,
  type EventType,
  type Handler,
  type Handlers,
  type Logger,
  type PayloadOf,
  type Tx,
} from "@ordermesh/shared";
import { nextStep, SAGA_INPUT_EVENTS, type OrderStatus } from "./saga.ts";

// Saga orchestrator: applies the state machine in saga.ts to the database.
// Runs inside the idempotent consumer's transaction, so the status change,
// its history row and the next command in the outbox commit together.

type OrderRow = { id: string; status: OrderStatus; total_paise: number; cancel_reason: string | null };

/** Builds the command the saga emits next. `reason` explains a cancellation. */
function commandFor(type: EventType, order: OrderRow, reason: string): Envelope {
  switch (type) {
    case "PaymentRequested":
      return createEvent("PaymentRequested", order.id, { orderId: order.id, amountPaise: order.total_paise });
    case "ReleaseInventoryRequested":
      return createEvent("ReleaseInventoryRequested", order.id, { orderId: order.id, reason });
    case "OrderConfirmed":
      return createEvent("OrderConfirmed", order.id, { orderId: order.id });
    case "OrderCancelled":
      return createEvent("OrderCancelled", order.id, { orderId: order.id, reason });
    default:
      throw new Error(`Order service does not emit ${type}`);
  }
}

export function sagaHandlers(logger: Logger): Handlers {
  const handle: Handler = async (event: Envelope, tx: Tx) => {
    const { orderId } = event.payload as PayloadOf<"InventoryReserved">;

    // Lock the order row: two events for the same order cannot interleave.
    const { rows } = await tx.query<OrderRow>(
      "SELECT id, status, total_paise, cancel_reason FROM orders WHERE id = $1 FOR UPDATE",
      [orderId],
    );
    const order = rows[0];
    if (!order) {
      logger.warn({ orderId, type: event.type }, "event for unknown order, ignoring");
      return;
    }

    const step = nextStep(order.status, event.type);
    if (!step) {
      logger.warn({ orderId, status: order.status, type: event.type }, "event not valid in this state, ignoring");
      return;
    }

    // The first failure's reason is kept: when the compensation finishes
    // (InventoryReleased), the customer still learns why the order failed.
    const incomingReason = (event.payload as { reason?: string }).reason ?? null;
    const cancelReason = order.cancel_reason ?? incomingReason;

    await tx.query(`UPDATE orders SET status = $2, cancel_reason = $3, updated_at = now() WHERE id = $1`, [
      orderId,
      step.next,
      cancelReason,
    ]);
    await tx.query(
      `INSERT INTO order_status_history (order_id, from_status, to_status, event_id, event_type)
       VALUES ($1, $2, $3, $4, $5)`,
      [orderId, order.status, step.next, event.id, event.type],
    );
    await appendToOutbox(tx, commandFor(step.emit, order, cancelReason ?? "unknown"));
  };

  return Object.fromEntries(SAGA_INPUT_EVENTS.map((type) => [type, handle])) as Handlers;
}
