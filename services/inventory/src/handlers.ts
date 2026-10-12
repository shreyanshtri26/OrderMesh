import type { Handlers, Logger } from "@ordermesh/shared";

// ─────────────────────────────────────────────────────────────────────────────
// TODO (Roadmap week 2): implement stock reservation.
//
// OrderCreated
//   1. In the handler's transaction, lock the stock rows for the order's SKUs:
//        SELECT sku, available FROM stock WHERE sku = ANY($1) ORDER BY sku FOR UPDATE
//      (ORDER BY sku: always lock in the same order, so two orders cannot deadlock.)
//   2. If any SKU has available < quantity:
//        appendToOutbox(tx, createEvent("InventoryRejected", orderId, { orderId, reason }))
//   3. Otherwise, for each item: available -= quantity, reserved += quantity;
//      insert a reservations row (status RESERVED); then
//        appendToOutbox(tx, createEvent("InventoryReserved", orderId, { orderId }))
//
// ReleaseInventoryRequested   (the compensating action)
//   1. Lock the reservation row. If it is missing or already RELEASED, emit
//      InventoryReleased anyway (releasing twice must be safe).
//   2. Return each quantity: available += quantity, reserved -= quantity.
//   3. Mark the reservation RELEASED and emit InventoryReleased.
//
// Prove it: run 2 concurrent orders for SKU-SAFFRON-1G (stock: 1).
// Exactly one must be reserved and the other rejected. Write the result in SKILL.md.
// ─────────────────────────────────────────────────────────────────────────────

export function inventoryHandlers(logger: Logger): Handlers {
  return {
    OrderCreated: async (event) => {
      logger.warn({ orderId: event.payload.orderId }, "TODO: reserve stock (roadmap week 2)");
    },
    ReleaseInventoryRequested: async (event) => {
      logger.warn({ orderId: event.payload.orderId }, "TODO: release stock (roadmap week 3)");
    },
  };
}
