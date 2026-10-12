import type { EventType } from "@ordermesh/shared";

// The order saga, as a pure state machine.
//
// The Order service orchestrates: it reacts to events from Inventory and
// Payment, moves the order to its next state, and emits the next command.
// When payment fails AFTER stock was reserved, it emits a compensating
// command (ReleaseInventoryRequested) to undo the reservation.
//
//   PENDING ──InventoryReserved──► INVENTORY_RESERVED ──PaymentCompleted──► CONFIRMED
//      │                                  │
//      └─InventoryRejected─► CANCELLED    └─PaymentFailed─► CANCELLING ──InventoryReleased──► CANCELLED
//
// Any other (state, event) pair is not a valid move and is ignored and logged,
// which also makes late or out-of-order events harmless.

export const ORDER_STATUSES = ["PENDING", "INVENTORY_RESERVED", "CONFIRMED", "CANCELLING", "CANCELLED"] as const;
export type OrderStatus = (typeof ORDER_STATUSES)[number];

export type Step = { next: OrderStatus; emit: EventType };

const TRANSITIONS: Partial<Record<OrderStatus, Partial<Record<EventType, Step>>>> = {
  PENDING: {
    InventoryReserved: { next: "INVENTORY_RESERVED", emit: "PaymentRequested" },
    InventoryRejected: { next: "CANCELLED", emit: "OrderCancelled" },
  },
  INVENTORY_RESERVED: {
    PaymentCompleted: { next: "CONFIRMED", emit: "OrderConfirmed" },
    PaymentFailed: { next: "CANCELLING", emit: "ReleaseInventoryRequested" },
  },
  CANCELLING: {
    InventoryReleased: { next: "CANCELLED", emit: "OrderCancelled" },
  },
};

/** Returns the next step, or null when the event does not apply in this state. */
export function nextStep(status: OrderStatus, event: EventType): Step | null {
  return TRANSITIONS[status]?.[event] ?? null;
}

export function isTerminal(status: OrderStatus): boolean {
  return status === "CONFIRMED" || status === "CANCELLED";
}

/** Events the orchestrator listens to, derived from the table above. */
export const SAGA_INPUT_EVENTS = [
  ...new Set(Object.values(TRANSITIONS).flatMap((byEvent) => Object.keys(byEvent ?? {}))),
] as EventType[];
