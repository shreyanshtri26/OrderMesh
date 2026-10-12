import { describe, expect, it } from "vitest";
import { isTerminal, nextStep, ORDER_STATUSES, SAGA_INPUT_EVENTS } from "../src/saga.ts";

describe("order saga", () => {
  it("follows the happy path to CONFIRMED", () => {
    expect(nextStep("PENDING", "InventoryReserved")).toEqual({ next: "INVENTORY_RESERVED", emit: "PaymentRequested" });
    expect(nextStep("INVENTORY_RESERVED", "PaymentCompleted")).toEqual({ next: "CONFIRMED", emit: "OrderConfirmed" });
  });

  it("compensates when payment fails after stock was reserved", () => {
    expect(nextStep("INVENTORY_RESERVED", "PaymentFailed")).toEqual({
      next: "CANCELLING",
      emit: "ReleaseInventoryRequested",
    });
    expect(nextStep("CANCELLING", "InventoryReleased")).toEqual({ next: "CANCELLED", emit: "OrderCancelled" });
  });

  it("cancels straight away when stock is not available", () => {
    expect(nextStep("PENDING", "InventoryRejected")).toEqual({ next: "CANCELLED", emit: "OrderCancelled" });
  });

  it("ignores late or out-of-order events", () => {
    expect(nextStep("CANCELLED", "PaymentCompleted")).toBeNull();
    expect(nextStep("PENDING", "PaymentCompleted")).toBeNull();
    expect(nextStep("CONFIRMED", "InventoryReserved")).toBeNull();
  });

  it("never leaves a terminal state", () => {
    for (const status of ORDER_STATUSES.filter(isTerminal)) {
      for (const event of SAGA_INPUT_EVENTS) expect(nextStep(status, event)).toBeNull();
    }
  });

  it("listens to exactly the events in the transition table", () => {
    expect(new Set(SAGA_INPUT_EVENTS)).toEqual(
      new Set(["InventoryReserved", "InventoryRejected", "PaymentCompleted", "PaymentFailed", "InventoryReleased"]),
    );
  });
});
