import { appendToOutbox, createEvent, type Handlers, type Logger } from "@ordermesh/shared";

// Stock reservation with row locks.
//
// Two concurrent orders for the last unit of a SKU cannot both succeed: the
// first transaction locks the stock row with FOR UPDATE, and the second waits
// for it to commit, then reads the reduced count and is rejected.
//
// Rows are always locked in SKU order, so two orders that touch the same SKUs
// cannot deadlock each other.

type Item = { sku: string; quantity: number };

export function inventoryHandlers(logger: Logger): Handlers {
  return {
    OrderCreated: async (event, tx) => {
      const { orderId, items } = event.payload;

      // Sum quantities per SKU first, so an order that lists the same SKU twice
      // is checked against the total it needs.
      const needed = new Map<string, number>();
      for (const item of items) needed.set(item.sku, (needed.get(item.sku) ?? 0) + item.quantity);
      const skus = [...needed.keys()].sort();

      const { rows } = await tx.query<{ sku: string; available: number }>(
        "SELECT sku, available FROM stock WHERE sku = ANY($1) ORDER BY sku FOR UPDATE",
        [skus],
      );
      const available = new Map(rows.map((row) => [row.sku, row.available]));

      const short = skus.find((sku) => (available.get(sku) ?? 0) < needed.get(sku)!);
      if (short) {
        const reason = `out_of_stock:${short}`;
        logger.info({ orderId, reason }, "stock rejected");
        await appendToOutbox(tx, createEvent("InventoryRejected", orderId, { orderId, reason }));
        return;
      }

      const reserved: Item[] = skus.map((sku) => ({ sku, quantity: needed.get(sku)! }));
      for (const { sku, quantity } of reserved) {
        await tx.query(
          "UPDATE stock SET available = available - $2, reserved = reserved + $2, updated_at = now() WHERE sku = $1",
          [sku, quantity],
        );
      }
      await tx.query("INSERT INTO reservations (order_id, items, status) VALUES ($1, $2, 'RESERVED')", [
        orderId,
        JSON.stringify(reserved),
      ]);
      await appendToOutbox(tx, createEvent("InventoryReserved", orderId, { orderId }));
      logger.info({ orderId }, "stock reserved");
    },

    // The compensating action. Safe to run twice: a missing or already released
    // reservation changes nothing, and InventoryReleased is emitted either way,
    // so the saga can always finish.
    ReleaseInventoryRequested: async (event, tx) => {
      const { orderId, reason } = event.payload;

      const { rows } = await tx.query<{ items: Item[]; status: string }>(
        "SELECT items, status FROM reservations WHERE order_id = $1 FOR UPDATE",
        [orderId],
      );
      const reservation = rows[0];
      if (reservation?.status === "RESERVED") {
        const items = [...reservation.items].sort((a, b) => a.sku.localeCompare(b.sku));
        for (const { sku, quantity } of items) {
          await tx.query(
            "UPDATE stock SET available = available + $2, reserved = reserved - $2, updated_at = now() WHERE sku = $1",
            [sku, quantity],
          );
        }
        await tx.query("UPDATE reservations SET status = 'RELEASED', released_at = now() WHERE order_id = $1", [
          orderId,
        ]);
        logger.info({ orderId, reason }, "stock released");
      } else {
        logger.info({ orderId, reason }, "nothing to release");
      }
      await appendToOutbox(tx, createEvent("InventoryReleased", orderId, { orderId }));
    },
  };
}
