import { createLogger, runService, TOPICS } from "@ordermesh/shared";
import { DEFAULTS, MIGRATIONS_DIR, NAME } from "./defaults.ts";
import { inventoryHandlers } from "./handlers.ts";
import { seedStock } from "./seed.ts";

// Inventory service: reserves stock for new orders and releases it when the
// saga compensates.
await runService({
  name: NAME,
  defaults: DEFAULTS,
  migrationsDir: MIGRATIONS_DIR,
  consumes: {
    topics: [TOPICS.orders],
    handlers: inventoryHandlers(createLogger("inventory-handlers", process.env.LOG_LEVEL)),
  },
  routes: async ({ app, pool }) => {
    await seedStock(pool);
    app.get("/stock", async () => (await pool.query("SELECT sku, available, reserved FROM stock ORDER BY sku")).rows);
  },
});
