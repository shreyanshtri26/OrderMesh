import { createLogger, runService, TOPICS } from "@ordermesh/shared";
import { DEFAULTS, MIGRATIONS_DIR, NAME } from "./defaults.ts";
import { notificationHandlers } from "./handlers.ts";

// Notification service: tells the customer how their order ended.
await runService({
  name: NAME,
  defaults: DEFAULTS,
  migrationsDir: MIGRATIONS_DIR,
  consumes: {
    topics: [TOPICS.orders],
    handlers: notificationHandlers(createLogger("notification-handlers", process.env.LOG_LEVEL)),
  },
  routes: ({ app, pool }) => {
    app.get(
      "/notifications",
      async () =>
        (await pool.query("SELECT order_id, kind, message, created_at FROM notifications ORDER BY id DESC LIMIT 20"))
          .rows,
    );
  },
});
