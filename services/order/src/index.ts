import { createLogger, runService, TOPICS } from "@ordermesh/shared";
import { DEFAULTS, MIGRATIONS_DIR, NAME } from "./defaults.ts";
import { sagaHandlers } from "./handlers.ts";
import { registerRoutes } from "./routes.ts";

// Order service: the public API and the saga orchestrator.
await runService({
  name: NAME,
  defaults: DEFAULTS,
  migrationsDir: MIGRATIONS_DIR,
  consumes: {
    topics: [TOPICS.inventory, TOPICS.payments],
    handlers: sagaHandlers(createLogger("order-saga", process.env.LOG_LEVEL)),
  },
  routes: registerRoutes,
});
