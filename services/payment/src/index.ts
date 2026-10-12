import { createLogger, runService, TOPICS } from "@ordermesh/shared";
import { DEFAULTS, MIGRATIONS_DIR, NAME } from "./defaults.ts";
import { createMockGateway } from "./gateway.ts";
import { paymentHandlers } from "./handlers.ts";

// Payment service: charges orders through a mock gateway.
const gateway = createMockGateway({
  failureRate: Number(process.env.PAYMENT_FAILURE_RATE ?? 0.2),
  latencyMs: Number(process.env.PAYMENT_LATENCY_MS ?? 150),
});

await runService({
  name: NAME,
  defaults: DEFAULTS,
  migrationsDir: MIGRATIONS_DIR,
  consumes: {
    topics: [TOPICS.orders],
    handlers: paymentHandlers(createLogger("payment-handlers", process.env.LOG_LEVEL), gateway),
  },
});
