import { loadConfig, type Config } from "./config.ts";
import { backoffMs, startConsumer, type Handlers } from "./consumer.ts";
import { createPool, migrate, type Pool } from "./db.ts";
import { createServer, type FastifyInstance } from "./http.ts";
import { createKafka, createProducer, type Consumer, type Producer } from "./kafka.ts";
import { createLogger, type Logger } from "./logger.ts";
import { startOutboxRelay } from "./outbox.ts";

// Boots a service the same way every time:
//   config → logger → database (+ migrations) → HTTP → Kafka (in background)
//
// HTTP starts BEFORE Kafka is connected, on purpose. Thanks to the outbox,
// the Order API can keep accepting orders while Kafka is down: events wait in
// the outbox table and are published once the broker is back.
// On SIGTERM everything shuts down in reverse order so in-flight work finishes.

export type ServiceContext = {
  config: Config;
  logger: Logger;
  pool: Pool;
  app: FastifyInstance;
};

async function retryForever<T>(label: string, logger: Logger, fn: () => Promise<T>): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const wait = backoffMs(attempt, 1_000, 30_000);
      logger.warn({ err: error, attempt, retryInMs: wait }, `${label} failed; retrying`);
      await new Promise((resolve) => setTimeout(resolve, wait));
    }
  }
}

export async function runService(opts: {
  name: string;
  defaults: { PORT: number; DATABASE_URL: string };
  migrationsDir: string;
  consumes?: { topics: string[]; handlers: Handlers };
  routes?: (ctx: ServiceContext) => void | Promise<void>;
}): Promise<void> {
  const config = loadConfig(opts.name, opts.defaults);
  const logger = createLogger(opts.name, config.LOG_LEVEL);
  const pool = createPool(config.DATABASE_URL);

  await retryForever("database migration", logger, () => migrate(pool, opts.migrationsDir, logger));

  const kafkaState = { status: "connecting" as "connecting" | "connected" };
  const app = createServer({ service: opts.name, pool, logLevel: config.LOG_LEVEL, kafkaState });
  await opts.routes?.({ config, logger, pool, app });
  await app.listen({ port: config.PORT, host: "0.0.0.0" });

  const kafka = createKafka(config);
  let producer: Producer | undefined;
  let consumer: Consumer | undefined;
  let relay: { stop: () => Promise<void> } | undefined;

  const connectKafka = (async () => {
    producer = await retryForever("kafka producer connect", logger, () => createProducer(kafka));
    relay = startOutboxRelay({ pool, producer, logger, intervalMs: config.OUTBOX_POLL_INTERVAL_MS });
    if (opts.consumes) {
      const { topics, handlers } = opts.consumes;
      const p = producer;
      consumer = await retryForever("kafka consumer start", logger, () =>
        startConsumer({ kafka, pool, producer: p, logger, consumerName: opts.name, topics, handlers }),
      );
    }
    kafkaState.status = "connected";
    logger.info("kafka connected");
  })();
  connectKafka.catch((error) => logger.error({ err: error }, "kafka setup crashed"));

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "shutting down");
    await app.close();
    await consumer?.disconnect().catch(() => {});
    await relay?.stop();
    await producer?.disconnect().catch(() => {});
    await pool.end();
    process.exit(0);
  };
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
  process.once("SIGINT", () => void shutdown("SIGINT"));
}

/** Standalone migration entry point (`pnpm migrate`). */
export async function runMigrations(opts: {
  name: string;
  defaults: { PORT: number; DATABASE_URL: string };
  migrationsDir: string;
}): Promise<void> {
  const config = loadConfig(opts.name, opts.defaults);
  const logger = createLogger(opts.name, config.LOG_LEVEL);
  const pool = createPool(config.DATABASE_URL);
  try {
    await migrate(pool, opts.migrationsDir, logger);
    logger.info("migrations up to date");
  } finally {
    await pool.end();
  }
}
