import Fastify, { type FastifyInstance } from "fastify";
import type { Pool } from "./db.ts";

// HTTP server factory with the two probes every service exposes:
//   GET /health  liveness: the process is up (never touches dependencies)
//   GET /ready   readiness: the database answers, so traffic can be sent here
// Kubernetes uses them as liveness and readiness probes. Kafka status is
// reported but does not fail readiness: the outbox absorbs a Kafka outage.

export function createServer(opts: {
  service: string;
  pool: Pool;
  logLevel: string;
  kafkaState: { status: string };
}): FastifyInstance {
  const app = Fastify({
    logger: { level: opts.logLevel, base: { service: opts.service } },
  });

  app.get("/health", async () => ({ status: "ok", service: opts.service, kafka: opts.kafkaState.status }));

  app.get("/ready", async (_req, reply) => {
    try {
      await opts.pool.query("SELECT 1");
      return { status: "ready", service: opts.service, kafka: opts.kafkaState.status };
    } catch {
      return reply.code(503).send({ status: "not_ready", service: opts.service, db: "unreachable" });
    }
  });

  return app;
}

export type { FastifyInstance };
