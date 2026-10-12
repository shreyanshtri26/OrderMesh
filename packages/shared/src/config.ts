import { z } from "zod";

// Configuration shared by every service, validated at startup.
// Each service passes its own name and defaults; secrets come from the
// environment (see .env.example).

const schema = z.object({
  SERVICE_NAME: z.string(),
  PORT: z.coerce.number().int().default(3000),
  DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .default("localhost:9092")
    .transform((s) => s.split(",").map((b) => b.trim())),
  // Managed Kafka (for example Aiven's free tier) needs SASL over TLS.
  KAFKA_SASL_MECHANISM: z.enum(["plain", "scram-sha-256", "scram-sha-512"]).optional(),
  KAFKA_SASL_USERNAME: z.string().optional(),
  KAFKA_SASL_PASSWORD: z.string().optional(),
  KAFKA_SSL_CA_PEM: z.string().optional(),
  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().default(500),
  LOG_LEVEL: z.enum(["trace", "debug", "info", "warn", "error"]).default("info"),
});

export type Config = z.infer<typeof schema>;

export function loadConfig(service: string, defaults: { PORT: number; DATABASE_URL: string }): Config {
  const parsed = schema.safeParse({
    ...defaults,
    ...process.env,
    // A per-service URL (ORDER_DATABASE_URL) wins over the generic one, so all
    // four services can share one .env while keeping separate databases.
    DATABASE_URL:
      process.env[`${service.toUpperCase()}_DATABASE_URL`] ?? process.env.DATABASE_URL ?? defaults.DATABASE_URL,
    PORT: process.env[`${service.toUpperCase()}_PORT`] ?? process.env.PORT ?? defaults.PORT,
    SERVICE_NAME: service,
  });
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `  - ${i.path.join(".")}: ${i.message}`).join("\n");
    throw new Error(`[${service}] invalid configuration:\n${issues}`);
  }
  return parsed.data;
}
