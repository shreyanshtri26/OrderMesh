import { pino, type Logger } from "pino";

// Structured JSON logs. Every line carries the service name, so logs from all
// four services can be merged and filtered (and, with tracing on, correlated
// by trace id).

export function createLogger(service: string, level = "info"): Logger {
  return pino({
    level,
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    transport:
      process.env.NODE_ENV === "production" ? undefined : { target: "pino-pretty", options: { colorize: true } },
  });
}

export type { Logger };
