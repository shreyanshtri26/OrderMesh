import { context, propagation, SpanKind, SpanStatusCode, trace, type Context } from "@opentelemetry/api";

// Distributed tracing across Kafka.
//
// HTTP and Postgres spans come from OpenTelemetry auto-instrumentation (loaded
// with `node --import @opentelemetry/auto-instrumentations-node/register`).
// Kafka hops are traced by hand: the W3C `traceparent` of the request that
// wrote an event is stored with the outbox row, sent as a Kafka header, and
// restored by the consumer. One order can then be followed across all four
// services in Jaeger.

const tracer = trace.getTracer("ordermesh");

/** Serialises the active trace context into plain string headers. */
export function currentTraceHeaders(): Record<string, string> {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier;
}

/** Rebuilds a trace context from message headers. */
export function contextFromHeaders(headers: Record<string, string>): Context {
  return propagation.extract(context.active(), headers);
}

/** Runs `fn` inside a span that is a child of `parent`, recording errors. */
export async function withSpan<T>(
  name: string,
  kind: "producer" | "consumer" | "internal",
  parent: Context,
  attributes: Record<string, string | number>,
  fn: () => Promise<T>,
): Promise<T> {
  const spanKind =
    kind === "producer" ? SpanKind.PRODUCER : kind === "consumer" ? SpanKind.CONSUMER : SpanKind.INTERNAL;
  return tracer.startActiveSpan(name, { kind: spanKind, attributes }, parent, async (span) => {
    try {
      return await fn();
    } catch (error) {
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw error;
    } finally {
      span.end();
    }
  });
}
