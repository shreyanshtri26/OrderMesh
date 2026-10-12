import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Config } from "./config.ts";

// Kafka client setup.
//
// Uses Confluent's official client (librdkafka) through its KafkaJS-compatible
// API. KafkaJS itself is unmaintained and does not support the protocol
// versions required by Kafka 4.x, which is what Aiven's free tier runs.

export type Kafka = KafkaJS.Kafka;
export type Producer = KafkaJS.Producer;
export type Consumer = KafkaJS.Consumer;
export type EachMessagePayload = KafkaJS.EachMessagePayload;

export function createKafka(config: Config): Kafka {
  const useSasl = Boolean(config.KAFKA_SASL_MECHANISM && config.KAFKA_SASL_USERNAME);
  return new KafkaJS.Kafka({
    kafkaJS: {
      clientId: config.SERVICE_NAME,
      brokers: config.KAFKA_BROKERS,
      ssl: useSasl,
      // The client checks whether the `sasl` key exists, so it must be
      // omitted entirely (not set to undefined) for a plain local broker.
      ...(useSasl
        ? {
            sasl: {
              mechanism: config.KAFKA_SASL_MECHANISM,
              username: config.KAFKA_SASL_USERNAME,
              password: config.KAFKA_SASL_PASSWORD,
            } as KafkaJS.SASLOptions,
          }
        : {}),
      logLevel: KafkaJS.logLevel.WARN,
    },
    // librdkafka option: trust the managed provider's CA certificate.
    ...(config.KAFKA_SSL_CA_PEM ? { "ssl.ca.pem": config.KAFKA_SSL_CA_PEM } : {}),
  });
}

/**
 * Idempotent producer with acks from all in-sync replicas: a network retry
 * cannot write the same message twice to a partition.
 */
export async function createProducer(kafka: Kafka): Promise<Producer> {
  const producer = kafka.producer({ kafkaJS: { idempotent: true, acks: -1 } });
  await producer.connect();
  return producer;
}

/** Converts Kafka message headers (Buffer | string | array) into plain strings. */
export function headersToStrings(headers: KafkaJS.IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers ?? {})) {
    if (value === undefined) continue;
    const first = Array.isArray(value) ? value[0] : value;
    if (first !== undefined) out[key] = first.toString();
  }
  return out;
}
