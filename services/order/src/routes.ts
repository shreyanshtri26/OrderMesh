import { timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import {
  appendToOutbox,
  CATALOG,
  createEvent,
  createKafka,
  createProducer,
  isSku,
  readDlq,
  replayFromDlq,
  withTransaction,
  z,
  type Producer,
  type ServiceContext,
} from "@ordermesh/shared";

// Public API of OrderMesh. The Order service is the only one exposed to clients.

const createOrderBody = z.object({
  customerId: z.string().min(1).max(64),
  items: z
    .array(z.object({ sku: z.string().refine(isSku, "Unknown SKU"), quantity: z.number().int().min(1).max(20) }))
    .min(1)
    .max(10),
});

type OrderRow = {
  id: string;
  customer_id: string;
  status: string;
  items: unknown;
  total_paise: number;
  cancel_reason: string | null;
  created_at: string;
  updated_at: string;
};

// The storefront is one static page; read once at startup, no build step.
const STOREFRONT = readFileSync(new URL("../public/index.html", import.meta.url), "utf8");

const replayBody = z.object({ partition: z.number().int().min(0), offset: z.number().int().min(0) });

function tokenMatches(header: string | undefined, token: string): boolean {
  const given = Buffer.from(header ?? "");
  const wanted = Buffer.from(`Bearer ${token}`);
  return given.length === wanted.length && timingSafeEqual(given, wanted);
}

export function registerRoutes({ app, pool, config }: ServiceContext) {
  app.get("/", async (_request, reply) => reply.type("text/html; charset=utf-8").send(STOREFRONT));

  // Dead-letter queue operations. Off unless ADMIN_TOKEN is set, because they
  // expose message bodies and can re-run events.
  const adminToken = process.env.ADMIN_TOKEN;
  if (adminToken) {
    const kafka = createKafka(config);
    let producer: Producer | undefined;

    app.get("/admin/dlq", async (request, reply) => {
      if (!tokenMatches(request.headers.authorization, adminToken))
        return reply.code(401).send({ error: "unauthorized" });
      const entries = await readDlq(kafka);
      return { count: entries.length, entries };
    });

    app.post("/admin/dlq/replay", async (request, reply) => {
      if (!tokenMatches(request.headers.authorization, adminToken))
        return reply.code(401).send({ error: "unauthorized" });
      const parsed = replayBody.safeParse(request.body);
      if (!parsed.success) return reply.code(400).send({ error: "invalid_request", issues: parsed.error.issues });

      producer ??= await createProducer(kafka);
      const entry = await replayFromDlq(kafka, producer, parsed.data);
      if (!entry) return reply.code(404).send({ error: "not_found_or_not_replayable" });
      return { replayed: true, topic: entry.originalTopic, key: entry.key };
    });
  }
  app.get("/catalog", async () => CATALOG);

  /**
   * Creates an order and returns 202 immediately. The outcome arrives later
   * through the saga (eventual consistency): poll GET /orders/:id.
   *
   * Clients may send an `Idempotency-Key` header. Retrying the same request
   * with the same key returns the original order instead of creating a second.
   */
  app.post("/orders", async (request, reply) => {
    const parsed = createOrderBody.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: "invalid_request", issues: parsed.error.issues });
    }
    const idempotencyKey = request.headers["idempotency-key"]?.toString() ?? null;

    if (idempotencyKey) {
      const existing = await pool.query<OrderRow>("SELECT * FROM orders WHERE idempotency_key = $1", [idempotencyKey]);
      if (existing.rows[0]) return reply.code(200).send(toApi(existing.rows[0]));
    }

    const items = parsed.data.items.map((item) => ({
      sku: item.sku,
      quantity: item.quantity,
      unitPricePaise: CATALOG[item.sku as keyof typeof CATALOG].unitPricePaise,
    }));
    const totalPaise = items.reduce((sum, i) => sum + i.unitPricePaise * i.quantity, 0);

    try {
      const order = await withTransaction(pool, async (tx) => {
        const { rows } = await tx.query<OrderRow>(
          `INSERT INTO orders (customer_id, status, items, total_paise, idempotency_key)
           VALUES ($1, 'PENDING', $2, $3, $4) RETURNING *`,
          [parsed.data.customerId, JSON.stringify(items), totalPaise, idempotencyKey],
        );
        const row = rows[0]!;
        // Same transaction: the order and its OrderCreated event commit together.
        await appendToOutbox(
          tx,
          createEvent("OrderCreated", row.id, {
            orderId: row.id,
            customerId: row.customer_id,
            items,
            totalPaise,
          }),
        );
        return row;
      });
      return reply.code(202).send(toApi(order));
    } catch (error) {
      // Two concurrent requests with the same key: the unique index lets only
      // one insert win; the loser returns the winner's order.
      if ((error as { code?: string }).code === "23505" && idempotencyKey) {
        const existing = await pool.query<OrderRow>("SELECT * FROM orders WHERE idempotency_key = $1", [
          idempotencyKey,
        ]);
        if (existing.rows[0]) return reply.code(200).send(toApi(existing.rows[0]));
      }
      throw error;
    }
  });

  app.get<{ Params: { id: string } }>("/orders/:id", async (request, reply) => {
    if (!z.string().uuid().safeParse(request.params.id).success) {
      return reply.code(404).send({ error: "not_found" });
    }
    const { rows } = await pool.query<OrderRow>("SELECT * FROM orders WHERE id = $1", [request.params.id]);
    if (!rows[0]) return reply.code(404).send({ error: "not_found" });

    const history = await pool.query(
      `SELECT from_status AS "from", to_status AS "to", event_type AS "event", at
         FROM order_status_history WHERE order_id = $1 ORDER BY at`,
      [request.params.id],
    );
    return { ...toApi(rows[0]), history: history.rows };
  });

  app.get("/orders", async () => {
    const { rows } = await pool.query<OrderRow>("SELECT * FROM orders ORDER BY created_at DESC LIMIT 20");
    return rows.map(toApi);
  });
}

function toApi(row: OrderRow) {
  return {
    id: row.id,
    customerId: row.customer_id,
    status: row.status,
    items: row.items,
    totalPaise: row.total_paise,
    cancelReason: row.cancel_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
