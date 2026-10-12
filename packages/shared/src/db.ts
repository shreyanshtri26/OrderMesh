import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import pg from "pg";
import type { Logger } from "./logger.ts";

// Database helpers. Each service owns its own database; nothing here lets one
// service reach into another's tables.

export type Pool = pg.Pool;
export type Tx = pg.PoolClient;

export function createPool(connectionString: string): Pool {
  const ssl = /sslmode=require/.test(connectionString) ? { rejectUnauthorized: false } : undefined;
  return new pg.Pool({ connectionString, ssl, max: 5, idleTimeoutMillis: 10_000 });
}

/** Runs `fn` inside BEGIN/COMMIT, rolling back on any error. */
export async function withTransaction<T>(pool: Pool, fn: (tx: Tx) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

// Tables every service needs: the outbox (events waiting to be published) and
// processed_events (event ids this service has already handled).
const INFRA_SQL = `
CREATE TABLE IF NOT EXISTS outbox (
  id            uuid PRIMARY KEY,
  topic         text        NOT NULL,
  message_key   text        NOT NULL,
  event_type    text        NOT NULL,
  envelope      jsonb       NOT NULL,
  headers       jsonb       NOT NULL DEFAULT '{}'::jsonb,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
  event_id      uuid        NOT NULL,
  consumer      text        NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer)
);
`;

/**
 * Applies the shared infra tables, then every `NNN_name.sql` file in
 * `migrationsDir` that has not run yet, each in its own transaction.
 * An advisory lock stops two instances from migrating at the same time.
 */
export async function migrate(pool: Pool, migrationsDir: string, logger: Logger): Promise<void> {
  await withTransaction(pool, async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(727274)");
    await tx.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);
    await tx.query(INFRA_SQL);

    const applied = new Set(
      (await tx.query<{ name: string }>("SELECT name FROM schema_migrations")).rows.map((r) => r.name),
    );
    const files = (await readdir(migrationsDir)).filter((f) => /^\d+_.+\.sql$/.test(f)).sort();

    for (const file of files) {
      if (applied.has(file)) continue;
      await tx.query(await readFile(join(migrationsDir, file), "utf8"));
      await tx.query("INSERT INTO schema_migrations (name) VALUES ($1)", [file]);
      logger.info({ migration: file }, "applied migration");
    }
  });
}
