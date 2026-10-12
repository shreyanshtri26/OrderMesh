import { chmod, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TestProject } from "vitest/node";

// Integration tests need a real Postgres. If TEST_DATABASE_URL is set (for
// example in CI or with Docker), it is used. Otherwise an embedded Postgres
// is started from npm binaries, so the tests run without Docker.

declare module "vitest" {
  export interface ProvidedContext {
    databaseUrl: string;
  }
}

export default async function setup(project: TestProject) {
  if (process.env.TEST_DATABASE_URL) {
    project.provide("databaseUrl", process.env.TEST_DATABASE_URL);
    return;
  }

  const { default: EmbeddedPostgres } = await import("embedded-postgres");
  const dir = await mkdtemp(join(tmpdir(), "ordermesh-pg-"));
  // When running as root, embedded-postgres runs Postgres as a separate
  // unprivileged user, which needs access to the data directory.
  await chmod(dir, 0o777);
  const port = 54000 + Math.floor(Math.random() * 1000);
  const pg = new EmbeddedPostgres({
    databaseDir: join(dir, "data"),
    user: "ordermesh",
    password: "ordermesh",
    port,
    persistent: false,
    onLog: () => {},
  });
  await pg.initialise();
  await pg.start();
  await pg.createDatabase("ordermesh_test");
  project.provide("databaseUrl", `postgresql://ordermesh:ordermesh@localhost:${port}/ordermesh_test`);

  return async () => {
    await pg.stop();
    await rm(dir, { recursive: true, force: true });
  };
}
