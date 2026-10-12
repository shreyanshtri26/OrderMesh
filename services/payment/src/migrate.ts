import { runMigrations } from "@ordermesh/shared";
import { DEFAULTS, MIGRATIONS_DIR, NAME } from "./defaults.ts";

await runMigrations({ name: NAME, defaults: DEFAULTS, migrationsDir: MIGRATIONS_DIR });
