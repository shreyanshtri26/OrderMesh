import { fileURLToPath } from "node:url";

// Local-development defaults. Override with NOTIFICATION_DATABASE_URL / NOTIFICATION_PORT
// (or DATABASE_URL / PORT) in the environment.
export const NAME = "notification";
export const DEFAULTS = {
  PORT: 3004,
  DATABASE_URL: "postgresql://ordermesh:ordermesh@localhost:5432/notifications",
};
export const MIGRATIONS_DIR = fileURLToPath(new URL("../migrations", import.meta.url));
