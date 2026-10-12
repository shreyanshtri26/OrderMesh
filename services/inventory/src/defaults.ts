import { fileURLToPath } from "node:url";

// Local-development defaults. Override with INVENTORY_DATABASE_URL / INVENTORY_PORT
// (or DATABASE_URL / PORT) in the environment.
export const NAME = "inventory";
export const DEFAULTS = {
  PORT: 3002,
  DATABASE_URL: "postgresql://ordermesh:ordermesh@localhost:5432/inventory",
};
export const MIGRATIONS_DIR = fileURLToPath(new URL("../migrations", import.meta.url));
