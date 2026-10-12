import { fileURLToPath } from "node:url";

// Local-development defaults. Override with ORDER_DATABASE_URL / ORDER_PORT
// (or DATABASE_URL / PORT) in the environment.
export const NAME = "order";
export const DEFAULTS = {
  PORT: 3001,
  DATABASE_URL: "postgresql://ordermesh:ordermesh@localhost:5432/orders",
};
export const MIGRATIONS_DIR = fileURLToPath(new URL("../migrations", import.meta.url));
