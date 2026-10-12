import { fileURLToPath } from "node:url";

// Local-development defaults. Override with PAYMENT_DATABASE_URL / PAYMENT_PORT
// (or DATABASE_URL / PORT) in the environment.
export const NAME = "payment";
export const DEFAULTS = {
  PORT: 3003,
  DATABASE_URL: "postgresql://ordermesh:ordermesh@localhost:5432/payments",
};
export const MIGRATIONS_DIR = fileURLToPath(new URL("../migrations", import.meta.url));
