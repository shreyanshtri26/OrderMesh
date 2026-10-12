import { CATALOG, type Pool } from "@ordermesh/shared";

/** Inserts starting stock for every catalogue SKU that does not have a row yet. */
export async function seedStock(pool: Pool): Promise<void> {
  for (const [sku, product] of Object.entries(CATALOG)) {
    await pool.query("INSERT INTO stock (sku, available) VALUES ($1, $2) ON CONFLICT (sku) DO NOTHING", [
      sku,
      product.initialStock,
    ]);
  }
}
