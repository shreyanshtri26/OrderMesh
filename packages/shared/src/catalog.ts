// Demo product catalogue. Order uses it for prices; Inventory seeds stock
// from it. In a real system this would be its own service; it is a constant
// here to keep the focus on the order pipeline.

export const CATALOG = {
  "SKU-TEA-250": { name: "Assam tea, 250 g", unitPricePaise: 24_900, initialStock: 50 },
  "SKU-RICE-5KG": { name: "Basmati rice, 5 kg", unitPricePaise: 69_900, initialStock: 20 },
  "SKU-GHEE-1L": { name: "Desi ghee, 1 L", unitPricePaise: 64_900, initialStock: 10 },
  "SKU-SAFFRON-1G": { name: "Kashmiri saffron, 1 g", unitPricePaise: 39_900, initialStock: 1 },
} as const;

export type Sku = keyof typeof CATALOG;

export function isSku(value: string): value is Sku {
  return Object.hasOwn(CATALOG, value);
}
