-- Inventory service schema. Owned by the Inventory service only.

-- Stock is kept per SKU. The CHECK constraints are the last line of defence:
-- even a buggy handler cannot drive stock below zero.
CREATE TABLE stock (
  sku        text PRIMARY KEY,
  available  integer NOT NULL CHECK (available >= 0),
  reserved   integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- One reservation per order. Releasing it returns the quantities to stock.
CREATE TABLE reservations (
  order_id    uuid PRIMARY KEY,
  items       jsonb       NOT NULL,
  status      text        NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz
);
