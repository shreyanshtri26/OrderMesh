-- Order service schema. Owned by the Order service only.

CREATE TABLE orders (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id      text        NOT NULL,
  status           text        NOT NULL CHECK (status IN ('PENDING','INVENTORY_RESERVED','CONFIRMED','CANCELLING','CANCELLED')),
  items            jsonb       NOT NULL,
  total_paise      integer     NOT NULL CHECK (total_paise >= 0),
  cancel_reason    text,
  -- Client-supplied Idempotency-Key: a retried POST returns the original order.
  idempotency_key  text UNIQUE,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_created_at_idx ON orders (created_at DESC);

-- Every saga step, for debugging and for the demo timeline.
CREATE TABLE order_status_history (
  id           bigserial PRIMARY KEY,
  order_id     uuid        NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  from_status  text        NOT NULL,
  to_status    text        NOT NULL,
  event_id     uuid        NOT NULL,
  event_type   text        NOT NULL,
  at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX order_status_history_order_idx ON order_status_history (order_id, at);
