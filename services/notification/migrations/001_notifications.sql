-- Notification service schema. Owned by the Notification service only.

CREATE TABLE notifications (
  id          bigserial PRIMARY KEY,
  order_id    uuid        NOT NULL,
  kind        text        NOT NULL CHECK (kind IN ('ORDER_CONFIRMED', 'ORDER_CANCELLED')),
  message     text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notifications_order_idx ON notifications (order_id);
