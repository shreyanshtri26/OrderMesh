-- Payment service schema. Owned by the Payment service only.

-- One payment attempt per order (UNIQUE): a redelivered PaymentRequested
-- can never charge twice, even before the consumer's idempotency check.
CREATE TABLE payments (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id        uuid        NOT NULL UNIQUE,
  amount_paise    integer     NOT NULL CHECK (amount_paise >= 0),
  status          text        NOT NULL CHECK (status IN ('COMPLETED', 'FAILED')),
  failure_reason  text,
  created_at      timestamptz NOT NULL DEFAULT now()
);
