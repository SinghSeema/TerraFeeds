-- Applied at every startup; every statement is idempotent.

-- Append-only log and the source of truth. Rows are never deleted; retraction tombstones them.
CREATE TABLE IF NOT EXISTS events (
  event_id      text PRIMARY KEY,
  kind          text NOT NULL CHECK (kind IN ('transfer', 'retract')),
  height        bigint,                       -- null for retract
  asset_id      text,
  from_investor text,                         -- null = initial issuance
  to_investor   text,
  units         bigint CHECK (units > 0),
  from_height   bigint,                       -- retract only
  content_hash  text NOT NULL,
  status        text NOT NULL CHECK (status IN ('waiting', 'applied', 'retracted', 'rejected')),
  received_at   timestamptz DEFAULT now(),
  applied_at    timestamptz
);
CREATE INDEX IF NOT EXISTS events_status_height ON events (status, height);
CREATE INDEX IF NOT EXISTS events_height ON events (height);

-- Derived from applied events; only changed in the same transaction as an apply or an undo.
CREATE TABLE IF NOT EXISTS holdings (
  asset_id    text NOT NULL,
  investor_id text NOT NULL,
  units       bigint NOT NULL CONSTRAINT holdings_units_nonnegative CHECK (units >= 0),
  PRIMARY KEY (asset_id, investor_id)
);

-- Single row; its FOR UPDATE lock is the frontier lock that serializes apply and retraction.
CREATE TABLE IF NOT EXISTS ledger_state (
  id             int PRIMARY KEY CHECK (id = 1),
  frontier       bigint NOT NULL,
  highest_seen   bigint NOT NULL,
  blocked_at     bigint,
  blocked_reason text
);
-- Highest height ever finalized. Only rises: a rewind lowers the frontier, but never what was declared final.
-- Added in place so a database created before this column upgrades on the next start.
ALTER TABLE ledger_state ADD COLUMN IF NOT EXISTS finalized_through bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS assets (
  asset_id      text PRIMARY KEY,
  issued_supply bigint NOT NULL CHECK (issued_supply >= 0)
);
