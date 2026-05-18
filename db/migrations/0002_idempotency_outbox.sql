-- ============================================================
-- 0002_idempotency_outbox.sql
--
-- Adds an outbox payload to `idempotency_keys` so the
-- transaction-create handler commits its publish intent inside
-- the same Postgres transaction as the idempotency reservation.
-- A background worker drains unpublished rows to RabbitMQ.
--
-- End state after this migration:
--   * `outbox_payload  JSONB`              — wire-form message
--                                            for the queue.
--   * `published       BOOLEAN NOT NULL`   — set true after the
--                                            broker confirms the
--                                            publish.
--   * `published_at    TIMESTAMPTZ`        — observability only.
--   * `idx_idempotency_keys_unpublished`   — partial index on
--                                            (created_at) WHERE
--                                            NOT published, so
--                                            the worker scan is
--                                            cheap.
--   * Pre-existing rows are flagged `published = true` so the
--     worker never re-emits them.
--
-- All operations are idempotent. Apply against each shard primary
-- via the pg-haproxy frontend ports (e.g. 5000 for shard 0,
-- 5001 for shard 1). Fresh clusters reach the same end state
-- from `db/init.sql`.
-- ============================================================

ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS outbox_payload JSONB;
ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS published BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

-- Backfill: rows that exist at migration time were created by
-- code that published to RabbitMQ synchronously, so either the
-- publish reached the broker or the row was deleted on publish
-- failure. Either way, the worker must not re-emit them.
UPDATE idempotency_keys
   SET published = true,
       published_at = COALESCE(published_at, now())
 WHERE NOT published;

-- `CONCURRENTLY` omitted: incompatible with the default `sqlx::migrate!`
-- transaction wrap. `IF NOT EXISTS` makes the statement a no-op on a
-- cluster where `init.sql` already provisioned the index, so the brief
-- `AccessExclusiveLock` is acceptable here.
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_unpublished
    ON idempotency_keys (created_at)
    WHERE NOT published;

-- Idempotency-key cleanup sweeps any row past `expires_at + 1h
-- grace`. Unpublished 'processing' rows are NEVER swept early:
-- the publish-outbox worker is the only thing that turns them
-- into queue messages, and a sweep would discard a payload the
-- producer already returned 202 for.
CREATE OR REPLACE FUNCTION cleanup_expired_idempotency_keys()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM idempotency_keys
    WHERE expires_at < NOW() - INTERVAL '1 hour';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
