-- ============================================================
-- 0001_transactions_keyset_index.sql
--
-- Aligns the `transactions` table's time-ordered index set with
-- the keyset pagination shape used by `GET /api/v2/transactions`.
--
-- End state after this migration:
--   * `idx_transactions_created_id (created_at DESC, id DESC)`
--     is the only time-ordered index on the table.
--   * `idx_transactions_created_at` and
--     `idx_transactions_created_at_brin` do not exist.
--
-- The composite covers both the ORDER BY and the cursor
-- predicate one-to-one; keeping it as the only choice prevents
-- the planner from picking a bitmap-heap-scan path on small
-- `LIMIT` reads.
--
-- `IF NOT EXISTS` / `IF EXISTS` make the script idempotent — on
-- a cluster where `init.sql` already provisioned the index the
-- statements are no-ops and the `_sqlx_migrations` row is the
-- only side effect. `CONCURRENTLY` is intentionally NOT used:
-- the migrator runs inside the default `sqlx::migrate!`
-- transaction wrap, and `CONCURRENTLY` is incompatible with
-- transaction blocks. The brief `AccessExclusiveLock` is
-- acceptable here because the operation is idempotent — on a
-- migrated cluster it returns immediately. For genuinely
-- online schema changes against a live table, run the change
-- via a separate `psql` script (see runbook).
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_transactions_created_id
    ON transactions (created_at DESC, id DESC);

DROP INDEX IF EXISTS idx_transactions_created_at;
DROP INDEX IF EXISTS idx_transactions_created_at_brin;
