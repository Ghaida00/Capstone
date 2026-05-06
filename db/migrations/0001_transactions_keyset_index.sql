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
-- All operations run `CONCURRENTLY` so live writers are not
-- blocked on `AccessExclusiveLock`. `IF NOT EXISTS` /
-- `IF EXISTS` make the script idempotent — re-running on a
-- migrated cluster is a no-op.
--
-- Apply against each shard primary via the pg-haproxy frontend
-- ports (e.g. 5000 for shard 0, 5001 for shard 1). Fresh
-- clusters reach the same end state from `db/init.sql`; this
-- file is for clusters whose bootstrap canary already exists.
-- ============================================================

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_created_id
    ON transactions (created_at DESC, id DESC);

DROP INDEX CONCURRENTLY IF EXISTS idx_transactions_created_at;
DROP INDEX CONCURRENTLY IF EXISTS idx_transactions_created_at_brin;
