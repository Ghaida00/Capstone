-- 0006_idempotency_cleanup_grace.sql
--
-- Adds a published-grace branch to cleanup_expired_idempotency_keys
-- so that rows the publisher has already drained (published = true)
-- become eligible for deletion `published_grace_secs` after
-- `published_at` instead of waiting the full 25 h that the existing
-- (expires_at + 1 h) check enforces.
--
-- Unpublished rows are still NEVER swept early — the existing
-- expires_at + 1 h grace continues to guard them, because a sweep
-- of an unpublished row would lose the queue message the producer
-- already returned 202 for.
--
-- The function signature gains a parameter with a DEFAULT of 300 s
-- (5 min), so existing callers that did not pass an argument keep
-- working. The Rust caller passes an env-driven value
-- (IDEMPOTENCY_PUBLISHED_GRACE_SECS) explicitly at startup so the
-- operator can tune without code changes.

CREATE OR REPLACE FUNCTION cleanup_expired_idempotency_keys(
    p_published_grace_secs INT DEFAULT 300
) RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM idempotency_keys
    WHERE (expires_at < NOW() - INTERVAL '1 hour')
       OR (published = TRUE
           AND published_at < NOW() - make_interval(secs => p_published_grace_secs));
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
