-- ============================================================
-- Peakload Capstone Database Schema
-- High-performance transaction processing
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- Transactions Table (Partitioned by month)
-- ============================================================
CREATE TABLE transactions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    from_account    VARCHAR(50) NOT NULL,
    to_account      VARCHAR(50) NOT NULL,
    amount          DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
    currency        VARCHAR(3) NOT NULL DEFAULT 'IDR',
    -- The consumer only ever writes 'completed' or 'failed'; the
    -- other values are reserved for future state machines. Keeping
    -- 'pending' / 'processing' in the CHECK lets a hypothetical
    -- API-layer pre-insert path slot in without a migration, and
    -- 'reversed' is reserved for the cross-shard outbox bundle's
    -- compensating-tx path.
    status          VARCHAR(20) NOT NULL
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'reversed')),
    -- Composite unique closes the cross-shard reuse hole: the
    -- same ref_id from different from_accounts no longer collides.
    -- Per-(account, ref) idempotency stays unique.
    reference_id    VARCHAR(100),
    description     TEXT,
    -- Populated by the cross-shard outbox credit-terminal path
    -- (handle_attempt_error) when retries are exhausted, so the
    -- customer-facing GET endpoint surfaces a real failure reason
    -- instead of indefinite 'processing'. NULL on every other row.
    failure_reason  TEXT,
    CONSTRAINT uq_transactions_reference UNIQUE (reference_id, from_account),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ
);

-- ============================================================
-- Indexes for high-throughput queries
-- ============================================================

-- Fast lookups by account (both sides)
CREATE INDEX idx_transactions_from_account ON transactions (from_account, created_at DESC);
CREATE INDEX idx_transactions_to_account ON transactions (to_account, created_at DESC);

-- Status-based queries (for queue processing).
-- Partial index narrowed to states the API/consumer might query
-- in-flight. The previous `('pending', 'processing')` partial was
-- dead because the consumer writes only terminal states.
CREATE INDEX idx_transactions_status ON transactions (status)
    WHERE status IN ('failed', 'reversed');

-- Fix #28: Removed redundant idx_transactions_reference_id — the UNIQUE
-- constraint on `reference_id` already creates an implicit unique index.

-- Keyset cursor index for `GET /api/v2/transactions`.
--
-- Serves the list pagination predicate
--   WHERE (created_at, id) < ($1, $2)
--   ORDER BY created_at DESC, id DESC
--   LIMIT N
-- one-to-one: column order matches both the cursor tuple and the
-- ORDER BY, so the planner reads the index forward and stops at
-- the LIMIT without a sort or a bitmap heap scan. `id DESC` is
-- the tie-breaker for rows sharing `created_at`. This is the only
-- time-ordered index on the table — keeping it the only choice
-- prevents the planner from picking a bitmap-heap-scan path on
-- small `LIMIT` reads.
CREATE INDEX idx_transactions_created_id ON transactions (created_at DESC, id DESC);

-- ============================================================
-- Updated_at trigger
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- Users Table
-- ============================================================
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_number  VARCHAR(50) NOT NULL UNIQUE,
    full_name       VARCHAR(150) NOT NULL,
    email           VARCHAR(150) UNIQUE,
    balance         DECIMAL(18, 2) NOT NULL DEFAULT 0.00 CHECK (balance >= 0),
    status          VARCHAR(20) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive', 'blocked')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_account_number ON users (account_number);
CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_users_status ON users (status);

CREATE TRIGGER trigger_update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- Idempotency Keys Table
-- ============================================================
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID             PRIMARY KEY DEFAULT uuid_generate_v4(),

    idempotency_key     TEXT UNIQUE NOT NULL,
    request_hash        TEXT NOT NULL,

    -- Caller always sets status explicitly ('processing' on
    -- reserve). Default removed to surface buggy callers.
    status VARCHAR(20)  NOT NULL,

    response_payload    JSONB,

    -- Outbox payload: the wire-form RabbitMQ message that the
    -- background outbox worker drains. Written in the same
    -- transaction as the idempotency reservation so a 202 to the
    -- client is durable before any broker round-trip.
    outbox_payload      JSONB,
    -- True once the broker confirms the publish; flipped by the
    -- outbox worker. Reservations created by the producer start
    -- false; rows touched only by maintenance / cleanup may be
    -- born true.
    published           BOOLEAN NOT NULL DEFAULT false,
    published_at        TIMESTAMPTZ,
    -- Logical lease stamp set by the publish-outbox worker when
    -- it claims a row in a short tx, so it can release the PG
    -- row lock before any broker round-trip. NULL means
    -- "available immediately"; a non-NULL value gates re-claim
    -- until the worker's lease window expires (`claimed_at <
    -- NOW() - lease`).
    claimed_at          TIMESTAMPTZ,

    expires_at          TIMESTAMPTZ NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_idempotency_keys_status ON idempotency_keys (status);
CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);
-- Partial index on the outbox worker's scan predicate.
CREATE INDEX idx_idempotency_keys_unpublished
    ON idempotency_keys (created_at) WHERE NOT published;


-- ============================================================
-- Cross-shard credit outbox (lives on the SENDER shard)
-- One row per cross-shard credit owed by this shard. The
-- receiver-side worker drains these into actual balance updates
-- + receiver-side `transactions` audit rows.
-- ============================================================
CREATE TABLE cross_shard_outbox (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    from_account    VARCHAR(50) NOT NULL,
    to_account      VARCHAR(50) NOT NULL,
    to_shard        INT NOT NULL,
    amount          DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
    currency        VARCHAR(3) NOT NULL,
    reference_id    VARCHAR(100) NOT NULL,
    description     TEXT,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'completed', 'failed')),
    -- When a cross-shard credit lands on a missing/inactive
    -- recipient, the receiver-side audit row is durably 'failed'
    -- but the sender's debit must be rolled back. The processor
    -- flips this flag and skips the credit step on subsequent
    -- attempts, going straight to a sender-side compensating
    -- refund (UPDATE balance + transactions row -> 'reversed').
    refund_required BOOLEAN NOT NULL DEFAULT FALSE,
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    -- Cooperative lease for HA. While lease_until is in the future
    -- another processor instance skips the row; expired lease lets
    -- a sibling re-claim a crashed holder's work.
    lease_until     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_cross_shard_outbox_pending ON cross_shard_outbox (created_at)
    WHERE status = 'pending';

-- Drain query filters on pending+lease_until; this index keeps
-- the planner off a seqscan when many rows are mid-lease.
CREATE INDEX idx_cross_shard_outbox_lease ON cross_shard_outbox (lease_until)
    WHERE status = 'pending';

CREATE TRIGGER trigger_update_cross_shard_outbox_updated_at
    BEFORE UPDATE ON cross_shard_outbox
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- Cross-shard credit dedupe (lives on the RECEIVER shard)
-- One row per (sender_shard, outbox_id) successfully applied.
-- The PRIMARY KEY makes the credit-application idempotent.
-- ============================================================
CREATE TABLE cross_shard_outbox_applied (
    sender_shard    INT NOT NULL,
    outbox_id       UUID NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sender_shard, outbox_id)
);

CREATE TRIGGER trigger_update_idempotency_keys_updated_at
    BEFORE UPDATE ON idempotency_keys
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- Fix #21: Cleanup function for expired idempotency keys.
-- Call periodically (e.g. via pg_cron or application-level task).
-- Example with pg_cron:
--   SELECT cron.schedule('cleanup-idempotency', '0 * * * *',
--     $$SELECT cleanup_expired_idempotency_keys()$$);
-- ============================================================
-- Sweeps any row whose 24h reservation has elapsed plus a 1h
-- grace window, regardless of status. Unpublished rows are NEVER
-- swept early: the publish-outbox worker is responsible for
-- draining them, and a sweep of an unpublished row would lose
-- the queue message the producer already returned 202 for.
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


-- ============================================================
-- Seed Data: 1,000,000 Test Accounts
-- ============================================================
-- Auto-seeded so no one needs to run extra scripts.
-- Account format: ACC_0000001 through ACC_1000000
-- All accounts start with a large balance and 'active' status.
--
-- Uses batch inserts (50k per batch) for fast bulk loading.
-- ON CONFLICT ensures idempotency — safe to re-run.
-- ============================================================

DO $$
DECLARE
    batch_size  INT := 50000;
    total       INT := 1000000;
    batch_start INT := 1;
    existing    INT;
BEGIN
    -- Skip the entire seed if it has already run (e.g. container
    -- restart on a persisted volume). Saves ~30s of startup.
    SELECT COUNT(*) INTO existing FROM users WHERE account_number = 'ACC_0000001';
    IF existing > 0 THEN
        RAISE NOTICE '[seed] Skipped (users already seeded)';
        RETURN;
    END IF;

    RAISE NOTICE '[seed] Seeding % test accounts...', total;

    WHILE batch_start <= total LOOP
        INSERT INTO users (account_number, full_name, email, balance, status)
        SELECT
            'ACC_' || LPAD(i::text, 7, '0'),
            'Test User ' || i,
            'user_' || i || '@loadtest.local',
            999999999.00,
            'active'
        FROM generate_series(batch_start, LEAST(batch_start + batch_size - 1, total)) AS s(i)
        ON CONFLICT (account_number) DO NOTHING;

        batch_start := batch_start + batch_size;
    END LOOP;

    RAISE NOTICE '[seed] Done. % accounts seeded.', total;
END $$;


-- ============================================================
-- Replication: managed by Patroni via the `replicator` role
-- declared in db/patroni/templates/patroni.yml.tmpl. This
-- schema file is therefore independent of the HA orchestrator
-- — db/bootstrap/bootstrap-schema.sh applies it through
-- pg-haproxy once a primary exists, regardless of who is
-- running the HA layer. See db/patroni/README.md.
-- ============================================================


-- ============================================================
-- apply_transactions_batch: one-round-trip-per-batch consumer
-- write path. Kept identical to db/migrations/0005 — that file
-- carries the full rationale. Replicated here so a fresh DB
-- (and the integration tests, which load this file) have the
-- function without depending on the migrator.
-- ============================================================
CREATE OR REPLACE FUNCTION apply_transactions_batch(
    p_ids                uuid[],
    p_outbox_ids         uuid[],
    p_from_accounts      text[],
    p_to_accounts        text[],
    p_amounts            numeric[],
    p_currencies         text[],
    p_reference_ids      text[],
    p_descriptions       text[],
    p_receiver_shards    int[],
    p_sender_shard       int
) RETURNS TABLE(idx int, outcome text, assigned_id uuid)
LANGUAGE plpgsql AS $$
DECLARE
    n int := array_length(p_ids, 1);
    i int;
    v_recv_shard int;
BEGIN
    IF n IS NULL THEN
        RETURN;
    END IF;

    FOR i IN 1..n LOOP
        v_recv_shard := p_receiver_shards[i];

        -- 1) Claim slot. ON CONFLICT DO NOTHING enforces the
        -- (reference_id, from_account) dedupe across and within
        -- batches.
        INSERT INTO transactions
            (id, from_account, to_account, amount, currency, status,
             reference_id, description, processed_at)
        VALUES
            (p_ids[i], p_from_accounts[i], p_to_accounts[i], p_amounts[i],
             p_currencies[i], 'pending', p_reference_ids[i],
             p_descriptions[i], NOW())
        ON CONFLICT (reference_id, from_account) DO NOTHING;

        IF NOT FOUND THEN
            idx := i; outcome := 'skipped'; assigned_id := NULL;
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- 2) Debit sender atomically against the running balance.
        UPDATE users
        SET balance = balance - p_amounts[i]
        WHERE account_number = p_from_accounts[i]
          AND balance >= p_amounts[i]
          AND status = 'active';

        IF NOT FOUND THEN
            UPDATE transactions
            SET status = 'failed', processed_at = NOW(), updated_at = NOW()
            WHERE id = p_ids[i];
            idx := i; outcome := 'failed'; assigned_id := p_ids[i];
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- 3a) Same-shard credit path.
        IF v_recv_shard = p_sender_shard THEN
            UPDATE users
            SET balance = balance + p_amounts[i]
            WHERE account_number = p_to_accounts[i]
              AND status = 'active';

            IF NOT FOUND THEN
                -- Recipient missing/inactive: refund sender in-tx.
                UPDATE users
                SET balance = balance + p_amounts[i]
                WHERE account_number = p_from_accounts[i];

                UPDATE transactions
                SET status = 'failed', processed_at = NOW(), updated_at = NOW()
                WHERE id = p_ids[i];

                idx := i; outcome := 'failed'; assigned_id := p_ids[i];
                RETURN NEXT;
                CONTINUE;
            END IF;

            UPDATE transactions
            SET status = 'completed', processed_at = NOW(), updated_at = NOW()
            WHERE id = p_ids[i];

            idx := i; outcome := 'completed'; assigned_id := p_ids[i];
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- 3b) Cross-shard: queue outbox row in the same tx as the
        -- debit; sender row stays 'processing' until the cross-shard
        -- processor applies the credit and flips it.
        INSERT INTO cross_shard_outbox
            (id, from_account, to_account, to_shard, amount, currency,
             reference_id, description, status)
        VALUES
            (p_outbox_ids[i], p_from_accounts[i], p_to_accounts[i],
             v_recv_shard, p_amounts[i], p_currencies[i],
             p_reference_ids[i], p_descriptions[i], 'pending');

        UPDATE transactions
        SET status = 'processing', processed_at = NOW(), updated_at = NOW()
        WHERE id = p_ids[i];

        idx := i; outcome := 'processing'; assigned_id := p_ids[i];
        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$$;
