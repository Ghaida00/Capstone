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

-- Time-based queries
CREATE INDEX idx_transactions_created_at ON transactions (created_at DESC);

-- BRIN index for time-series queries (very efficient for ordered data)
CREATE INDEX idx_transactions_created_at_brin ON transactions USING BRIN (created_at);

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

    expires_at          TIMESTAMPTZ NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_idempotency_keys_status ON idempotency_keys (status);
CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);


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
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_cross_shard_outbox_pending ON cross_shard_outbox (created_at)
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
CREATE OR REPLACE FUNCTION cleanup_expired_idempotency_keys()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM idempotency_keys
    WHERE expires_at < NOW() - INTERVAL '1 hour'
      AND status IN ('completed', 'failed');
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
