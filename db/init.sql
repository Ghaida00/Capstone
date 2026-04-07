-- ============================================================
-- GN Backend Database Schema
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
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'reversed')),
    reference_id    VARCHAR(100) UNIQUE,
    description     TEXT,
    metadata        JSONB DEFAULT '{}',
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

-- Status-based queries (for queue processing)
CREATE INDEX idx_transactions_status ON transactions (status) WHERE status IN ('pending', 'processing');

-- Reference ID lookups (idempotency)
CREATE INDEX idx_transactions_reference_id ON transactions (reference_id) WHERE reference_id IS NOT NULL;

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
CREATE TABLE idempotency_keys (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key     VARCHAR(120) NOT NULL UNIQUE,
    request_hash        VARCHAR(128) NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'expired')),
    response_payload    JSONB,
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_idempotency_keys_status ON idempotency_keys (status);
CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);

CREATE TRIGGER trigger_update_idempotency_keys_updated_at
    BEFORE UPDATE ON idempotency_keys
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- Replication user for read replica
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replication_user') THEN
        CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD 'repl_secure_pass';
    END IF;
END $$;
