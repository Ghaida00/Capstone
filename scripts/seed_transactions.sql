-- ============================================================
-- Peakload Capstone Transaction Seeder
-- ------------------------------------------------------------
-- Complements the default users/test accounts from db/init.sql.
-- Safe to re-run: deterministic reference_id values plus
-- ON CONFLICT (reference_id, from_account) DO NOTHING prevent
-- duplicate seed rows and leave existing data untouched.
--
-- Usage:
--   psql "$DATABASE_URL" -f scripts/seed_transactions.sql
--
-- Optional total override:
--   psql "$DATABASE_URL" -c "SET app.seed_transactions_total = '250000';" \
--     -f scripts/seed_transactions.sql
-- ============================================================

DO $$
DECLARE
    batch_size      INT := 10000;
    total           INT := COALESCE(
        NULLIF(current_setting('app.seed_transactions_total', true), '')::INT,
        100000
    );
    batch_start     INT := 1;
    batch_inserted  INT := 0;
    total_inserted  INT := 0;
    existing_seeded INT := 0;
BEGIN
    IF total < 1 THEN
        RAISE EXCEPTION 'app.seed_transactions_total must be >= 1, got %', total;
    END IF;

    SELECT COUNT(*)
    INTO existing_seeded
    FROM transactions
    WHERE reference_id LIKE 'SEED_TRX_%';

    IF existing_seeded >= total THEN
        RAISE NOTICE '[seed-transactions] Skipped: % seed transactions already exist (target %).',
            existing_seeded, total;
        RETURN;
    END IF;

    RAISE NOTICE '[seed-transactions] Seeding up to % transactions in batches of %...',
        total, batch_size;

    WHILE batch_start <= total LOOP
        INSERT INTO transactions (
            from_account,
            to_account,
            amount,
            currency,
            status,
            reference_id,
            description,
            processed_at
        )
        SELECT
            'ACC_' || LPAD((((i - 1) % 1000000) + 1)::TEXT, 7, '0') AS from_account,
            'ACC_' || LPAD(((i % 1000000) + 1)::TEXT, 7, '0') AS to_account,
            ((i % 500000) + 1000)::NUMERIC(18, 2) AS amount,
            'IDR' AS currency,
            CASE WHEN i % 97 = 0 THEN 'failed' ELSE 'completed' END AS status,
            'SEED_TRX_' || LPAD(i::TEXT, 10, '0') AS reference_id,
            'Seed transaction for local scalability testing' AS description,
            NOW() - ((total - i) || ' seconds')::INTERVAL AS processed_at
        FROM generate_series(batch_start, LEAST(batch_start + batch_size - 1, total)) AS s(i)
        ON CONFLICT (reference_id, from_account) DO NOTHING;

        GET DIAGNOSTICS batch_inserted = ROW_COUNT;
        total_inserted := total_inserted + batch_inserted;
        batch_start := batch_start + batch_size;
    END LOOP;

    RAISE NOTICE '[seed-transactions] Done. Inserted %, existing seed rows before run %.',
        total_inserted, existing_seeded;
END $$;
