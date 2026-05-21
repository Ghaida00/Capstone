-- 0005_apply_transactions_batch.sql
--
-- One-round-trip-per-batch consumer write path, hybrid shape:
-- bulk where the work is set-oriented, per-row only where it must
-- be sequential.
--
--   Phase 1 — bulk claim: ONE INSERT … SELECT FROM unnest(…)
--             ON CONFLICT (reference_id, from_account) DO NOTHING
--             RETURNING id. The returned ids are the rows this
--             call won; everything else lost the claim race (a
--             prior batch's commit, or an intra-batch duplicate).
--   Phase 2 — per-row debit / credit: a FOR loop. This stays
--             sequential on purpose — each debit checks the
--             running balance, so two debits from the same
--             from_account in one batch see each other's effects
--             (partial-success semantics). Same-shard credit miss
--             refunds the sender in-tx. Cross-shard rows are
--             accumulated for the bulk insert below.
--   Phase 3 — bulk outbox insert + bulk status promote: ONE
--             INSERT for all cross_shard_outbox rows, and three
--             UPDATE … WHERE id = ANY(…) that flip the claimed
--             placeholders to their terminal status in bulk.
--
-- Statement count for an N-row batch is `1 + 2N + 4` (claim +
-- per-row debit/credit + outbox + 3 status), versus `4N` for a
-- naive all-per-row loop: the claim and the status promotion are
-- the set-oriented steps, so bulking them removes ~2N statement
-- executions.
--
-- Idempotency / dedupe: `ON CONFLICT DO NOTHING` collapses both
-- cross-batch and intra-batch `(reference_id, from_account)`
-- duplicates — the first occurrence inserts, the rest are dropped
-- and fall through to outcome 'skipped'.
--
-- Money safety: the per-row debit checks the running balance;
-- same-shard credit miss triggers an atomic in-loop refund;
-- cross-shard rows queue a `cross_shard_outbox` row in the same
-- transaction as the debit.
--
-- No EXCEPTION block: a poison row (CHECK violation, NUMERIC
-- overflow) aborts the function and rolls back the caller's
-- transaction -- the per-message poison-fallback path in Rust
-- (`process_messages_individually`) then isolates the bad row.

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
    v_claimed_ids uuid[];
    -- Terminal-status accumulators, bulk-applied after the loop.
    v_completed  uuid[] := ARRAY[]::uuid[];
    v_failed     uuid[] := ARRAY[]::uuid[];
    v_processing uuid[] := ARRAY[]::uuid[];
    -- Cross-shard outbox accumulators, bulk-inserted after the loop.
    v_ob_ids      uuid[]    := ARRAY[]::uuid[];
    v_ob_from     text[]    := ARRAY[]::text[];
    v_ob_to       text[]    := ARRAY[]::text[];
    v_ob_shard    int[]     := ARRAY[]::int[];
    v_ob_amount   numeric[] := ARRAY[]::numeric[];
    v_ob_currency text[]    := ARRAY[]::text[];
    v_ob_ref      text[]    := ARRAY[]::text[];
    v_ob_descr    text[]    := ARRAY[]::text[];
BEGIN
    IF n IS NULL THEN
        RETURN;
    END IF;

    -- ── Phase 1: bulk claim ──────────────────────────────────
    WITH ins AS (
        INSERT INTO transactions
            (id, from_account, to_account, amount, currency, status,
             reference_id, description, processed_at)
        SELECT
            t.id, t.from_acct, t.to_acct, t.amount, t.currency, 'pending',
            t.ref, t.descr, NOW()
        FROM unnest(
            p_ids, p_from_accounts, p_to_accounts, p_amounts,
            p_currencies, p_reference_ids, p_descriptions
        ) AS t(id, from_acct, to_acct, amount, currency, ref, descr)
        ON CONFLICT (reference_id, from_account) DO NOTHING
        RETURNING id
    )
    SELECT array_agg(id) INTO v_claimed_ids FROM ins;

    IF v_claimed_ids IS NULL THEN
        v_claimed_ids := ARRAY[]::uuid[];
    END IF;

    -- ── Phase 2: per-row debit / credit (sequential) ─────────
    FOR i IN 1..n LOOP
        IF NOT (p_ids[i] = ANY(v_claimed_ids)) THEN
            idx := i; outcome := 'skipped'; assigned_id := NULL;
            RETURN NEXT;
            CONTINUE;
        END IF;

        v_recv_shard := p_receiver_shards[i];

        -- Debit sender atomically against the running balance.
        UPDATE users
        SET balance = balance - p_amounts[i]
        WHERE account_number = p_from_accounts[i]
          AND balance >= p_amounts[i]
          AND status = 'active';

        IF NOT FOUND THEN
            v_failed := array_append(v_failed, p_ids[i]);
            idx := i; outcome := 'failed'; assigned_id := p_ids[i];
            RETURN NEXT;
            CONTINUE;
        END IF;

        IF v_recv_shard = p_sender_shard THEN
            -- Same-shard credit.
            UPDATE users
            SET balance = balance + p_amounts[i]
            WHERE account_number = p_to_accounts[i]
              AND status = 'active';

            IF NOT FOUND THEN
                -- Recipient missing/inactive: refund sender in-tx.
                UPDATE users
                SET balance = balance + p_amounts[i]
                WHERE account_number = p_from_accounts[i];

                v_failed := array_append(v_failed, p_ids[i]);
                idx := i; outcome := 'failed'; assigned_id := p_ids[i];
                RETURN NEXT;
                CONTINUE;
            END IF;

            v_completed := array_append(v_completed, p_ids[i]);
            idx := i; outcome := 'completed'; assigned_id := p_ids[i];
            RETURN NEXT;
        ELSE
            -- Cross-shard: accumulate the outbox row; the sender
            -- row stays 'processing' until the cross-shard
            -- processor applies the credit and flips it.
            v_ob_ids      := array_append(v_ob_ids, p_outbox_ids[i]);
            v_ob_from     := array_append(v_ob_from, p_from_accounts[i]);
            v_ob_to       := array_append(v_ob_to, p_to_accounts[i]);
            v_ob_shard    := array_append(v_ob_shard, v_recv_shard);
            v_ob_amount   := array_append(v_ob_amount, p_amounts[i]);
            v_ob_currency := array_append(v_ob_currency, p_currencies[i]);
            v_ob_ref      := array_append(v_ob_ref, p_reference_ids[i]);
            v_ob_descr    := array_append(v_ob_descr, p_descriptions[i]);

            v_processing := array_append(v_processing, p_ids[i]);
            idx := i; outcome := 'processing'; assigned_id := p_ids[i];
            RETURN NEXT;
        END IF;
    END LOOP;

    -- ── Phase 3: bulk outbox insert + bulk status promote ────
    IF array_length(v_ob_ids, 1) IS NOT NULL THEN
        INSERT INTO cross_shard_outbox
            (id, from_account, to_account, to_shard, amount, currency,
             reference_id, description, status)
        SELECT * FROM unnest(
            v_ob_ids, v_ob_from, v_ob_to, v_ob_shard, v_ob_amount,
            v_ob_currency, v_ob_ref, v_ob_descr,
            array_fill('pending'::text, ARRAY[array_length(v_ob_ids, 1)])
        );
    END IF;

    IF array_length(v_completed, 1) IS NOT NULL THEN
        UPDATE transactions
        SET status = 'completed', processed_at = NOW(), updated_at = NOW()
        WHERE id = ANY(v_completed);
    END IF;

    IF array_length(v_failed, 1) IS NOT NULL THEN
        UPDATE transactions
        SET status = 'failed', processed_at = NOW(), updated_at = NOW()
        WHERE id = ANY(v_failed);
    END IF;

    IF array_length(v_processing, 1) IS NOT NULL THEN
        UPDATE transactions
        SET status = 'processing', processed_at = NOW(), updated_at = NOW()
        WHERE id = ANY(v_processing);
    END IF;

    RETURN;
END;
$$;
