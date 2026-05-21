-- 0005_apply_transactions_batch.sql
--
-- One-round-trip-per-batch consumer write path.
--
-- Replaces the ~75-100 client-issued UPDATEs the Rust consumer
-- used to drive statement-by-statement (one debit + 0-2
-- credits/refunds + one status update per message) with a single
-- function call. The body is a tight FOR loop over the input
-- arrays; each iteration runs the same per-row sequence the Rust
-- consumer used to issue. Plan caches are warm; PG processes the
-- trivial UPDATEs with no network in the way.
--
-- Idempotency / dedupe: identical INSERT ... ON CONFLICT DO
-- NOTHING semantics as the previous `bulk_claim_slots` for
-- `(reference_id, from_account)`. Lost claims return
-- outcome='skipped' and skip debit/credit.
--
-- Money safety: each iteration's debit checks the running balance,
-- preserving the per-row partial-success behaviour the Rust loop
-- had (two debits from the same `from_account` in one batch see
-- each other's effects). Same-shard credit miss triggers an
-- atomic in-loop refund. Cross-shard rows queue a
-- `cross_shard_outbox` row inside the same transaction as the
-- debit, preserving the durable-outbox pattern.
--
-- No EXCEPTION block: a poison row (CHECK violation, NUMERIC
-- overflow) aborts the function and rolls back the caller's
-- transaction -- the existing per-message poison-fallback path in
-- Rust (`process_messages_individually`) then isolates the bad row.

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
