-- =====================================================================
-- Portfolio 2: indexes chosen from the baseline execution plans.
-- Each index targets a specific query; see README.md for the reasoning.
-- =====================================================================
SET search_path = bank, public;

-- Q1 statement, Q6 anti-join, Q5 per-account joins.
-- Composite (account_id, created_at): equality column first, range/sort column second,
-- so the index returns rows already ordered by time. INCLUDE makes it covering.
CREATE INDEX IF NOT EXISTS ix_ledger_account_time
    ON ledger_entry (account_id, created_at DESC)
    INCLUDE (direction, amount, balance_after, txn_id);

-- Q2 login: expression index; a plain index on email cannot serve lower(email).
CREATE INDEX IF NOT EXISTS ix_customer_lower_email
    ON customer (lower(email));

-- Q3 failed-transfer queue: partial index. FAILED rows are a small share of txn,
-- so this index is tiny and never touched by the hot COMPLETED inserts.
CREATE INDEX IF NOT EXISTS ix_txn_failed_recent
    ON txn (created_at DESC)
    WHERE status = 'FAILED';

-- Q4 daily report: BRIN on an append-only, time-ordered column.
-- Stores min/max per block range, so it is a tiny fraction of a B-tree's size.
CREATE INDEX IF NOT EXISTS brin_txn_created
    ON txn USING brin (created_at) WITH (pages_per_range = 32);

-- Q5 quarter analytics over ledger: BRIN on ledger time (also append-only).
CREATE INDEX IF NOT EXISTS brin_ledger_created
    ON ledger_entry USING brin (created_at) WITH (pages_per_range = 32);

-- Q7 fraud queue: partial index on the open, high-risk alerts the desk actually reads.
CREATE INDEX IF NOT EXISTS ix_fraud_open_risk
    ON fraud_alert (risk_score DESC)
    WHERE status = 'OPEN';

-- Foreign key support: PostgreSQL does not index FK columns automatically.
CREATE INDEX IF NOT EXISTS ix_account_customer ON account (customer_id);
CREATE INDEX IF NOT EXISTS ix_fraud_txn        ON fraud_alert (txn_id);

ANALYZE;
