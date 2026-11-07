-- =====================================================================
-- Portfolio 2: business queries used for the optimisation study.
-- Each block starts with "-- name: <id>" and is executed by run_benchmarks.py.
-- %(param)s placeholders are bound by the runner (values chosen from data).
-- =====================================================================

-- name: q1_statement
-- Mobile app "last 50 transactions" screen for one account in a date window.
SELECT l.created_at, t.txn_type, t.channel, l.direction, l.amount, l.balance_after, t.narration
  FROM bank.ledger_entry l
  JOIN bank.txn t ON t.txn_id = l.txn_id
 WHERE l.account_id = %(acct)s
   AND l.created_at >= '2026-04-01' AND l.created_at < '2026-10-01'
 ORDER BY l.created_at DESC
 LIMIT 50;

-- name: q2_login_lookup
-- Login by e-mail, case-insensitive (users type "Ada.Okafor@..." or "ada.okafor@...").
SELECT customer_id, first_name, last_name, kyc_tier_id, status
  FROM bank.customer
 WHERE lower(email) = lower(%(email)s);

-- name: q3_failed_investigation
-- Ops dashboard: recent failed transfers for insufficient funds, newest first.
SELECT txn_id, amount, channel, created_at
  FROM bank.txn
 WHERE status = 'FAILED' AND failure_reason = 'INSUFFICIENT_FUNDS'
   AND txn_type = 'TRANSFER'
   AND created_at >= '2026-10-01'
 ORDER BY created_at DESC
 LIMIT 100;

-- name: q4_daily_channel_report
-- Finance report: completed volume per day and channel for September 2026.
SELECT date_trunc('day', created_at)::date AS day, channel,
       count(*) AS n, sum(amount) AS value
  FROM bank.txn
 WHERE created_at >= '2026-09-01' AND created_at < '2026-10-01'
   AND status = 'COMPLETED'
 GROUP BY 1, 2
 ORDER BY 1, 2;

-- name: q5_top_spenders
-- Relationship managers: top 10 customers by outgoing value in Q3 2026, with region.
SELECT c.customer_id, c.first_name || ' ' || c.last_name AS name, r.region_name,
       sum(l.amount) AS outflow, count(*) AS debits
  FROM bank.ledger_entry l
  JOIN bank.account  a ON a.account_id  = l.account_id
  JOIN bank.customer c ON c.customer_id = a.customer_id
  JOIN bank.state    s ON s.state_code  = c.state_code
  JOIN bank.region   r ON r.region_code = s.region_code
 WHERE l.direction = 'D'
   AND l.created_at >= '2026-07-01' AND l.created_at < '2026-10-01'
   AND c.customer_id <> 1
 GROUP BY c.customer_id, name, r.region_name
 ORDER BY outflow DESC
 LIMIT 10;

-- name: q5b_top_spenders_rewritten
-- Same report, rewritten: aggregate the ledger by account first (narrow rows, integer keys),
-- roll up to customer, keep the top 10, and only THEN join names/regions for 10 rows.
WITH per_account AS (
    SELECT l.account_id, sum(l.amount) AS outflow, count(*) AS debits
      FROM bank.ledger_entry l
     WHERE l.direction = 'D'
       AND l.created_at >= '2026-07-01' AND l.created_at < '2026-10-01'
     GROUP BY l.account_id
), top10 AS (
    SELECT a.customer_id, sum(p.outflow) AS outflow, sum(p.debits) AS debits
      FROM per_account p JOIN bank.account a ON a.account_id = p.account_id
     WHERE a.customer_id <> 1
     GROUP BY a.customer_id
     ORDER BY outflow DESC
     LIMIT 10
)
SELECT c.customer_id, c.first_name || ' ' || c.last_name AS name, r.region_name, t.outflow, t.debits
  FROM top10 t
  JOIN bank.customer c ON c.customer_id = t.customer_id
  JOIN bank.state    s ON s.state_code  = c.state_code
  JOIN bank.region   r ON r.region_code = s.region_code
 ORDER BY t.outflow DESC;

-- name: q6_dormant_not_in
-- Compliance: ACTIVE accounts with no ledger activity since 2026-08-01 (NOT IN form).
SELECT count(*)
  FROM bank.account a
 WHERE a.status = 'ACTIVE'
   AND a.account_id NOT IN (SELECT l.account_id FROM bank.ledger_entry l
                             WHERE l.created_at >= '2026-08-01');

-- name: q6_dormant_not_exists
-- Same question rewritten as NOT EXISTS (anti-join), semantically equal here.
SELECT count(*)
  FROM bank.account a
 WHERE a.status = 'ACTIVE'
   AND NOT EXISTS (SELECT 1 FROM bank.ledger_entry l
                    WHERE l.account_id = a.account_id
                      AND l.created_at >= '2026-08-01');

-- name: q7_open_fraud_alerts
-- Fraud desk queue: open alerts with the debited account and customer.
SELECT f.alert_id, f.rule_code, f.risk_score, t.amount, t.channel, t.created_at,
       a.account_number, c.first_name || ' ' || c.last_name AS sender
  FROM bank.fraud_alert f
  JOIN bank.txn t           ON t.txn_id = f.txn_id
  JOIN bank.ledger_entry l  ON l.txn_id = t.txn_id AND l.direction = 'D'
  JOIN bank.account a       ON a.account_id = l.account_id
  JOIN bank.customer c      ON c.customer_id = a.customer_id
 WHERE f.status = 'OPEN' AND f.risk_score >= 60
 ORDER BY f.risk_score DESC, t.created_at DESC
 LIMIT 25;
