-- =====================================================================
-- NairaFlow Digital Bank : relational schema (PostgreSQL 17)
-- Portfolio 1 : Database Design and Modelling
--
-- Design principles
--   * 3NF / BCNF for all operational tables (see normalisation.md)
--   * Double-entry ledger: every money movement is a txn header plus
--     balanced debit/credit ledger_entry rows (sum D = sum C per txn)
--   * Money is NUMERIC(18,2), never FLOAT
--   * Business rules enforced in the database, not only the app:
--     CHECK, UNIQUE, FK, deferred constraint trigger, audit trigger
-- =====================================================================

DROP SCHEMA IF EXISTS bank CASCADE;
CREATE SCHEMA bank;
SET search_path = bank, public;

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

-- ---------------------------------------------------------------------
-- Reference / lookup tables
-- ---------------------------------------------------------------------
CREATE TABLE region (
    region_code  VARCHAR(3)  PRIMARY KEY,          -- NC, NE, NW, SE, SS, SW
    region_name  VARCHAR(40) NOT NULL UNIQUE
);

CREATE TABLE state (
    state_code   CHAR(2)     PRIMARY KEY,          -- LA, AB, KN ...
    state_name   VARCHAR(40) NOT NULL UNIQUE,
    region_code  VARCHAR(3)  NOT NULL REFERENCES region(region_code)
);

-- CBN three-tier KYC: limits depend on the tier, not on the customer (3NF)
CREATE TABLE kyc_tier (
    tier_id          SMALLINT      PRIMARY KEY CHECK (tier_id BETWEEN 1 AND 3),
    tier_name        VARCHAR(20)   NOT NULL UNIQUE,
    daily_limit      NUMERIC(18,2) NOT NULL CHECK (daily_limit > 0),
    max_balance      NUMERIC(18,2)          CHECK (max_balance IS NULL OR max_balance > 0)  -- NULL = unlimited
);

CREATE TABLE bank (
    bank_code    CHAR(3)     PRIMARY KEY,          -- NIBSS institution code
    bank_name    VARCHAR(80) NOT NULL UNIQUE
);

CREATE TABLE branch (
    branch_id    SMALLINT    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    branch_code  CHAR(4)     NOT NULL UNIQUE,
    branch_name  VARCHAR(80) NOT NULL,
    state_code   CHAR(2)     NOT NULL REFERENCES state(state_code)
);

-- ---------------------------------------------------------------------
-- Core entities
-- ---------------------------------------------------------------------
CREATE TABLE customer (
    customer_id    INTEGER      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- surrogate key
    bvn            CHAR(11)     NOT NULL UNIQUE CHECK (bvn ~ '^[0-9]{11}$'),-- natural / candidate key
    first_name     VARCHAR(50)  NOT NULL,
    last_name      VARCHAR(50)  NOT NULL,
    email          VARCHAR(120) NOT NULL UNIQUE CHECK (position('@' IN email) > 1),
    phone          VARCHAR(14)  NOT NULL UNIQUE CHECK (phone ~ '^\+234[0-9]{10}$'),
    date_of_birth  DATE         NOT NULL,
    state_code     CHAR(2)      NOT NULL REFERENCES state(state_code),
    home_branch_id SMALLINT     NOT NULL REFERENCES branch(branch_id),
    kyc_tier_id    SMALLINT     NOT NULL DEFAULT 1 REFERENCES kyc_tier(tier_id),
    status         VARCHAR(10)  NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE','DORMANT','BLOCKED','CLOSED')),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT customer_adult CHECK (date_of_birth <= (created_at::date - INTERVAL '18 years'))
);

CREATE TABLE account (
    account_id      INTEGER       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_number  CHAR(10)      NOT NULL UNIQUE CHECK (account_number ~ '^[0-9]{10}$'), -- NUBAN
    customer_id     INTEGER       NOT NULL REFERENCES customer(customer_id) ON DELETE RESTRICT,
    account_type    VARCHAR(8)    NOT NULL CHECK (account_type IN ('SAVINGS','CURRENT','WALLET')),
    currency        CHAR(3)       NOT NULL DEFAULT 'NGN' CHECK (currency IN ('NGN','USD')),
    balance         NUMERIC(18,2) NOT NULL DEFAULT 0,
    overdraft_limit NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (overdraft_limit >= 0),
    status          VARCHAR(8)    NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
    version         INTEGER       NOT NULL DEFAULT 0,       -- optimistic-locking counter (Portfolio 3)
    opened_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    CONSTRAINT balance_within_overdraft CHECK (balance >= -overdraft_limit),
    CONSTRAINT closed_has_date CHECK ((status = 'CLOSED') = (closed_at IS NOT NULL)),
    CONSTRAINT only_current_overdraws CHECK (overdraft_limit = 0 OR account_type = 'CURRENT')
);

-- One row per money movement (the business event)
CREATE TABLE txn (
    txn_id           BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference        UUID          NOT NULL UNIQUE DEFAULT gen_random_uuid(),  -- idempotency key
    txn_type         VARCHAR(10)   NOT NULL
                     CHECK (txn_type IN ('DEPOSIT','WITHDRAWAL','TRANSFER','REVERSAL','FEE')),
    channel          VARCHAR(6)    NOT NULL CHECK (channel IN ('MOBILE','WEB','USSD','POS','ATM','BRANCH')),
    amount           NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    status           VARCHAR(9)    NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING','COMPLETED','FAILED','REVERSED')),
    narration        VARCHAR(140),
    reverses_txn_id  BIGINT        UNIQUE REFERENCES txn(txn_id),  -- a txn can be reversed at most once
    failure_reason   VARCHAR(60),
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    CONSTRAINT reversal_points_back CHECK ((txn_type = 'REVERSAL') = (reverses_txn_id IS NOT NULL)),
    CONSTRAINT failed_has_reason   CHECK (status <> 'FAILED' OR failure_reason IS NOT NULL),
    CONSTRAINT completed_has_time  CHECK (status NOT IN ('COMPLETED','REVERSED') OR completed_at IS NOT NULL)
);

-- Double-entry legs. Deposits credit the customer and debit the bank's
-- cash/settlement GL account, so every txn balances.
CREATE TABLE ledger_entry (
    entry_id       BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_id         BIGINT        NOT NULL REFERENCES txn(txn_id),
    account_id     INTEGER       NOT NULL REFERENCES account(account_id),
    direction      CHAR(1)       NOT NULL CHECK (direction IN ('D','C')),
    amount         NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    balance_after  NUMERIC(18,2) NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT one_leg_per_side UNIQUE (txn_id, account_id, direction)
);

CREATE TABLE beneficiary (
    customer_id     INTEGER     NOT NULL REFERENCES customer(customer_id) ON DELETE CASCADE,
    bank_code       CHAR(3)     NOT NULL REFERENCES bank(bank_code),
    account_number  CHAR(10)    NOT NULL CHECK (account_number ~ '^[0-9]{10}$'),
    nickname        VARCHAR(40),
    added_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, bank_code, account_number)        -- composite key
);

CREATE TABLE fraud_alert (
    alert_id     BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_id       BIGINT       NOT NULL REFERENCES txn(txn_id),
    rule_code    VARCHAR(20)  NOT NULL,
    risk_score   NUMERIC(5,2) NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    status       VARCHAR(10)  NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CLEARED','CONFIRMED')),
    raised_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE audit_log (
    audit_id     BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name   TEXT        NOT NULL,
    row_pk       TEXT        NOT NULL,
    action       CHAR(1)     NOT NULL CHECK (action IN ('I','U','D')),
    old_row      JSONB,
    new_row      JSONB,
    changed_by   TEXT        NOT NULL DEFAULT current_user,
    changed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Integrity triggers
-- ---------------------------------------------------------------------

-- (1) Double-entry rule: sum(debits) = sum(credits) for every txn.
--     DEFERRABLE INITIALLY DEFERRED so it is checked at COMMIT, after
--     both legs exist (a row-level CHECK cannot see other rows).
CREATE FUNCTION check_txn_balanced() RETURNS trigger LANGUAGE plpgsql SET search_path = bank, public AS $$
DECLARE d NUMERIC; c NUMERIC;
BEGIN
    SELECT COALESCE(SUM(amount) FILTER (WHERE direction='D'),0),
           COALESCE(SUM(amount) FILTER (WHERE direction='C'),0)
      INTO d, c
      FROM ledger_entry WHERE txn_id = NEW.txn_id;
    IF d <> c THEN
        RAISE EXCEPTION 'Unbalanced txn %: debits % <> credits %', NEW.txn_id, d, c
              USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER ledger_balanced
    AFTER INSERT OR UPDATE ON ledger_entry
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_txn_balanced();

-- (2) Ledger is append-only: corrections are made with REVERSAL txns.
CREATE FUNCTION forbid_ledger_change() RETURNS trigger LANGUAGE plpgsql SET search_path = bank, public AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entry is append-only; post a REVERSAL instead';
END $$;

CREATE TRIGGER ledger_append_only
    BEFORE UPDATE OR DELETE ON ledger_entry
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();

-- (3) Audit every change to account rows (status, balance, limits).
CREATE FUNCTION audit_account() RETURNS trigger LANGUAGE plpgsql SET search_path = bank, public AS $$
BEGIN
    INSERT INTO audit_log(table_name, row_pk, action, old_row, new_row)
    VALUES (TG_TABLE_NAME,
            COALESCE(NEW.account_id, OLD.account_id)::text,
            left(TG_OP,1),
            CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
            CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END);
    RETURN COALESCE(NEW, OLD);
END $$;

-- Created disabled during bulk load (see load_data.py), enabled afterwards.
CREATE TRIGGER account_audit
    AFTER INSERT OR UPDATE OR DELETE ON account
    FOR EACH ROW EXECUTE FUNCTION audit_account();

-- ---------------------------------------------------------------------
-- Stored procedure: the one safe way to move money (used in Portfolio 3)
-- ---------------------------------------------------------------------
CREATE PROCEDURE transfer_funds(
    p_from      CHAR(10),
    p_to        CHAR(10),
    p_amount    NUMERIC,
    p_channel   VARCHAR DEFAULT 'MOBILE',
    p_reference UUID    DEFAULT gen_random_uuid(),
    p_narration VARCHAR DEFAULT NULL)
LANGUAGE plpgsql SET search_path = bank, public AS $$
DECLARE
    v_from account%ROWTYPE;
    v_to   account%ROWTYPE;
    v_txn  BIGINT;
BEGIN
    IF p_from = p_to THEN RAISE EXCEPTION 'Cannot transfer to the same account'; END IF;

    -- Idempotency: replaying the same reference is a no-op
    IF EXISTS (SELECT 1 FROM txn WHERE reference = p_reference) THEN
        RAISE NOTICE 'Duplicate reference %, ignored', p_reference;
        RETURN;
    END IF;

    -- Lock both rows in a fixed order (by account_number) to prevent deadlocks
    PERFORM 1 FROM account
     WHERE account_number IN (p_from, p_to)
     ORDER BY account_number
     FOR UPDATE;

    SELECT * INTO v_from FROM account WHERE account_number = p_from;
    SELECT * INTO v_to   FROM account WHERE account_number = p_to;

    IF v_from.account_id IS NULL OR v_to.account_id IS NULL THEN
        RAISE EXCEPTION 'Unknown account';
    END IF;
    IF v_from.status <> 'ACTIVE' OR v_to.status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Account not active';
    END IF;
    IF v_from.balance - p_amount < -v_from.overdraft_limit THEN
        RAISE EXCEPTION 'Insufficient funds in % (balance %, requested %)',
              p_from, v_from.balance, p_amount USING ERRCODE = 'P0001';
    END IF;

    INSERT INTO txn(txn_type, channel, amount, status, narration, reference, completed_at)
    VALUES ('TRANSFER', p_channel, p_amount, 'COMPLETED', p_narration, p_reference, now())
    RETURNING txn_id INTO v_txn;

    UPDATE account SET balance = balance - p_amount, version = version + 1
     WHERE account_id = v_from.account_id;
    UPDATE account SET balance = balance + p_amount, version = version + 1
     WHERE account_id = v_to.account_id;

    INSERT INTO ledger_entry(txn_id, account_id, direction, amount, balance_after)
    VALUES (v_txn, v_from.account_id, 'D', p_amount, v_from.balance - p_amount),
           (v_txn, v_to.account_id,   'C', p_amount, v_to.balance   + p_amount);
END $$;

-- ---------------------------------------------------------------------
-- Convenience view
-- ---------------------------------------------------------------------
CREATE VIEW v_account_summary AS
SELECT a.account_number, c.first_name || ' ' || c.last_name AS holder,
       a.account_type, a.balance, a.status, k.tier_name, s.state_name, r.region_name
  FROM account a
  JOIN customer c ON c.customer_id = a.customer_id
  JOIN kyc_tier k ON k.tier_id     = c.kyc_tier_id
  JOIN state    s ON s.state_code  = c.state_code
  JOIN region   r ON r.region_code = s.region_code;
