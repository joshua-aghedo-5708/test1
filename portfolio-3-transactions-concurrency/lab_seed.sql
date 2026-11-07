-- Small, fully controlled fixture for the Portfolio 3 experiments.
-- Applied after ../portfolio-1-database-design/schema.sql inside database nairaflow_p3.
SET search_path = bank, public;

INSERT INTO region VALUES ('SW','South West');
INSERT INTO state  VALUES ('LA','Lagos','SW');
INSERT INTO kyc_tier VALUES (1,'TIER_1',50000,300000),(2,'TIER_2',200000,500000),(3,'TIER_3',5000000,NULL);
INSERT INTO branch(branch_code, branch_name, state_code) VALUES ('B001','Lagos Main','LA');

INSERT INTO customer(bvn, first_name, last_name, email, phone, date_of_birth, state_code, home_branch_id, kyc_tier_id, created_at)
VALUES ('00000000000','NairaFlow','Treasury','treasury@nairaflow.ng','+2340000000000','1990-01-01','LA',1,3,'2020-01-01'),
       ('22200000001','Ada','Okafor','ada@lab.ng','+2348000000001','1990-05-01','LA',1,3,'2024-01-01'),
       ('22200000002','Musa','Bello','musa@lab.ng','+2348000000002','1988-02-11','LA',1,3,'2024-01-01'),
       ('22200000003','Ngozi','Eze','ngozi@lab.ng','+2348000000003','1995-09-23','LA',1,3,'2024-01-01');

-- Bank GL (cash/settlement) account, allowed to go negative
INSERT INTO account(account_number, customer_id, account_type, overdraft_limit)
VALUES ('0000000001', 1, 'CURRENT', 9000000000000);

-- Correct way to fund an account: a DEPOSIT txn with two balanced legs.
CREATE FUNCTION lab_deposit(p_nuban CHAR(10), p_amount NUMERIC) RETURNS void LANGUAGE plpgsql SET search_path = bank, public AS $$
DECLARE v_txn BIGINT; v_acc account%ROWTYPE; v_gl account%ROWTYPE;
BEGIN
    SELECT * INTO v_acc FROM account WHERE account_number = p_nuban FOR UPDATE;
    SELECT * INTO v_gl  FROM account WHERE account_number = '0000000001' FOR UPDATE;
    INSERT INTO txn(txn_type, channel, amount, status, narration, completed_at)
    VALUES ('DEPOSIT','BRANCH',p_amount,'COMPLETED','Lab funding',now()) RETURNING txn_id INTO v_txn;
    UPDATE account SET balance = balance + p_amount, version = version + 1 WHERE account_id = v_acc.account_id;
    UPDATE account SET balance = balance - p_amount WHERE account_id = v_gl.account_id;
    INSERT INTO ledger_entry(txn_id, account_id, direction, amount, balance_after)
    VALUES (v_txn, v_acc.account_id, 'C', p_amount, v_acc.balance + p_amount),
           (v_txn, v_gl.account_id,  'D', p_amount, v_gl.balance  - p_amount);
END $$;

CREATE FUNCTION lab_open(p_customer INT, p_nuban CHAR(10), p_type VARCHAR, p_amount NUMERIC, p_overdraft NUMERIC DEFAULT 0)
RETURNS void LANGUAGE plpgsql SET search_path = bank, public AS $$
BEGIN
    INSERT INTO account(account_number, customer_id, account_type, overdraft_limit)
    VALUES (p_nuban, p_customer, p_type, p_overdraft);
    IF p_amount > 0 THEN PERFORM lab_deposit(p_nuban, p_amount); END IF;
END $$;
