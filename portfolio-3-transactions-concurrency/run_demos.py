"""
Portfolio 3: transactions and concurrency experiments on NairaFlow.

    python portfolio-3-transactions-concurrency/run_demos.py

Creates a fresh lab database (nairaflow_p3) from the Portfolio 1 schema, then runs
eleven scenarios with two or more real concurrent sessions. Each scenario prints a
timestamped transcript ([S1]/[S2] = separate connections) and an outcome line.
The full transcript is saved to results/transcript.txt and outcomes to results/outcomes.md.
"""
import sys, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
import random

import psycopg
from psycopg import errors

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from db import PG_DSN  # noqa: E402

HERE = Path(__file__).parent
ROOT = HERE.parent
OUT = HERE / "results"
OUT.mkdir(exist_ok=True)
LAB_DSN = PG_DSN.replace("dbname=nairaflow", "dbname=nairaflow_p3")

LOG, OUTCOMES = [], []
T0 = time.perf_counter()
_lock = threading.Lock()
_nuban = iter(range(1000000001, 1999999999))


def log(tag, msg):
    line = f"{time.perf_counter() - T0:8.3f}s  [{tag}] {msg}"
    with _lock:
        print(line)
        LOG.append(line)


def header(title):
    for l in ("", "=" * 78, title, "=" * 78):
        print(l)
        LOG.append(l)


def outcome(scenario, result, verdict):
    OUTCOMES.append((scenario, result, verdict))
    log("RESULT", f"{result}  =>  {verdict}")


def conn(tag, isolation=None):
    c = psycopg.connect(LAB_DSN, autocommit=False)
    if isolation:
        c.isolation_level = isolation      # must be set while idle (before any statement)
    c.execute("SET search_path = bank, public")
    c.execute("SET lock_timeout = '10s'")
    c.add_notice_handler(lambda d: log(tag, f"NOTICE: {d.message_primary}"))
    c.commit()
    return c


def new_account(customer, amount, acc_type="SAVINGS", overdraft=0):
    n = str(next(_nuban))
    with psycopg.connect(LAB_DSN, autocommit=True) as c:
        c.execute("SET search_path = bank, public")
        c.execute("SELECT lab_open(%s,%s,%s,%s,%s)", (customer, n, acc_type, amount, overdraft))
    return n


def balance(nuban):
    with psycopg.connect(LAB_DSN, autocommit=True) as c:
        return c.execute("SELECT balance FROM bank.account WHERE account_number=%s", (nuban,)).fetchone()[0]


# ---------------------------------------------------------------------------
def setup():
    header("SETUP: create lab database nairaflow_p3 from Portfolio 1 schema")
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute("DROP DATABASE IF EXISTS nairaflow_p3 WITH (FORCE)")
        c.execute("CREATE DATABASE nairaflow_p3")
    with psycopg.connect(LAB_DSN, autocommit=True) as c:
        c.execute((ROOT / "portfolio-1-database-design" / "schema.sql").read_text(encoding="utf-8"))
        c.execute((HERE / "lab_seed.sql").read_text(encoding="utf-8"))
    log("SETUP", "schema + fixture loaded (customers: 1 bank GL, Ada=2, Musa=3, Ngozi=4)")


def s1_commit():
    header("S1  COMMIT: a transfer is atomic and durable (ledger + both balances)")
    a, b = new_account(2, 50_000), new_account(3, 10_000)
    c = conn("S1")
    log("S1", f"BEGIN; CALL transfer_funds('{a}','{b}', 15000)")
    c.execute("CALL transfer_funds(%s,%s,15000,'MOBILE')", (a, b))
    log("S1", "COMMIT")
    c.commit()
    rows = c.execute("""SELECT a.account_number, l.direction, l.amount, l.balance_after
                          FROM ledger_entry l JOIN account a USING (account_id)
                         WHERE l.txn_id = (SELECT max(txn_id) FROM txn WHERE txn_type='TRANSFER')
                         ORDER BY l.direction DESC""").fetchall()
    for r in rows:
        log("S1", f"ledger leg: acct={r[0]} {r[1]} {r[2]} balance_after={r[3]}")
    c.close()
    ba, bb = balance(a), balance(b)
    outcome("S1 commit", f"sender {ba}, receiver {bb}; 2 ledger legs",
            "PASS" if (ba, bb) == (Decimal("35000.00"), Decimal("25000.00")) else "FAIL")


def s2_rollback():
    header("S2  ROLLBACK: insufficient funds aborts the whole transfer; explicit ROLLBACK undoes work")
    a, b = new_account(2, 5_000), new_account(3, 0)
    c = conn("S1")
    try:
        log("S1", f"CALL transfer_funds('{a}','{b}', 9000)  -- balance is only 5000")
        c.execute("CALL transfer_funds(%s,%s,9000)", (a, b))
        c.commit()
    except errors.RaiseException as e:
        log("S1", f"ERROR: {e.diag.message_primary}")
        c.rollback()
        log("S1", "ROLLBACK")
    log("S1", "BEGIN; UPDATE account SET status='FROZEN' (operator mistake) ...")
    c.execute("UPDATE account SET status='FROZEN' WHERE account_number=%s", (a,))
    log("S1", "inside txn status = " + c.execute("SELECT status FROM account WHERE account_number=%s", (a,)).fetchone()[0])
    c.rollback()
    log("S1", "ROLLBACK")
    st = c.execute("SELECT status FROM account WHERE account_number=%s", (a,)).fetchone()[0]
    n = c.execute("SELECT count(*) FROM txn WHERE amount=9000").fetchone()[0]
    c.close()
    outcome("S2 rollback", f"balance {balance(a)}, status {st}, txns for 9000 = {n}",
            "PASS" if balance(a) == 5000 and st == "ACTIVE" and n == 0 else "FAIL")


def s3_deferred_constraint():
    header("S3  ATOMICITY via DEFERRED constraint: a one-legged (unbalanced) posting is rejected at COMMIT")
    a = new_account(2, 20_000)
    c = conn("S1")
    log("S1", "BEGIN; INSERT txn + only the DEBIT leg (bug: credit leg missing)")
    tid = c.execute("""INSERT INTO txn(txn_type,channel,amount,status,completed_at)
                       VALUES ('TRANSFER','WEB',777,'COMPLETED',now()) RETURNING txn_id""").fetchone()[0]
    c.execute("""INSERT INTO ledger_entry(txn_id,account_id,direction,amount,balance_after)
                 SELECT %s, account_id, 'D', 777, balance-777 FROM account WHERE account_number=%s""", (tid, a))
    log("S1", "INSERTs succeeded (check is deferred) ... COMMIT")
    try:
        c.commit()
        verdict = "FAIL"
    except errors.CheckViolation as e:
        log("S1", f"COMMIT failed: {e.diag.message_primary}")
        verdict = "PASS"
    n = c.execute("SELECT count(*) FROM txn WHERE txn_id=%s", (tid,)).fetchone()[0]
    c.close()
    outcome("S3 deferred constraint", f"txn rows persisted = {n}", verdict if n == 0 else "FAIL")


def s4_savepoint():
    header("S4  SAVEPOINT: salary batch where one payee is frozen; the batch continues")
    payer = new_account(1, 0, "CURRENT", 0)
    with psycopg.connect(LAB_DSN, autocommit=True) as c0:
        c0.execute("SELECT bank.lab_deposit(%s, 1000000)", (payer,))
    payees = [new_account(i, 0) for i in (2, 3, 4)]
    with psycopg.connect(LAB_DSN, autocommit=True) as c0:
        c0.execute("UPDATE bank.account SET status='FROZEN' WHERE account_number=%s", (payees[1],))
    c = conn("S1")
    paid = 0
    for p in payees:
        c.execute("SAVEPOINT pay_one")
        try:
            c.execute("CALL transfer_funds(%s,%s,250000,'WEB')", (payer, p))
            c.execute("RELEASE SAVEPOINT pay_one")
            log("S1", f"paid 250000 to {p}")
            paid += 1
        except errors.RaiseException as e:
            c.execute("ROLLBACK TO SAVEPOINT pay_one")
            log("S1", f"payee {p}: {e.diag.message_primary} -> ROLLBACK TO SAVEPOINT, continue")
    c.commit()
    log("S1", "COMMIT batch")
    c.close()
    outcome("S4 savepoint", f"{paid}/3 paid, payer balance {balance(payer)}",
            "PASS" if paid == 2 and balance(payer) == 500000 else "FAIL")


def s5_lost_update():
    header("S5  LOST UPDATE under READ COMMITTED with read-modify-write in the app")
    a = new_account(2, 10_000)
    s1, s2 = conn("S1"), conn("S2")
    b1 = s1.execute("SELECT balance FROM account WHERE account_number=%s", (a,)).fetchone()[0]
    log("S1", f"BEGIN; SELECT balance -> {b1}; wants to withdraw 7000")
    b2 = s2.execute("SELECT balance FROM account WHERE account_number=%s", (a,)).fetchone()[0]
    log("S2", f"BEGIN; SELECT balance -> {b2}; wants to withdraw 7000")
    s1.execute("UPDATE account SET balance=%s WHERE account_number=%s", (b1 - 7000, a))
    s1.commit()
    log("S1", f"UPDATE balance={b1 - 7000}; COMMIT")
    s2.execute("UPDATE account SET balance=%s WHERE account_number=%s", (b2 - 7000, a))
    s2.commit()
    log("S2", f"UPDATE balance={b2 - 7000}; COMMIT  (overwrites S1's write)")
    s1.close(); s2.close()
    with psycopg.connect(LAB_DSN, autocommit=True) as c:
        drift = c.execute("""SELECT a.balance - COALESCE(sum(CASE l.direction WHEN 'C' THEN l.amount ELSE -l.amount END),0)
                               FROM bank.account a LEFT JOIN bank.ledger_entry l USING (account_id)
                              WHERE a.account_number=%s GROUP BY a.balance""", (a,)).fetchone()[0]
    outcome("S5 lost update (anomaly shown)", f"14000 withdrawn from 10000 but final balance {balance(a)}; "
            f"reconciliation vs ledger shows drift {drift}", "ANOMALY REPRODUCED")


def s6_for_update():
    header("S6  FIX 1, pessimistic locking: SELECT ... FOR UPDATE serialises the two withdrawals")
    a = new_account(2, 10_000)
    s1, s2 = conn("S1"), conn("S2")
    got = threading.Event()
    result = {}

    def session2():
        got.wait()
        log("S2", "BEGIN; SELECT balance ... FOR UPDATE   (blocks: S1 holds the row lock)")
        t = time.perf_counter()
        b = s2.execute("SELECT balance FROM account WHERE account_number=%s FOR UPDATE", (a,)).fetchone()[0]
        log("S2", f"lock granted after {time.perf_counter() - t:.2f}s, sees balance {b}")
        if b - 7000 < 0:
            log("S2", "insufficient funds -> ROLLBACK")
            s2.rollback()
            result["s2"] = "rejected"
        else:
            s2.execute("UPDATE account SET balance=balance-7000 WHERE account_number=%s", (a,))
            s2.commit()
            result["s2"] = "withdrew"

    th = threading.Thread(target=session2)
    th.start()
    b = s1.execute("SELECT balance FROM account WHERE account_number=%s FOR UPDATE", (a,)).fetchone()[0]
    log("S1", f"BEGIN; SELECT balance FOR UPDATE -> {b} (row locked)")
    got.set()
    time.sleep(1.5)
    s1.execute("UPDATE account SET balance=balance-7000 WHERE account_number=%s", (a,))
    s1.commit()
    log("S1", "UPDATE balance-7000; COMMIT (lock released)")
    th.join()
    s1.close(); s2.close()
    outcome("S6 FOR UPDATE", f"final balance {balance(a)}, second withdrawal {result['s2']}",
            "PASS" if balance(a) == 3000 and result["s2"] == "rejected" else "FAIL")


def s7_optimistic():
    header("S7  FIX 2, optimistic locking: version column, UPDATE ... WHERE version = :read_version")
    a = new_account(2, 10_000)
    s1, s2 = conn("S1"), conn("S2")
    b1, v1 = s1.execute("SELECT balance, version FROM account WHERE account_number=%s", (a,)).fetchone()
    log("S1", f"read balance={b1}, version={v1}")
    b2, v2 = s2.execute("SELECT balance, version FROM account WHERE account_number=%s", (a,)).fetchone()
    log("S2", f"read balance={b2}, version={v2}")
    n1 = s1.execute("UPDATE account SET balance=%s, version=version+1 WHERE account_number=%s AND version=%s",
                    (b1 - 7000, a, v1)).rowcount
    s1.commit()
    log("S1", f"UPDATE ... WHERE version={v1} -> {n1} row; COMMIT")
    n2 = s2.execute("UPDATE account SET balance=%s, version=version+1 WHERE account_number=%s AND version=%s",
                    (b2 - 7000, a, v2)).rowcount
    log("S2", f"UPDATE ... WHERE version={v2} -> {n2} rows (stale read detected)")
    s2.rollback()
    b2, v2 = s2.execute("SELECT balance, version FROM account WHERE account_number=%s", (a,)).fetchone()
    log("S2", f"retry: re-read balance={b2}, version={v2} -> insufficient, abort")
    s2.rollback()
    s1.close(); s2.close()
    outcome("S7 optimistic", f"rows updated S1={n1}, S2={n2}; final balance {balance(a)}",
            "PASS" if (n1, n2) == (1, 0) and balance(a) == 3000 else "FAIL")


def s8_repeatable_read():
    header("S8  FIX 3, REPEATABLE READ: the second writer gets a serialization failure (SQLSTATE 40001)")
    a = new_account(2, 10_000)
    rr = psycopg.IsolationLevel.REPEATABLE_READ
    s1, s2 = conn("S1", rr), conn("S2", rr)
    b1 = s1.execute("SELECT balance FROM account WHERE account_number=%s", (a,)).fetchone()[0]
    log("S1", f"BEGIN ISOLATION LEVEL REPEATABLE READ; balance -> {b1}")
    b2 = s2.execute("SELECT balance FROM account WHERE account_number=%s", (a,)).fetchone()[0]
    log("S2", f"BEGIN ISOLATION LEVEL REPEATABLE READ; balance -> {b2}")
    s1.execute("UPDATE account SET balance=%s WHERE account_number=%s", (b1 - 7000, a))
    s1.commit()
    log("S1", "UPDATE; COMMIT")
    err = None
    try:
        s2.execute("UPDATE account SET balance=%s WHERE account_number=%s", (b2 - 7000, a))
        s2.commit()
    except errors.SerializationFailure as e:
        err = e.sqlstate
        log("S2", f"ERROR {e.sqlstate}: {e.diag.message_primary} -> app must retry")
        s2.rollback()
    s1.close(); s2.close()
    outcome("S8 repeatable read", f"S2 error {err}, final balance {balance(a)}",
            "PASS" if err == "40001" and balance(a) == 3000 else "FAIL")


def s9_nonrepeatable_read():
    header("S9  NON-REPEATABLE READ: READ COMMITTED vs REPEATABLE READ for an audit report")
    res = {}
    for iso in (psycopg.IsolationLevel.READ_COMMITTED, psycopg.IsolationLevel.REPEATABLE_READ):
        a = new_account(3, 40_000)
        s1, s2 = conn("S1", iso), conn("S2")
        r1 = s1.execute("SELECT balance FROM account WHERE account_number=%s", (a,)).fetchone()[0]
        log("S1", f"{iso.name}: audit read #1 balance = {r1}")
        s2.execute("SELECT lab_deposit(%s, 5000)", (a,))
        s2.commit()
        log("S2", "deposit 5000; COMMIT")
        r2 = s1.execute("SELECT balance FROM account WHERE account_number=%s", (a,)).fetchone()[0]
        log("S1", f"{iso.name}: audit read #2 balance = {r2}  ({'changed' if r1 != r2 else 'stable snapshot'})")
        s1.commit(); s1.close(); s2.close()
        res[iso.name] = f"{r1} then {r2}"
    same = lambda s: len(set(s.split(" then "))) == 1  # noqa: E731
    ok = not same(res["READ_COMMITTED"]) and same(res["REPEATABLE_READ"])
    outcome("S9 non-repeatable read", f"RC reads {res['READ_COMMITTED']}, RR reads {res['REPEATABLE_READ']}",
            "PASS (RC shows anomaly, RR prevents it)" if ok else "FAIL")


def s10_write_skew():
    header("S10 WRITE SKEW: rule 'Ngozi's combined balance across her two accounts must stay >= 0'")
    res = {}
    for iso in (psycopg.IsolationLevel.REPEATABLE_READ, psycopg.IsolationLevel.SERIALIZABLE):
        a = new_account(4, 5_000, "CURRENT", 50_000)
        b = new_account(4, 5_000, "CURRENT", 50_000)
        s1, s2 = conn("S1", iso), conn("S2", iso)
        q = "SELECT sum(balance) FROM account WHERE account_number IN (%s,%s)"
        t1 = s1.execute(q, (a, b)).fetchone()[0]
        log("S1", f"{iso.name}: combined = {t1}, withdraw 8000 from A is allowed")
        t2 = s2.execute(q, (a, b)).fetchone()[0]
        log("S2", f"{iso.name}: combined = {t2}, withdraw 8000 from B is allowed")
        s1.execute("UPDATE account SET balance=balance-8000 WHERE account_number=%s", (a,))
        log("S1", "UPDATE A balance-8000")
        s1.commit()
        log("S1", "COMMIT ok")
        try:
            s2.execute("UPDATE account SET balance=balance-8000 WHERE account_number=%s", (b,))
            log("S2", "UPDATE B balance-8000")
            s2.commit()
            log("S2", "COMMIT ok")
            res[iso.name] = "both committed"
        except errors.SerializationFailure as e:
            log("S2", f"COMMIT failed {e.sqlstate}: {e.diag.message_primary}")
            res[iso.name] = "S2 aborted (40001)"
        s1.close(); s2.close()
        total = balance(a) + balance(b)
        log("CHECK", f"{iso.name}: combined balance now {total}")
        res[iso.name] += f", combined {total}"
    ok = "both committed" in res["REPEATABLE_READ"] and "aborted" in res["SERIALIZABLE"]
    outcome("S10 write skew", f"REPEATABLE READ: {res['REPEATABLE_READ']}; SERIALIZABLE: {res['SERIALIZABLE']}",
            "PASS (SSI prevents write skew)" if ok else "FAIL")


def s11_deadlock():
    header("S11 DEADLOCK: two transfers lock accounts in opposite order; PostgreSQL detects it")
    a, b = new_account(2, 100_000), new_account(3, 100_000)
    s1, s2 = conn("S1"), conn("S2")
    barrier = threading.Barrier(2)
    victims = []

    def run(tag, c, first, second):
        try:
            c.execute("UPDATE account SET balance=balance-100 WHERE account_number=%s", (first,))
            log(tag, f"locked {first}")
            barrier.wait()
            log(tag, f"now wants {second} ...")
            c.execute("UPDATE account SET balance=balance+100 WHERE account_number=%s", (second,))
            c.commit()
            log(tag, "COMMIT")
        except errors.DeadlockDetected as e:
            log(tag, f"ERROR 40P01 deadlock detected, chosen as victim: {e.diag.message_detail.splitlines()[0]}")
            c.rollback()
            victims.append(tag)

    t1 = threading.Thread(target=run, args=("S1", s1, a, b))
    t2 = threading.Thread(target=run, args=("S2", s2, b, a))
    t1.start(); t2.start(); t1.join(); t2.join()
    s1.close(); s2.close()
    outcome("S11a deadlock (naive order)", f"victims: {victims}",
            "DEADLOCK REPRODUCED and resolved by the DBMS" if len(victims) == 1 else "FAIL")

    header("S11b FIX: transfer_funds() locks both rows ordered by account_number; 400 opposing transfers, 8 threads")
    x, y = new_account(2, 1_000_000), new_account(3, 1_000_000)
    stats = {"ok": 0, "deadlock": 0, "other": 0}

    def worker(i):
        c = psycopg.connect(LAB_DSN, autocommit=True)
        c.execute("SET search_path = bank, public")
        for k in range(50):
            src, dst = (x, y) if (i + k) % 2 else (y, x)
            try:
                c.execute("CALL transfer_funds(%s,%s,%s)", (src, dst, random.randint(1, 500)))
                key = "ok"
            except errors.DeadlockDetected:
                key = "deadlock"
            except psycopg.Error:
                key = "other"
            with _lock:
                stats[key] += 1
        c.close()

    t = time.perf_counter()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(worker, range(8)))
    el = time.perf_counter() - t
    log("STRESS", f"{stats} in {el:.2f}s ({400 / el:.0f} transfers/s)")
    total = balance(x) + balance(y)
    outcome("S11b ordered locking", f"{stats['ok']} committed, {stats['deadlock']} deadlocks, money conserved: {total == 2_000_000}",
            "PASS" if stats["deadlock"] == 0 and total == 2_000_000 else "FAIL")


def s12_idempotency_and_audit():
    header("S12 IDEMPOTENCY + AUDIT: a retried request with the same reference is applied once")
    a, b = new_account(2, 30_000), new_account(3, 0)
    ref = str(uuid.uuid4())
    c = conn("S1")
    for attempt in (1, 2):
        log("S1", f"attempt {attempt}: CALL transfer_funds(..., 12000, reference={ref[:8]}...)")
        c.execute("CALL transfer_funds(%s,%s,12000,'MOBILE',%s)", (a, b, ref))
        c.commit()
    n = c.execute("SELECT count(*) FROM txn WHERE reference=%s", (ref,)).fetchone()[0]
    aud = c.execute("""SELECT count(*), max((old_row->>'balance')||' -> '||(new_row->>'balance'))
                         FROM audit_log WHERE table_name='account' AND action='U'
                          AND row_pk=(SELECT account_id::text FROM account WHERE account_number=%s)""", (a,)).fetchone()
    log("S1", f"audit_log rows for sender: {aud[0]}, last change {aud[1]}")
    c.close()
    outcome("S12 idempotency", f"txn rows with that reference = {n}, sender balance {balance(a)}",
            "PASS" if n == 1 and balance(a) == 18000 else "FAIL")


def global_checks():
    header("FINAL: ledger integrity in the lab database")
    with psycopg.connect(LAB_DSN, autocommit=True) as c:
        c.execute("SET search_path = bank, public")
        unb = c.execute("""SELECT count(*) FROM (SELECT txn_id FROM ledger_entry GROUP BY txn_id
                            HAVING sum(amount) FILTER (WHERE direction='D') <> sum(amount) FILTER (WHERE direction='C')) x""").fetchone()[0]
        drift = c.execute("""SELECT count(*) FROM account a WHERE a.balance <>
                               COALESCE((SELECT sum(CASE direction WHEN 'C' THEN amount ELSE -amount END)
                                           FROM ledger_entry l WHERE l.account_id=a.account_id),0)""").fetchone()[0]
    log("CHECK", f"unbalanced txns: {unb}")
    log("CHECK", f"accounts whose balance drifted from the ledger: {drift} "
                 "(expected: only the accounts hit by direct UPDATEs in S5-S11a, which bypassed transfer_funds)")


if __name__ == "__main__":
    setup()
    for fn in (s1_commit, s2_rollback, s3_deferred_constraint, s4_savepoint, s5_lost_update, s6_for_update,
               s7_optimistic, s8_repeatable_read, s9_nonrepeatable_read, s10_write_skew, s11_deadlock,
               s12_idempotency_and_audit):
        fn()
    global_checks()
    (OUT / "transcript.txt").write_text("\n".join(LOG) + "\n", encoding="utf-8")
    md = ["| Scenario | Observed | Verdict |", "|---|---|---|"]
    md += [f"| {s} | {r} | {v} |" for s, r, v in OUTCOMES]
    (OUT / "outcomes.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n" + "\n".join(md))
