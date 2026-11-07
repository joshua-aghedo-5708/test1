# Portfolio 3: Transactions and Concurrency

The experiments run in a separate lab database, `nairaflow_p3`, built from the **same Portfolio 1 schema** plus a small fixture ([`lab_seed.sql`](lab_seed.sql)). Some scenarios *deliberately* corrupt balances to reproduce anomalies, and the main dataset must stay clean for Portfolios 2, 4 and 5.

Every scenario uses **two or more real, separate database connections** (labelled `[S1]`, `[S2]`) driven from Python threads, with timestamps. These are not simulated: the waits, errors and SQLSTATEs come from PostgreSQL 17.

| File | Purpose |
|---|---|
| [`run_demos.py`](run_demos.py) | All 12 scenarios |
| [`results/transcript.txt`](results/transcript.txt) | Full timestamped transcript (main evidence) |
| [`results/outcomes.md`](results/outcomes.md) | One line per scenario with verdict |

## Results

| # | Concept | What happened | Verdict |
|---|---|---|---|
| S1 | **COMMIT / durability** | `transfer_funds` moved 15,000: sender 50,000 → 35,000, receiver 10,000 → 25,000, one D and one C ledger leg | PASS |
| S2 | **ROLLBACK** | Transfer of 9,000 from a 5,000 balance raised `Insufficient funds`; nothing persisted. An operator's accidental `FROZEN` update was undone with `ROLLBACK` | PASS |
| S3 | **Atomicity via deferred constraint** | A buggy posting with only the debit leg: both INSERTs succeed, then `COMMIT` fails with `Unbalanced txn: debits 777.00 <> credits 0` and the txn row is gone | PASS |
| S4 | **SAVEPOINT** | Salary batch of 3 payees, one frozen: `ROLLBACK TO SAVEPOINT` skipped only that payee, and the other 2 were paid in the same transaction | PASS |
| S5 | **Lost update** (anomaly) | Under READ COMMITTED, two sessions read 10,000 and each wrote `balance = 3000`. 14,000 was withdrawn but the balance shows 3,000. Reconciliation against the ledger detects the **-7,000 drift** | Reproduced |
| S6 | Fix 1: **pessimistic `SELECT … FOR UPDATE`** | S2 blocked for **1.63 s** until S1 committed, then saw 3,000 and correctly rejected its withdrawal | PASS |
| S7 | Fix 2: **optimistic locking** (`version` column) | `UPDATE … WHERE version = 1`: S1 updated 1 row, S2 updated **0 rows**, so it detected the stale read, re-read and aborted | PASS |
| S8 | Fix 3: **REPEATABLE READ** | The second writer got **SQLSTATE 40001** "could not serialize access due to concurrent update", which tells the app to retry | PASS |
| S9 | **Non-repeatable read** | Audit report reading twice: READ COMMITTED saw 40,000 then 45,000. REPEATABLE READ saw 40,000 both times (stable snapshot) | PASS |
| S10 | **Write skew** | Rule: Ngozi's *combined* balance must stay ≥ 0. Each session saw 10,000 and withdrew 8,000 from a *different* account. REPEATABLE READ let both commit (combined **-6,000**, rule broken). **SERIALIZABLE (SSI)** aborted S2 with 40001 (combined 2,000) | PASS |
| S11a | **Deadlock** | Opposite lock order (A→B vs B→A): PostgreSQL detected the cycle after `deadlock_timeout` = 1 s, aborted S2 with **40P01**, and S1 committed | Reproduced |
| S11b | Fix: **ordered locking** | 400 opposing transfers on 8 threads through `transfer_funds()` (which locks both rows `ORDER BY account_number`): **0 deadlocks**, 400 committed, total money conserved | PASS |
| S12 | **Idempotency + audit** | The same transfer request sent twice with one UUID `reference` was applied once. The audit trigger logged `30000.00 -> 18000.00` | PASS |

Final integrity check of the lab database: **0 unbalanced transactions**. 9 accounts have balances that differ from the ledger, and they are exactly the accounts that S5-S11a modified with raw `UPDATE`s that bypassed `transfer_funds()`. That's why the production design allows balance changes only through the procedure, and why reconciliation exists.

## Excerpt from the transcript (S11 deadlock)

```
   9.098s  [S1] locked 1000000020
   9.098s  [S2] locked 1000000021
   9.098s  [S2] now wants 1000000020 ...
   9.098s  [S1] now wants 1000000021 ...
  10.119s  [S2] ERROR 40P01 deadlock detected, chosen as victim: Process 672 waits for ShareLock on transaction 1582; blocked by process 671.
  10.144s  [S1] COMMIT
```

## Discussion

**Which mechanism does NairaFlow use, and why?**

- **Money movement** uses pessimistic row locks inside `transfer_funds()`. Transfers on the same account are short and highly contended (salary days, a merchant's busy account). Blocking for a few milliseconds is cheaper than repeatedly aborting and retrying. Locking both rows in a deterministic order (by `account_number`) removes the deadlock cycle entirely (S11b).
- **Profile edits and limits** (low contention, longer user "think time" between read and write in the app) use **optimistic locking** with the `version` column (S7). No locks are held while a customer-service agent has a screen open.
- **Cross-row business rules** such as the combined-balance rule (S10) need **SERIALIZABLE**. Row locks on the two rows being updated don't help, because the conflict is between what one transaction *read* and what another *wrote*. PostgreSQL's Serializable Snapshot Isolation tracks those read/write dependencies and aborts one transaction. The cost is that the application must catch SQLSTATE 40001 and retry, so this is used only where needed.
- **READ COMMITTED** (the PostgreSQL default) is kept for ordinary reads. S5 shows why application code must never do read-then-write-absolute-value under it. Use `balance = balance - x` in one statement, `FOR UPDATE`, or a higher isolation level.

**MVCC observation.** In S6 the blocked session waited only for the *row lock*. Plain `SELECT`s from other sessions were never blocked during the whole run, because PostgreSQL readers see a snapshot and don't take locks that block writers (MVCC). S9 shows the same snapshot mechanism exposed at REPEATABLE READ.

**Idempotency.** Mobile networks retry. The UUID `reference` (a `UNIQUE` column) plus the early `EXISTS` check in `transfer_funds()` makes "send money" safe to retry. Portfolio 4 shows the same idea in Redis (`SET … NX`) and MongoDB (unique index).

## Bug found and fixed while building this

The first run failed in S4 with `relation "account" does not exist` inside `lab_deposit()`. The PL/pgSQL functions used unqualified table names, so they only worked when the *caller* had set `search_path = bank`. This also affected `transfer_funds()` and all three trigger functions in Portfolio 1. Besides being fragile, a function that resolves names through the caller's `search_path` is a known security risk (search-path hijacking). Fixed by adding `SET search_path = bank, public` to every function and procedure in [`schema.sql`](../portfolio-1-database-design/schema.sql) and applying it to the live database with `ALTER FUNCTION … SET search_path`.

## How to run

```bash
python portfolio-3-transactions-concurrency/run_demos.py     # ~20 s, recreates nairaflow_p3
```
