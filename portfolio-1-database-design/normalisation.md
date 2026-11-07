# Normalisation of the NairaFlow schema

## Starting point: the "transfer slip" (unnormalised form)

Before the database existed, NairaFlow's operations team tracked transfers in a spreadsheet with one row per transfer:

| slip_no | date | sender_name | sender_bvn | sender_phone | sender_state | sender_region | sender_tier | tier_daily_limit | sender_accounts | receiver_acct | receiver_name | amount | channel |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1001 | 2025-03-02 | Ada Okafor | 22345678901 | +2348031234567 | Lagos | South West | TIER_2 | 200,000 | 0123456789 (SAV), 0987654321 (CUR) | 0555555555 | Musa Bello | 25,000 | MOBILE |

Problems:

- **Repeating group:** `sender_accounts` holds several values in one cell.
- **Update anomaly:** if Ada changes her phone number, every slip she ever sent must be edited.
- **Insert anomaly:** a new customer can't be recorded until they make a transfer.
- **Delete anomaly:** deleting the only slip for a customer deletes the customer.
- **Redundancy:** "Lagos → South West" and "TIER_2 → 200,000" are repeated on thousands of rows.

## 1NF: atomic values, no repeating groups

Split the multi-valued `sender_accounts` cell into separate rows, so every cell holds one value. Each account gets its own row in an `ACCOUNT` relation keyed by `account_number`. Every relation now has a primary key and atomic attributes.

## 2NF: no partial dependency on part of a composite key

After 1NF, a combined relation `TRANSFER_LINE(slip_no, account_number, amount, channel, date, customer_name, ...)` would have the composite key `(slip_no, account_number)`. But:

- `date`, `channel` and `amount` depend only on `slip_no`, which is only part of the key.
- `customer_name` depends only on `account_number`, which is also only part of the key.

Splitting along those partial dependencies gives:

- `TXN(txn_id, date, channel, amount, status, ...)`
- `ACCOUNT(account_number, customer_id, type, ...)`
- `LEDGER_ENTRY(txn_id, account_id, direction, amount, balance_after)`: the *association* between a transaction and the accounts it touches. One transfer has a debit leg and a credit leg (double entry).

`BENEFICIARY` keeps a real composite key `(customer_id, bank_code, account_number)`. Its only non-key attributes (`nickname`, `added_at`) depend on the **whole** key, so it is in 2NF.

## 3NF: no transitive dependencies

Remaining transitive dependencies in `CUSTOMER`:

| Dependency chain | Fix |
|---|---|
| `customer_id → state → region` | `STATE(state_code, state_name, region_code)` and `REGION(region_code, region_name)` |
| `customer_id → kyc_tier → daily_limit, max_balance` | `KYC_TIER(tier_id, tier_name, daily_limit, max_balance)` |
| `customer_id → home_branch → branch_name` | `BRANCH(branch_id, branch_code, branch_name, state_code)` |
| `beneficiary → bank_code → bank_name` | `BANK(bank_code, bank_name)` |

Now if the CBN raises the Tier-2 limit, one row in `KYC_TIER` changes instead of every customer row.

## BCNF

Every determinant in the final schema is a candidate key:

- `CUSTOMER`: `customer_id` (surrogate) and `bvn`, `email`, `phone` (each declared `UNIQUE`) all determine the full row. All of them are candidate keys, so there is no BCNF violation.
- `ACCOUNT`: `account_id` and `account_number` are both candidate keys.
- `TXN`: `txn_id` and `reference` (UUID idempotency key) are both candidate keys.
- `LEDGER_ENTRY`: `entry_id` and `(txn_id, account_id, direction)` are both candidate keys.

## Deliberate, controlled denormalisation

Two attributes are derivable but are stored on purpose:

| Attribute | Derivable from | Why it is stored | How consistency is protected |
|---|---|---|---|
| `account.balance` | `SUM(credits) - SUM(debits)` in `ledger_entry` | Every transfer must check the balance. Summing millions of ledger rows on each request would be far too slow. | Only `transfer_funds()` changes the balance, in the same transaction as the ledger insert. `load_data.py` and `portfolio-2` run a reconciliation query that must return 0 rows. |
| `ledger_entry.balance_after` | Running sum per account | Lets statements and disputes show the balance at any point without a window function over the whole history. | The ledger is append-only (trigger `ledger_append_only`), so stored values cannot drift. |

This trade-off (a read-optimised cache of a derived value, guarded by transactional writes and reconciliation) is standard practice in core banking and is the main design decision of this portfolio.

## Keys and constraints summary

| Kind | Examples |
|---|---|
| Surrogate PKs | `customer_id`, `account_id`, `txn_id` (identity columns) |
| Natural / alternate keys | `bvn`, `email`, `phone`, `account_number` (NUBAN), `txn.reference` (UUID) |
| Composite PK | `beneficiary(customer_id, bank_code, account_number)` |
| Foreign keys | 12 FKs, including the self-reference `txn.reverses_txn_id → txn.txn_id` |
| Domain CHECKs | BVN is 11 digits, NUBAN is 10 digits, phone matches `+234…`, `amount > 0`, enumerated statuses |
| Multi-column CHECKs | `balance >= -overdraft_limit`, only CURRENT accounts may overdraw, CLOSED ⇔ `closed_at` set, a REVERSAL must reference the txn it reverses, FAILED requires `failure_reason` |
| Cross-row rule | Deferred constraint trigger: debits = credits for each txn, checked at COMMIT |
| Immutability | `ledger_entry` rejects UPDATE/DELETE |
| Auditing | Row-level trigger writes old/new JSONB images of `account` changes to `audit_log` |
