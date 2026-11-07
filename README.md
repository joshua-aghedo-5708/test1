# Advanced Database Systems Portfolio: NairaFlow Digital Bank

**MIT 8103 Advanced Database Systems, 2026/2027 First Semester Continuous Assessment**
Student: Joshua Aghedo · Student ID: 301835708

## Selected case study

**NairaFlow** is a simulated Nigerian digital bank (microfinance-bank model) offering savings, current and wallet accounts through mobile, web, USSD, POS, ATM and branch channels.

- **Users of the database:** customers (through the apps), customer-service agents, operations and reconciliation staff, the fraud desk, finance/reporting, compliance auditors, and the core-banking services.
- **Major data:** customers and KYC tiers, accounts and balances, transactions, a double-entry ledger, beneficiaries at other banks, fraud alerts, an audit trail, and reference data (states, regions, branches, banks).
- **Key operations:** open account, transfer funds atomically, reverse failed or disputed transfers, produce statements, reconcile balances against the ledger, detect and investigate fraud, and report volumes per channel.

The same case study and the same generated dataset (20,001 customers, 29,482 accounts, 630,985 transactions, 1,183,886 ledger entries) are used in all five portfolios.

## Portfolio map

| # | Folder | Highlights |
|---|---|---|
| 1 | [`portfolio-1-database-design`](portfolio-1-database-design/) | ERD, 12-table 3NF/BCNF schema, double-entry ledger, deferred constraint trigger, append-only ledger, audit trigger, `transfer_funds()` procedure, reproducible data generator, 15/15 constraint tests pass |
| 2 | [`portfolio-2-query-optimisation`](portfolio-2-query-optimisation/) | 9 queries with before/after `EXPLAIN ANALYZE` plans; composite covering, expression, partial and BRIN indexes (up to ~1,170x faster); query rewrite; materialised view; join-algorithm comparison |
| 3 | [`portfolio-3-transactions-concurrency`](portfolio-3-transactions-concurrency/) | 12 two-session experiments: commit, rollback, savepoint, deferred constraints, lost update, FOR UPDATE, optimistic locking, REPEATABLE READ, non-repeatable read, write skew vs SERIALIZABLE, deadlock and its fix, idempotency |
| 4 | [`portfolio-4-nosql`](portfolio-4-nosql/) | MongoDB document model (embedding, bucket pattern, `$jsonSchema`), index study, aggregation pipelines, multi-document ACID transfer, `$graphLookup` fraud trace; Redis cache, idempotency, rate limit, leaderboard |
| 5 | [`portfolio-5-distributed-cloud`](portfolio-5-distributed-cloud/) | PostgreSQL sync + async replication with crash failover (RPO = 0, 3.2 s to first write), Citus sharding with 2PC, MongoDB replica-set election and quorum loss, proposed AWS deployment (Terraform, not applied) |
| - | [`docs/`](docs/) | 2-3 page LMS summary, AI-use declaration, references, individual reflection |

## Quick start

Requirements: Docker Desktop (4 GB+ for the VM), Python 3.10+.

```bash
pip install -r requirements.txt
docker compose up -d                                         # PostgreSQL 17 :5433, MongoDB 7 :27018, Redis 7 :6380
python portfolio-1-database-design/load_data.py              # ~4 min: schema + 1.2M-row dataset
python portfolio-1-database-design/test_constraints.py
python portfolio-2-query-optimisation/run_benchmarks.py      # ~3 min
python portfolio-3-transactions-concurrency/run_demos.py     # ~20 s
python portfolio-4-nosql/run_nosql.py                        # ~2 min
python portfolio-5-distributed-cloud/run_distributed.py      # ~10 min, starts its own containers
```

Every script is deterministic where possible (fixed random seed) and writes its evidence into its portfolio's `results/` folder (or the portfolio folder itself), so the committed outputs can be regenerated and compared.

## Tech stack

PostgreSQL 17 · Citus 13 · MongoDB 7 · Redis 7 · Docker Compose · Python 3.10 (psycopg 3, pymongo, redis-py, matplotlib) · Mermaid · Terraform (design only)

Credentials in this repository (`nairaflow_dev`, `repl_dev`) are for disposable local Docker containers only.
