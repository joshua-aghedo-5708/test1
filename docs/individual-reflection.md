# Individual Technical Reflection

> **To the student:** this section must be written by you, in your own words (about 300-500 words). The brief (sections 8 and 12) says it is the main evidence that *you* understand the work, and you may be asked to explain it. The prompts below point to real events from building this portfolio. Pick the ones you can explain confidently, re-run the scripts yourself, and delete these notes when you are done.

## Design choices (prompts)

- Why a double-entry ledger instead of just updating `account.balance`? What does the deferred constraint trigger catch that a CHECK cannot?
- Why store `balance` even though it can be derived from the ledger (controlled denormalisation), and how is drift detected?
- Why `customer_id` as the Citus shard key and not `account_id` or `created_at`?
- Which concurrency mechanism would you use for transfers vs profile edits, and why (P3 discussion)?

## Challenges faced (real events you can talk about)

- The first data load failed: the `customer_adult` constraint rejected generated customers under 18. The constraint found a bug in the generator.
- A correlated-subquery reconciliation ran for over 8 minutes; rewriting it as a grouped join took about 1 second.
- PL/pgSQL functions failed with `relation "account" does not exist` because they relied on the caller's `search_path` (and why that is also a security issue).
- Synchronous replication configured at first boot made PostgreSQL's own initialisation hang (bootstrap deadlock).
- Benchmark timings varied up to 2x because the host machine was heavily loaded. Why plan shape and buffer counts are more reliable than raw milliseconds.
- Indexing alone did not fix the Q5 analytics query; a rewrite and a materialised view did.

## Lessons learned (prompts)

- What surprised you about BRIN vs B-tree size (32 kB vs 13 MB)?
- What did the write-skew experiment teach you about REPEATABLE READ vs SERIALIZABLE?
- What do RPO = 0 and the blocked commit in A4 say about the CAP trade-off for a bank?
- What would you do differently or next (e.g. capped statement buckets in MongoDB, automated failover with Patroni)?

## My reflection

`<WRITE YOUR REFLECTION HERE>`
