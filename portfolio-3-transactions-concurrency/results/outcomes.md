| Scenario | Observed | Verdict |
|---|---|---|
| S1 commit | sender 35000.00, receiver 25000.00; 2 ledger legs | PASS |
| S2 rollback | balance 5000.00, status ACTIVE, txns for 9000 = 0 | PASS |
| S3 deferred constraint | txn rows persisted = 0 | PASS |
| S4 savepoint | 2/3 paid, payer balance 500000.00 | PASS |
| S5 lost update (anomaly shown) | 14000 withdrawn from 10000 but final balance 3000.00; reconciliation vs ledger shows drift -7000.00 | ANOMALY REPRODUCED |
| S6 FOR UPDATE | final balance 3000.00, second withdrawal rejected | PASS |
| S7 optimistic | rows updated S1=1, S2=0; final balance 3000.00 | PASS |
| S8 repeatable read | S2 error 40001, final balance 3000.00 | PASS |
| S9 non-repeatable read | RC reads 40000.00 then 45000.00, RR reads 40000.00 then 40000.00 | PASS (RC shows anomaly, RR prevents it) |
| S10 write skew | REPEATABLE READ: both committed, combined -6000.00; SERIALIZABLE: S2 aborted (40001), combined 2000.00 | PASS (SSI prevents write skew) |
| S11a deadlock (naive order) | victims: ['S2'] | DEADLOCK REPRODUCED and resolved by the DBMS |
| S11b ordered locking | 400 committed, 0 deadlocks, money conserved: True | PASS |
| S12 idempotency | txn rows with that reference = 1, sender balance 18000.00 | PASS |
