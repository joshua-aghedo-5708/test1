"""
Generate and bulk-load a realistic, internally consistent NairaFlow dataset.

    python portfolio-1-database-design/load_data.py [--customers 20000] [--txns 600000]

* Deterministic (random.seed) so every run produces the same data.
* Balances are simulated in time order, so ledger_entry.balance_after and
  account.balance agree, and transfers that would overdraw are recorded as
  FAILED with no ledger legs (just like a real core-banking system).
* Uses COPY for speed; user triggers are disabled during the load and the
  double-entry rule is then verified with one set-based query.
"""
import argparse, bisect, datetime as dt, math, random, sys, time, uuid
from decimal import Decimal
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from db import PG_DSN  # noqa: E402

HERE = Path(__file__).parent
random.seed(8103)

REGIONS = [("NC", "North Central"), ("NE", "North East"), ("NW", "North West"),
           ("SE", "South East"), ("SS", "South South"), ("SW", "South West")]
STATES = [("LA", "Lagos", "SW", 30), ("OY", "Oyo", "SW", 6), ("OG", "Ogun", "SW", 6),
          ("FC", "FCT Abuja", "NC", 12), ("KW", "Kwara", "NC", 3), ("PL", "Plateau", "NC", 2),
          ("KN", "Kano", "NW", 7), ("KD", "Kaduna", "NW", 5), ("SO", "Sokoto", "NW", 1),
          ("BO", "Borno", "NE", 1), ("BA", "Bauchi", "NE", 1), ("AD", "Adamawa", "NE", 1),
          ("EN", "Enugu", "SE", 5), ("AN", "Anambra", "SE", 5), ("IM", "Imo", "SE", 3),
          ("RI", "Rivers", "SS", 8), ("ED", "Edo", "SS", 4), ("DE", "Delta", "SS", 4),
          ("AK", "Akwa Ibom", "SS", 2), ("CR", "Cross River", "SS", 2)]
TIERS = [(1, "TIER_1", 50_000, 300_000), (2, "TIER_2", 200_000, 500_000), (3, "TIER_3", 5_000_000, None)]
BANKS = [("044", "Access Bank"), ("058", "GTBank"), ("011", "First Bank"), ("033", "UBA"),
         ("057", "Zenith Bank"), ("070", "Fidelity Bank"), ("232", "Sterling Bank"),
         ("214", "FCMB"), ("035", "Wema Bank"), ("999", "NairaFlow MFB")]
FIRST = ("Chinedu Ngozi Emeka Adaeze Tunde Bisi Femi Funmi Ibrahim Aisha Musa Hauwa Uche Ifeoma "
         "Segun Yetunde Kelechi Amaka Bola Kemi Ade Tolu Zainab Yusuf Obinna Chioma Efe Osas "
         "Ese Tamuno Ebi Nnamdi Halima Sani Bukola Damilola Ijeoma Ikenna Sola Temitope").split()
LAST = ("Okafor Adeyemi Bello Okonkwo Abubakar Eze Balogun Nwosu Ogunleye Ibrahim Danjuma Obi "
        "Adebayo Umeh Lawal Oyelaran Akpan Etim Igwe Mohammed Salami Oladipo Nnadi Fashola "
        "Ekanem Usman Chukwu Afolabi Garba Onyeka Ojo Babatunde Ike Orji Oronsaye Briggs").split()
CHANNELS = ["MOBILE", "WEB", "USSD", "POS", "ATM", "BRANCH"]

GL_CASH, GL_FEES = 1, 2   # account_ids of the bank's own GL accounts
T0 = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
T1 = dt.datetime(2026, 10, 31, tzinfo=dt.timezone.utc)


def money(x: float) -> Decimal:
    return Decimal(str(round(max(x, 100.0), 2)))


def rand_ts(a: dt.datetime, b: dt.datetime) -> dt.datetime:
    return a + dt.timedelta(seconds=random.random() * (b - a).total_seconds())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=20_000)
    ap.add_argument("--txns", type=int, default=600_000)
    args = ap.parse_args()
    t_start = time.time()

    with psycopg.connect(PG_DSN, autocommit=False) as conn:
        cur = conn.cursor()
        print("Creating schema ...")
        cur.execute((HERE / "schema.sql").read_text(encoding="utf-8"))
        cur.execute("SET search_path = bank, public")

        # ---------- reference data ----------
        cur.executemany("INSERT INTO region VALUES (%s,%s)", REGIONS)
        cur.executemany("INSERT INTO state VALUES (%s,%s,%s)", [s[:3] for s in STATES])
        cur.executemany("INSERT INTO kyc_tier VALUES (%s,%s,%s,%s)", TIERS)
        cur.executemany("INSERT INTO bank VALUES (%s,%s)", BANKS)
        branches = []
        for i, (code, name, _, _) in enumerate(STATES):
            branches.append((f"B{i+1:03d}", f"{name} Main", code))
        cur.executemany("INSERT INTO branch(branch_code,branch_name,state_code) VALUES (%s,%s,%s)", branches)
        branch_of_state = {s[0]: i + 1 for i, s in enumerate(STATES)}

        # ---------- customers ----------
        print(f"Generating {args.customers:,} customers ...")
        state_codes = [s[0] for s in STATES]
        state_w = [s[3] for s in STATES]
        customers = []
        # customer 1 = the bank itself (owner of the GL accounts)
        customers.append((1, "00000000000", "NairaFlow", "Treasury", "treasury@nairaflow.ng",
                          "+2340000000000", dt.date(1990, 1, 1), "LA", branch_of_state["LA"], 3,
                          "ACTIVE", dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)))
        bvns = set()
        for cid in range(2, args.customers + 2):
            fn, ln = random.choice(FIRST), random.choice(LAST)
            while (bvn := f"{random.randint(10**10, 10**11 - 1)}") in bvns:
                pass
            bvns.add(bvn)
            st = random.choices(state_codes, weights=state_w)[0]
            created = rand_ts(dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
                              dt.datetime(2024, 12, 31, tzinfo=dt.timezone.utc))
            # must be 18+ on the sign-up date (CHECK customer_adult); 2003 cut-off covers 2022 sign-ups
            dob = dt.date(random.randint(1960, 2003), random.randint(1, 12), random.randint(1, 28))
            if (dob.year + 18, dob.month, dob.day) > (created.year, created.month, created.day):
                dob = dob.replace(year=created.year - 19)
            tier = random.choices([1, 2, 3], weights=[35, 40, 25])[0]
            status = random.choices(["ACTIVE", "DORMANT", "BLOCKED"], weights=[94, 5, 1])[0]
            customers.append((cid, bvn, fn, ln, f"{fn.lower()}.{ln.lower()}{cid}@mail.ng",
                              f"+234{8000000000 + cid}", dob, st, branch_of_state[st], tier, status, created))
        with cur.copy("COPY customer(customer_id,bvn,first_name,last_name,email,phone,date_of_birth,"
                      "state_code,home_branch_id,kyc_tier_id,status,created_at) FROM STDIN") as cp:
            for row in customers:
                cp.write_row(row)

        # ---------- accounts ----------
        accounts = []   # (id, nuban, cid, type, balance, overdraft, status, opened)
        accounts.append([GL_CASH, "0000000001", 1, "CURRENT", Decimal(0), Decimal("9000000000000"), "ACTIVE", customers[0][11]])
        accounts.append([GL_FEES, "0000000002", 1, "CURRENT", Decimal(0), Decimal(0), "ACTIVE", customers[0][11]])
        nubans = {"0000000001", "0000000002"}
        aid = 3
        for c in customers[1:]:
            n = random.choices([1, 2, 3], weights=[60, 32, 8])[0]
            types = random.sample(["SAVINGS", "CURRENT", "WALLET"], n)
            for t in types:
                while (nuban := f"{random.randint(10**9, 10**10 - 1)}") in nubans:
                    pass
                nubans.add(nuban)
                od = Decimal(random.choice([0, 0, 50_000, 200_000])) if t == "CURRENT" else Decimal(0)
                status = "FROZEN" if c[10] == "BLOCKED" else "ACTIVE"
                accounts.append([aid, nuban, c[0], t, Decimal(0), od, status, c[11]])
                aid += 1
        n_acc = len(accounts)
        print(f"Generated {n_acc:,} accounts")

        # ---------- transactions (simulated in time order) ----------
        print(f"Simulating {args.txns:,} transactions ...")
        txns, legs = [], []
        tid = 0

        def post(ttype, channel, amount, status, ts, entries, narr=None, rev=None, reason=None):
            nonlocal tid
            tid += 1
            done = ts + dt.timedelta(seconds=random.randint(1, 20)) if status in ("COMPLETED", "REVERSED") else None
            txns.append([tid, str(uuid.UUID(int=random.getrandbits(128))), ttype, channel, amount,
                         status, narr, rev, reason, ts, done])
            for acc_idx, d, amt in entries:
                acc = accounts[acc_idx]
                acc[4] += amt if d == "C" else -amt
                legs.append((tid, acc[0], d, amt, acc[4], ts))
            return tid

        # opening deposit for every customer account
        opening = []
        for idx in range(2, n_acc):
            opening.append((accounts[idx][7] + dt.timedelta(minutes=5), idx))
        opening.sort()
        for ts, idx in opening:
            amt = money(random.lognormvariate(math.log(40_000), 1.0))
            post("DEPOSIT", "BRANCH", amt, "COMPLETED", ts,
                 [(idx, "C", amt), (0, "D", amt)], "Account opening deposit")

        # activity is skewed: a few accounts are very busy (Zipf-like weights)
        weights = [0, 0] + [1.0 / (i ** 0.8) for i in range(1, n_acc - 1)]
        order = list(range(2, n_acc))
        random.shuffle(order)
        w = [0.0] * n_acc
        for rank, idx in enumerate(order, start=1):
            w[idx] = weights[rank + 1]
        cum, s = [], 0.0
        for x in w:
            s += x
            cum.append(s)

        def pick():
            return bisect.bisect_left(cum, random.random() * s)

        stamps = sorted(rand_ts(T0, T1) for _ in range(args.txns))
        completed_transfers = []
        for ts in stamps:
            r = random.random()
            channel = random.choices(CHANNELS, weights=[55, 12, 10, 12, 8, 3])[0]
            a = pick()
            acc = accounts[a]
            if r < 0.16:                                   # deposit / salary credit
                amt = money(random.lognormvariate(math.log(120_000), 0.9))
                post("DEPOSIT", random.choice(["BRANCH", "WEB", "MOBILE"]), amt, "COMPLETED", ts,
                     [(a, "C", amt), (0, "D", amt)], "Inflow")
            elif r < 0.30:                                 # cash withdrawal
                amt = money(random.lognormvariate(math.log(15_000), 0.8))
                ch = random.choice(["ATM", "POS", "BRANCH"])
                if acc[6] == "ACTIVE" and acc[4] - amt >= -acc[5]:
                    post("WITHDRAWAL", ch, amt, "COMPLETED", ts, [(a, "D", amt), (0, "C", amt)], "Cash withdrawal")
                else:
                    post("WITHDRAWAL", ch, amt, "FAILED", ts, [], "Cash withdrawal",
                         reason="INSUFFICIENT_FUNDS" if acc[6] == "ACTIVE" else "ACCOUNT_FROZEN")
            elif r < 0.33:                                 # SMS / maintenance fee
                amt = Decimal(random.choice(["50.00", "100.00", "150.00"]))
                if acc[4] - amt >= -acc[5]:
                    post("FEE", "WEB", amt, "COMPLETED", ts, [(a, "D", amt), (1, "C", amt)], "Maintenance fee")
            else:                                          # transfer between customers
                b = pick()
                while b == a:
                    b = pick()
                amt = money(random.lognormvariate(math.log(18_000), 1.1))
                if acc[6] != "ACTIVE" or accounts[b][6] != "ACTIVE":
                    post("TRANSFER", channel, amt, "FAILED", ts, [], "Transfer", reason="ACCOUNT_FROZEN")
                elif acc[4] - amt < -acc[5]:
                    post("TRANSFER", channel, amt, "FAILED", ts, [], "Transfer", reason="INSUFFICIENT_FUNDS")
                else:
                    t = post("TRANSFER", channel, amt, "COMPLETED", ts, [(a, "D", amt), (b, "C", amt)], "Transfer")
                    completed_transfers.append((t, a, b, amt, ts))
                    # ~0.4% are disputed and reversed within 2 days
                    if random.random() < 0.004 and accounts[b][4] - amt >= -accounts[b][5]:
                        txns[t - 1][5] = "REVERSED"
                        post("REVERSAL", "WEB", amt, "COMPLETED", ts + dt.timedelta(hours=random.randint(1, 48)),
                             [(b, "D", amt), (a, "C", amt)], f"Reversal of txn {t}", rev=t)

        print(f"  {len(txns):,} txns, {len(legs):,} ledger legs")

        # ---------- bulk load ----------
        cur.execute("ALTER TABLE account DISABLE TRIGGER USER")
        cur.execute("ALTER TABLE ledger_entry DISABLE TRIGGER USER")
        with cur.copy("COPY account(account_id,account_number,customer_id,account_type,balance,"
                      "overdraft_limit,status,opened_at) FROM STDIN") as cp:
            for acc in accounts:
                cp.write_row(acc)
        # reversals point at earlier txns, so load in id order (FK is satisfied row by row)
        with cur.copy("COPY txn(txn_id,reference,txn_type,channel,amount,status,narration,"
                      "reverses_txn_id,failure_reason,created_at,completed_at) FROM STDIN") as cp:
            for t in txns:
                cp.write_row(t)
        with cur.copy("COPY ledger_entry(txn_id,account_id,direction,amount,balance_after,created_at) FROM STDIN") as cp:
            for leg in legs:
                cp.write_row(leg)

        # beneficiaries and fraud alerts
        bens = set()
        for _ in range(args.customers // 2):
            c = random.randint(2, args.customers + 1)
            bens.add((c, random.choice(BANKS)[0], f"{random.randint(10**9, 10**10 - 1)}"))
        with cur.copy("COPY beneficiary(customer_id,bank_code,account_number) FROM STDIN") as cp:
            for b in bens:
                cp.write_row(b)
        cur.execute("""
            INSERT INTO fraud_alert(txn_id, rule_code, risk_score, status, raised_at)
            SELECT txn_id,
                   CASE WHEN amount > 1000000 THEN 'HIGH_VALUE' ELSE 'NIGHT_USSD' END,
                   LEAST(99, 40 + amount / 50000)::numeric(5,2),
                   (ARRAY['OPEN','CLEARED','CONFIRMED'])[1 + (txn_id % 3)],
                   created_at
              FROM txn
             WHERE txn_type = 'TRANSFER' AND status = 'COMPLETED'
               AND (amount > 1000000
                    OR (channel = 'USSD' AND extract(hour FROM created_at) BETWEEN 1 AND 4))""")

        # re-sync identity sequences after explicit ids
        for tbl, col in [("customer", "customer_id"), ("account", "account_id"), ("txn", "txn_id")]:
            cur.execute(f"SELECT setval(pg_get_serial_sequence('bank.{tbl}','{col}'), (SELECT max({col}) FROM {tbl}))")

        # verify integrity rules that were bypassed by disabling triggers
        cur.execute("""SELECT count(*) FROM (
                         SELECT txn_id FROM ledger_entry GROUP BY txn_id
                         HAVING sum(amount) FILTER (WHERE direction='D')
                             <> sum(amount) FILTER (WHERE direction='C')) x""")
        unbalanced = cur.fetchone()[0]
        # set-based reconciliation: one hash aggregate over the ledger, then a join
        # (a correlated subquery here re-scans the ledger once per account: O(accounts x entries))
        cur.execute("""SELECT count(*) FROM account a
                         LEFT JOIN (SELECT account_id, sum(CASE direction WHEN 'C' THEN amount ELSE -amount END) AS net
                                      FROM ledger_entry GROUP BY account_id) l USING (account_id)
                        WHERE a.balance <> COALESCE(l.net, 0)""")
        drift = cur.fetchone()[0]
        if unbalanced or drift:
            raise SystemExit(f"Integrity check failed: unbalanced={unbalanced}, balance drift={drift}")
        cur.execute("ALTER TABLE account ENABLE TRIGGER USER")
        cur.execute("ALTER TABLE ledger_entry ENABLE TRIGGER USER")
        conn.commit()

        conn.autocommit = True
        cur.execute("VACUUM (ANALYZE)")
        cur.execute("""SELECT relname, n_live_tup FROM pg_stat_user_tables
                        WHERE schemaname='bank' ORDER BY n_live_tup DESC""")
        print("\nRow counts:")
        for name, n in cur.fetchall():
            print(f"  {name:<14}{n:>12,}")
        print(f"\nIntegrity: 0 unbalanced txns, 0 balance drift. Done in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
