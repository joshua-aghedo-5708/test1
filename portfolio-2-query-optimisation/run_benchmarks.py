"""
Portfolio 2 benchmark: baseline vs. indexed execution plans for every query in queries.sql.

    python portfolio-2-query-optimisation/run_benchmarks.py

Steps
  1. drop the tuning indexes (baseline = PK/UNIQUE constraint indexes only)
  2. EXPLAIN (ANALYZE, BUFFERS) each query 1 warm-up + 5 timed runs, keep the median
  3. apply indexes.sql
  4. repeat step 2
  5. extra experiments: join algorithms, B-tree vs BRIN size, materialised view
  6. write results/*.txt plans, results/summary.md, results/summary.csv, results/timings.png
"""
import csv, re, statistics, sys, time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from db import PG_DSN  # noqa: E402

HERE = Path(__file__).parent
OUT = HERE / "results"
OUT.mkdir(exist_ok=True)
RUNS = 7

TUNING_INDEXES = re.findall(r"CREATE INDEX IF NOT EXISTS (\w+)", (HERE / "indexes.sql").read_text())


def load_queries():
    text = (HERE / "queries.sql").read_text(encoding="utf-8")
    blocks = re.split(r"^-- name: (\w+)\s*$", text, flags=re.M)[1:]
    out = {}
    for name, body in zip(blocks[::2], blocks[1::2]):
        purpose = re.findall(r"^-- (.*)$", body, flags=re.M)
        sql = "\n".join(l for l in body.strip().splitlines() if not l.startswith("--")).strip().rstrip(";")
        out[name] = (purpose[0] if purpose else "", sql)
    return out


def explain(cur, sql, params, timing=False):
    # TIMING OFF for the measured runs: per-node clock calls add large overhead to
    # row-heavy plans (especially inside Docker on Windows). One fully-timed plan is kept as evidence.
    opts = "ANALYZE, BUFFERS, SETTINGS" + ("" if timing else ", TIMING OFF")
    cur.execute(f"EXPLAIN ({opts}) " + sql, params)
    return "\n".join(r[0] for r in cur.fetchall())


def exec_ms(plan):
    return float(re.search(r"Execution Time: ([\d.]+) ms", plan).group(1))


def top_node(plan):
    """Short description of the access paths used, for the summary table."""
    nodes = re.findall(r"(Parallel )?(Seq Scan|Index Only Scan|Index Scan|Bitmap Heap Scan|Bitmap Index Scan)"
                       r"(?: Backward)?(?: using (\w+))? on (\w+)", plan)
    seen, parts = set(), []
    for par, kind, idx, tbl in nodes:
        label = f"{par or ''}{kind} {tbl}" + (f" ({idx})" if idx else "")
        if label not in seen:
            seen.add(label)
            parts.append(label)
    return "; ".join(parts[:3])


def measure(cur, queries, params, phase):
    rows = {}
    for name, (_, sql) in queries.items():
        explain(cur, sql, params)  # warm-up (cache)
        plans = [explain(cur, sql, params) for _ in range(RUNS)]
        times = [exec_ms(p) for p in plans]
        med = statistics.median(times)
        plan = plans[times.index(sorted(times)[len(times) // 2])]
        (OUT / f"{name}_{phase}.txt").write_text(
            f"-- {name} ({phase}) median of {RUNS} runs: {med:.2f} ms\n-- runs: {times}\n\n{plan}\n", encoding="utf-8")
        rows[name] = (med, top_node(plan))
        print(f"  {phase:<6} {name:<26}{med:>10.2f} ms   {top_node(plan)}")
    return rows


def main():
    queries = load_queries()
    with psycopg.connect(PG_DSN, autocommit=True, cursor_factory=psycopg.ClientCursor) as conn:
        cur = conn.cursor()
        cur.execute("SET search_path = bank, public")
        cur.execute("SET max_parallel_workers_per_gather = 2")

        # pick realistic parameters from the data: the busiest customer account and a real e-mail
        cur.execute("""SELECT account_id FROM ledger_entry WHERE account_id > 2
                        GROUP BY account_id ORDER BY count(*) DESC LIMIT 1""")
        acct = cur.fetchone()[0]
        cur.execute("SELECT upper(email) FROM customer WHERE customer_id = 4242")
        email = cur.fetchone()[0]
        params = {"acct": acct, "email": email}
        print(f"Parameters: account_id={acct}, email={email}")

        cur.execute("""SELECT relname, n_live_tup FROM pg_stat_user_tables
                        WHERE schemaname='bank' AND relname IN ('txn','ledger_entry','account','customer')""")
        sizes = dict(cur.fetchall())

        print("\n[1] Baseline (constraint indexes only)")
        for ix in TUNING_INDEXES:
            cur.execute(f"DROP INDEX IF EXISTS bank.{ix}")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS bank.mv_monthly_outflow")
        cur.execute("ANALYZE")
        before = measure(cur, queries, params, "before")

        print("\n[2] Applying indexes.sql")
        t = time.time()
        cur.execute((HERE / "indexes.sql").read_text(encoding="utf-8"))
        build_s = time.time() - t
        print(f"  built in {build_s:.1f}s")
        after = measure(cur, queries, params, "after")

        # ---- extra experiment A: index sizes, B-tree vs BRIN on the same column ----
        cur.execute("CREATE INDEX tmp_btree_txn_created ON txn (created_at)")
        cur.execute("""SELECT c.relname, am.amname, pg_size_pretty(pg_relation_size(c.oid)), pg_relation_size(c.oid)
                         FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
                         JOIN pg_am am ON am.oid = c.relam
                         JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'bank' AND c.relname = ANY(%s)
                        ORDER BY pg_relation_size(c.oid) DESC""", (TUNING_INDEXES + ["tmp_btree_txn_created"],))
        index_sizes = cur.fetchall()
        cur.execute("DROP INDEX tmp_btree_txn_created")

        # ---- extra experiment B: join algorithms for Q5 ----
        q5 = queries["q5_top_spenders"][1]
        join_rows = []
        for label, settings in [
            ("planner choice", []),
            ("hash join disabled", ["enable_hashjoin = off"]),
            ("hash + merge disabled (nested loop)", ["enable_hashjoin = off", "enable_mergejoin = off"]),
        ]:
            for s in settings:
                cur.execute(f"SET {s}")
            explain(cur, q5, params)
            plans = [explain(cur, q5, params) for _ in range(3)]
            ms = statistics.median(exec_ms(p) for p in plans)
            joins = sorted(set(re.findall(r"(Hash Join|Merge Join|Nested Loop)", plans[0])))
            join_rows.append((label, ", ".join(joins), ms))
            (OUT / f"q5_joins_{['planner', 'no_hash', 'nested_loop'][len(join_rows) - 1]}.txt").write_text(plans[0], encoding="utf-8")
            cur.execute("RESET enable_hashjoin; RESET enable_mergejoin")

        # ---- extra experiment C: materialised view for the Q5 analytics workload ----
        t = time.time()
        cur.execute("""CREATE MATERIALIZED VIEW bank.mv_monthly_outflow AS
                         SELECT a.customer_id, date_trunc('month', l.created_at) AS month,
                                sum(l.amount) AS outflow, count(*) AS debits
                           FROM ledger_entry l JOIN account a ON a.account_id = l.account_id
                          WHERE l.direction = 'D' AND a.customer_id <> 1
                          GROUP BY 1, 2""")
        cur.execute("CREATE INDEX ON bank.mv_monthly_outflow (month)")
        cur.execute("ANALYZE bank.mv_monthly_outflow")
        mv_build = time.time() - t
        mv_sql = """SELECT m.customer_id, c.first_name || ' ' || c.last_name AS name, r.region_name,
                           sum(m.outflow) AS outflow, sum(m.debits) AS debits
                      FROM bank.mv_monthly_outflow m
                      JOIN customer c ON c.customer_id = m.customer_id
                      JOIN state s ON s.state_code = c.state_code
                      JOIN region r ON r.region_code = s.region_code
                     WHERE m.month >= '2026-07-01' AND m.month < '2026-10-01'
                     GROUP BY 1, 2, 3 ORDER BY outflow DESC LIMIT 10"""
        explain(cur, mv_sql, params)
        mv_plans = [explain(cur, mv_sql, params) for _ in range(RUNS)]
        mv_ms = statistics.median(exec_ms(p) for p in mv_plans)
        (OUT / "q5_materialized_view.txt").write_text(mv_plans[0], encoding="utf-8")
        cur.execute(q5, params)
        live = cur.fetchall()
        cur.execute(mv_sql, params)
        mv_res = cur.fetchall()
        mv_same = [r[0] for r in live] == [r[0] for r in mv_res]

        # ---- reconciliation (correctness check for the denormalised balance) ----
        cur.execute("""SELECT count(*) FROM account a
                         LEFT JOIN (SELECT account_id, sum(CASE direction WHEN 'C' THEN amount ELSE -amount END) AS net
                                      FROM ledger_entry GROUP BY account_id) l USING (account_id)
                        WHERE a.balance <> COALESCE(l.net, 0)""")
        drift = cur.fetchone()[0]

    # ---- write summary ----
    with open(OUT / "summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "before_ms", "after_ms", "speedup", "plan_before", "plan_after"])
        for n in queries:
            b, a = before[n][0], after[n][0]
            w.writerow([n, f"{b:.2f}", f"{a:.2f}", f"{b / a:.1f}", before[n][1], after[n][1]])

    lines = ["# Portfolio 2 benchmark results", "",
             f"Generated by `run_benchmarks.py`. Median of {RUNS} `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` runs after one warm-up run.",
             f"Data: {sizes.get('customer', 0):,} customers, {sizes.get('account', 0):,} accounts, "
             f"{sizes.get('txn', 0):,} txns, {sizes.get('ledger_entry', 0):,} ledger entries. "
             f"Parameters: account_id={params['acct']}, email={params['email']}.", "",
             "## Before vs after indexing", "",
             "| Query | Before (ms) | After (ms) | Speed-up | Access path before | Access path after |",
             "|---|---:|---:|---:|---|---|"]
    for n in queries:
        b, a = before[n][0], after[n][0]
        lines.append(f"| {n} | {b:.2f} | {a:.2f} | {b / a:.1f}x | {before[n][1]} | {after[n][1]} |")
    lines += ["", f"All tuning indexes built in {build_s:.1f}s.", "",
              "## Index sizes (B-tree vs BRIN on `txn.created_at`)", "",
              "| Index | Type | Size |", "|---|---|---:|"]
    lines += [f"| {r[0]} | {r[1]} | {r[2]} |" for r in index_sizes]
    lines += ["", "`tmp_btree_txn_created` was built only for this comparison and then dropped.", "",
              "## Join algorithms for Q5 (planner settings forced per session)", "",
              "| Setting | Join nodes in plan | Median (ms) |", "|---|---|---:|"]
    lines += [f"| {r[0]} | {r[1]} | {r[2]:.2f} |" for r in join_rows]
    lines += ["", "## Materialised view for Q5", "",
              f"* Live query (indexed): **{after['q5_top_spenders'][0]:.2f} ms**; rewritten (aggregate first): "
              f"**{after['q5b_top_spenders_rewritten'][0]:.2f} ms**",
              f"* Same report from `mv_monthly_outflow`: **{mv_ms:.2f} ms** (view built in {mv_build:.1f}s)",
              f"* Same top-10 customers returned: **{mv_same}**", "",
              "## Integrity", "", f"Accounts whose stored balance differs from the ledger: **{drift}**", ""]
    (OUT / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    # ---- chart ----
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    names = list(queries)
    fig, ax = plt.subplots(figsize=(10, 4.8))
    x = range(len(names))
    ax.bar([i - 0.2 for i in x], [before[n][0] for n in names], 0.4, label="before (constraint indexes only)", color="#9aa5b1")
    ax.bar([i + 0.2 for i in x], [after[n][0] for n in names], 0.4, label="after indexes.sql", color="#2f6fdb")
    ax.set_yscale("log")
    ax.set_ylabel("median execution time, ms (log scale)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(names, rotation=25, ha="right", fontsize=8)
    ax.set_title("NairaFlow query latency before and after indexing")
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(OUT / "timings.png", dpi=150)
    print(f"\nWrote {OUT / 'summary.md'}")


if __name__ == "__main__":
    main()
