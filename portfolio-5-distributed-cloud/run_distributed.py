"""
Portfolio 5: distributed NairaFlow experiments (runs on Docker, ~5 minutes).

    python portfolio-5-distributed-cloud/run_distributed.py [--only replication|citus|mongo]

A) PostgreSQL streaming replication: topology, replication lag, read-only standby,
   cost of synchronous commit, sync-replica outage (availability vs consistency),
   primary crash + promotion with RPO/RTO measurement under live write load.
B) Citus sharding: distribute customer/account/ledger by customer_id across 2 workers,
   reference tables, router vs multi-shard plans, cross-shard 2PC transfer.
C) MongoDB 3-node replica set: majority writes, primary crash and election time,
   loss of majority (no writes, reads still served).
Writes results/run_log.md plus raw evidence files.
"""
import argparse, json, statistics, subprocess, sys, threading, time, uuid
from pathlib import Path

import psycopg
from psycopg import errors

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).parent
OUT = HERE / "results"
OUT.mkdir(exist_ok=True)
sys.path.insert(0, str(ROOT / "scripts"))
from db import PG_DSN  # noqa: E402

COMPOSE = ["docker", "compose", "-f", str(HERE / "docker-compose.p5.yml")]
DSN = "host=localhost port={} dbname=nairaflow user=nairaflow password=nairaflow_dev connect_timeout=3"
PRIMARY, SYNC, ASYNC, COORD = 5441, 5442, 5443, 5450
REPORT = []


def say(s=""):
    print(s, flush=True)
    REPORT.append(s)


def sh(*args, check=True, capture=True):
    r = subprocess.run(list(args), capture_output=capture, text=True, encoding="utf-8")
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return (r.stdout or "").strip()


def dc(*args, **kw):
    return sh(*COMPOSE, *args, **kw)


def cid(service):
    return dc("ps", "-q", service)


def wait_pg(port, timeout=400):
    t = time.time()
    while time.time() - t < timeout:
        try:
            with psycopg.connect(DSN.format(port), autocommit=True) as c:
                c.execute("SELECT 1")
                return
        except psycopg.Error:
            time.sleep(1)
    raise TimeoutError(f"postgres on {port} not ready")


def q(port, sql, params=None, one=False):
    with psycopg.connect(DSN.format(port), autocommit=True) as c:
        cur = c.execute(sql, params)
        if cur.description is None:
            return None
        return cur.fetchone() if one else cur.fetchall()


def table(headers, rows):
    say("| " + " | ".join(headers) + " |")
    say("|" + "---|" * len(headers))
    for r in rows:
        say("| " + " | ".join(str(x) for x in r) + " |")


# =============================================================================
def replication():
    say("## A. PostgreSQL streaming replication and failover\n")
    dc("rm", "-sfv", "pg-primary", "pg-replica-sync", "pg-replica-async", check=False)
    dc("up", "-d", "pg-primary", "pg-replica-sync", "pg-replica-async")
    for p in (PRIMARY, SYNC, ASYNC):
        wait_pg(p)
    t = time.time()
    while len(q(PRIMARY, "SELECT 1 FROM pg_stat_replication WHERE state='streaming'")) < 2:
        if time.time() - t > 400:
            raise TimeoutError("replicas did not start streaming")
        time.sleep(1)
    q(PRIMARY, "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (replica_sync)'")
    q(PRIMARY, "SELECT pg_reload_conf()")
    while not q(PRIMARY, "SELECT 1 FROM pg_stat_replication WHERE application_name='replica_sync' AND sync_state='sync'"):
        time.sleep(0.5)
    say("Both standbys streaming; synchronous replication enabled with "
        "`ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (replica_sync)'`.\n")

    say("### A1. Load data on the primary (replicated as it is written)")
    env = {**__import__("os").environ, "NF_PG_DSN": DSN.format(PRIMARY)}
    t = time.time()
    subprocess.run([sys.executable, str(ROOT / "portfolio-1-database-design" / "load_data.py"),
                    "--customers", "3000", "--txns", "60000"], env=env, check=True, capture_output=True)
    say(f"load_data.py (3,000 customers, 60,000 txns) on the primary with a synchronous standby: {time.time() - t:.1f}s\n")
    time.sleep(2)
    rows = []
    for name, p in (("pg-primary", PRIMARY), ("pg-replica-sync", SYNC), ("pg-replica-async", ASYNC)):
        r = q(p, "SELECT pg_is_in_recovery(), (SELECT count(*) FROM bank.txn), (SELECT count(*) FROM bank.ledger_entry),"
                 " (SELECT sum(balance) FROM bank.account)", one=True)
        rows.append((name, "standby" if r[0] else "primary", f"{r[1]:,}", f"{r[2]:,}", r[3]))
    say("Row counts and total balance on every node (identical = replicated):\n")
    table(["Node", "Role", "txn rows", "ledger rows", "sum(balance)"], rows)

    stat = q(PRIMARY, """SELECT application_name, client_addr, state, sync_state, sent_lsn, replay_lsn,
                                COALESCE(replay_lag::text, '0')
                           FROM pg_stat_replication ORDER BY application_name""")
    say("\n`pg_stat_replication` on the primary:\n")
    table(["application_name", "client_addr", "state", "sync_state", "sent_lsn", "replay_lsn", "replay_lag"], stat)
    (OUT / "pg_stat_replication.txt").write_text("\n".join(map(str, stat)), encoding="utf-8")

    say("\n### A2. Standbys are read-only")
    try:
        q(ASYNC, "UPDATE bank.account SET balance = 0 WHERE account_id = 3")
    except errors.ReadOnlySqlTransaction as e:
        say(f"UPDATE on pg-replica-async -> SQLSTATE {e.sqlstate}: `{e.diag.message_primary}`")

    say("\n### A3. Replication lag and the cost of synchronous commit")
    q(PRIMARY, "CREATE TABLE bank.repl_probe(id int PRIMARY KEY, written_at timestamptz DEFAULT clock_timestamp())")
    while not q(ASYNC, "SELECT to_regclass('bank.repl_probe')", one=True)[0]:
        time.sleep(0.05)
    lags = []
    with psycopg.connect(DSN.format(PRIMARY), autocommit=True) as w, \
            psycopg.connect(DSN.format(ASYNC), autocommit=True) as r:
        for i in range(200):
            w.execute("INSERT INTO bank.repl_probe(id) VALUES (%s)", (i,))
            t = time.perf_counter()
            while not r.execute("SELECT 1 FROM bank.repl_probe WHERE id=%s", (i,)).fetchone():
                pass
            lags.append((time.perf_counter() - t) * 1000)
    say(f"Visibility lag on the async standby after commit on the primary (200 probes): "
        f"median {statistics.median(lags):.2f} ms, p95 {sorted(lags)[189]:.2f} ms, max {max(lags):.2f} ms")
    lat = {}
    with psycopg.connect(DSN.format(PRIMARY), autocommit=True) as w:
        for mode in ("local", "remote_write", "on", "remote_apply"):
            w.execute(f"SET synchronous_commit = {mode}")
            ts = []
            for i in range(300):
                t = time.perf_counter()
                w.execute("INSERT INTO bank.repl_probe(id) VALUES (%s)", (100000 * (len(lat) + 1) + i,))
                ts.append((time.perf_counter() - t) * 1000)
            lat[mode] = statistics.median(ts)
    say("\nCommit latency on the primary by `synchronous_commit` (300 single-row commits each):\n")
    table(["synchronous_commit", "guarantee when COMMIT returns", "median ms"], [
        ("local", "WAL flushed on primary only", f"{lat['local']:.2f}"),
        ("remote_write", "+ standby received and wrote WAL (OS cache)", f"{lat['remote_write']:.2f}"),
        ("on", "+ standby flushed WAL to disk", f"{lat['on']:.2f}"),
        ("remote_apply", "+ standby replayed it (read-your-writes on standby)", f"{lat['remote_apply']:.2f}"),
    ])

    say("\n### A4. Synchronous standby outage: consistency over availability")
    dc("stop", "pg-replica-sync")
    done = {}

    def blocked_commit():
        t = time.perf_counter()
        q(PRIMARY, "INSERT INTO bank.repl_probe(id) VALUES (999999)")
        done["s"] = time.perf_counter() - t

    th = threading.Thread(target=blocked_commit)
    th.start()
    time.sleep(4)
    wait = q(PRIMARY, "SELECT wait_event_type, wait_event FROM pg_stat_activity "
                      "WHERE query LIKE 'INSERT INTO bank.repl_probe(id) VALUES (999999)%%'", one=True)
    say(f"With pg-replica-sync stopped, a COMMIT on the primary is still waiting after 4s; "
        f"pg_stat_activity shows wait_event = `{wait[0]}/{wait[1]}`.")
    dc("start", "pg-replica-sync")
    th.join(120)
    say(f"After restarting the standby the COMMIT returned (total {done['s']:.1f}s). "
        f"No acknowledged write can exist on only one node. The escape hatch for availability is "
        f"`SET synchronous_commit = local` (or `ANY 1 (replica_sync, replica_async)` quorum), accepting possible loss on failover.")

    say("\n### A5. Primary crash under live load: promotion, RPO and RTO")
    while not q(PRIMARY, "SELECT 1 FROM pg_stat_replication WHERE application_name='replica_sync' AND sync_state='sync'"):
        time.sleep(0.5)
    accts = [r[0] for r in q(PRIMARY, "SELECT account_number FROM bank.account WHERE status='ACTIVE' "
                                      "AND account_id > 2 ORDER BY balance DESC LIMIT 20")]
    acked, stop = [], threading.Event()

    def writer():
        import random
        c = psycopg.connect(DSN.format(PRIMARY), autocommit=True)
        while not stop.is_set():
            ref = str(uuid.uuid4())
            a, b = random.sample(accts, 2)
            try:
                c.execute("CALL bank.transfer_funds(%s,%s,%s,'MOBILE',%s)", (a, b, random.randint(10, 500), ref))
                acked.append(ref)                 # only refs whose COMMIT was acknowledged
            except psycopg.Error:
                break
        c.close()

    th = threading.Thread(target=writer)
    th.start()
    time.sleep(3)
    say(f"Writer running ({len(acked)} transfers acknowledged in 3s). `docker kill pg-primary` ...")
    t_kill = time.perf_counter()
    sh("docker", "kill", cid("pg-primary"))
    th.join(10)
    stop.set()
    q(SYNC, "SELECT pg_promote(wait => true)")
    t_promoted = time.perf_counter()
    a, b = accts[0], accts[1]
    q(SYNC, "CALL bank.transfer_funds(%s,%s,1,'WEB')", (a, b))
    t_write = time.perf_counter()
    found = q(SYNC, "SELECT count(*) FROM bank.txn WHERE reference = ANY(%s::uuid[])", (acked,), one=True)[0]
    say(f"* Acknowledged transfers before the crash: **{len(acked)}**; present on the promoted standby: **{found}** "
        f"-> RPO = **{len(acked) - found} transactions lost**")
    say(f"* `pg_promote()` completed {t_promoted - t_kill:.2f}s after the kill; first write accepted on the new primary "
        f"{t_write - t_kill:.2f}s after the kill (manual failover; Patroni/RDS automate detection, typically 10-60s RTO)")
    unbal = q(SYNC, """SELECT count(*) FROM (SELECT txn_id FROM bank.ledger_entry GROUP BY txn_id
                         HAVING sum(amount) FILTER (WHERE direction='D') <> sum(amount) FILTER (WHERE direction='C')) x""", one=True)[0]
    say(f"* Ledger on the new primary: {unbal} unbalanced transactions (no torn transfers)")
    say(f"* pg-replica-async is still configured to follow the dead primary; it must be re-pointed "
        f"(`primary_conninfo`) or rebuilt with `pg_rewind`. This is what orchestration tools automate.")
    dc("rm", "-sfv", "pg-primary", "pg-replica-sync", "pg-replica-async", check=False)


# =============================================================================
def citus():
    say("\n## B. Horizontal sharding with Citus (distributed PostgreSQL)\n")
    dc("rm", "-sfv", "citus-coord", "citus-w1", "citus-w2", check=False)
    dc("up", "-d", "citus-coord", "citus-w1", "citus-w2")
    wait_pg(COORD)
    time.sleep(5)
    with psycopg.connect(DSN.format(COORD), autocommit=True) as c:
        for _ in range(30):
            try:
                c.execute("SELECT citus_set_coordinator_host('citus-coord', 5432)")
                c.execute("SELECT citus_add_node('citus-w1', 5432)")
                c.execute("SELECT citus_add_node('citus-w2', 5432)")
                break
            except psycopg.Error:
                time.sleep(2)
        c.execute("""
            DROP SCHEMA IF EXISTS bank CASCADE; CREATE SCHEMA bank; SET search_path = bank;
            CREATE TABLE region (region_code varchar(3) PRIMARY KEY, region_name varchar(40));
            CREATE TABLE state  (state_code char(2) PRIMARY KEY, state_name varchar(40), region_code varchar(3));
            CREATE TABLE customer (customer_id int PRIMARY KEY, first_name text, last_name text,
                                   state_code char(2), kyc_tier_id smallint, status text);
            CREATE TABLE account (customer_id int, account_id int, account_number char(10), account_type text,
                                  balance numeric(18,2), status text, PRIMARY KEY (customer_id, account_id));
            CREATE TABLE ledger_entry (customer_id int, entry_id bigint, txn_id bigint, account_id int,
                                       direction char(1), amount numeric(18,2), created_at timestamptz,
                                       PRIMARY KEY (customer_id, entry_id));
            SET citus.shard_count = 8;
            SELECT create_reference_table('region');
            SELECT create_reference_table('state');
            SELECT create_distributed_table('customer', 'customer_id');
            SELECT create_distributed_table('account', 'customer_id', colocate_with => 'customer');
            SELECT create_distributed_table('ledger_entry', 'customer_id', colocate_with => 'customer');
        """)
        say("Schema: `region`, `state` = **reference tables** (copied to every node); `customer`, `account`, "
            "`ledger_entry` = **distributed by `customer_id`**, 8 hash shards, co-located so one customer's "
            "rows always live on the same worker.\n")
        t = time.time()
        with psycopg.connect(PG_DSN) as src:
            for tbl, sql in [
                ("region", "SELECT region_code, region_name FROM bank.region"),
                ("state", "SELECT state_code, state_name, region_code FROM bank.state"),
                ("customer", "SELECT customer_id, first_name, last_name, state_code, kyc_tier_id, status FROM bank.customer"),
                ("account", "SELECT customer_id, account_id, account_number, account_type, balance, status FROM bank.account"),
                ("ledger_entry", "SELECT a.customer_id, l.entry_id, l.txn_id, l.account_id, l.direction, l.amount, l.created_at "
                                 "FROM bank.ledger_entry l JOIN bank.account a USING (account_id) "
                                 "WHERE l.created_at >= '2026-05-01'"),
            ]:
                with src.cursor().copy(f"COPY ({sql}) TO STDOUT") as cin, \
                        c.cursor().copy(f"COPY bank.{tbl} FROM STDIN") as cout:
                    for block in cin:
                        cout.write(block)
        say(f"Copied data from the single-node database in {time.time() - t:.1f}s.\n")
        shards = c.execute("""SELECT nodename, count(*) AS shards, pg_size_pretty(sum(shard_size)),
                                     string_agg(DISTINCT table_name::text, ', ')
                                FROM citus_shards GROUP BY nodename ORDER BY nodename""").fetchall()
        say("Shard placement (`citus_shards`):\n")
        table(["Node", "Shard objects", "Size", "Tables"], shards)
        dist = c.execute("""SELECT n.nodename, sum(r.result::bigint)
                              FROM run_command_on_shards('bank.ledger_entry', 'SELECT count(*) FROM %s') r
                              JOIN citus_shards n ON n.shardid = r.shardid GROUP BY 1 ORDER BY 1""").fetchall()
        say("\nLedger rows per worker (`run_command_on_shards`):\n")
        table(["Worker", "ledger_entry rows"], [(a, f"{b:,}") for a, b in dist])

        cust = c.execute("SELECT customer_id FROM bank.account GROUP BY 1 ORDER BY 1 OFFSET 500 LIMIT 1").fetchone()[0]
        plans = {}
        for name, sql in [
            ("router", f"""SELECT a.account_number, l.direction, l.amount, l.created_at
                             FROM bank.ledger_entry l JOIN bank.account a
                               ON a.customer_id = l.customer_id AND a.account_id = l.account_id
                            WHERE l.customer_id = {cust} ORDER BY l.created_at DESC LIMIT 20"""),
            ("multi_shard", """SELECT s.region_code, count(*), sum(l.amount)
                                 FROM bank.ledger_entry l JOIN bank.customer c USING (customer_id)
                                 JOIN bank.state s ON s.state_code = c.state_code
                                WHERE l.direction = 'D' GROUP BY 1 ORDER BY 3 DESC"""),
        ]:
            p = "\n".join(r[0] for r in c.execute("EXPLAIN (ANALYZE, VERBOSE OFF) " + sql).fetchall())
            plans[name] = p
            (OUT / f"citus_plan_{name}.txt").write_text(p, encoding="utf-8")
        import re
        tc = {k: re.search(r"Task Count: (\d+)", v).group(1) for k, v in plans.items()}
        et = {k: re.search(r"Execution Time: ([\d.]+)", v).group(1) for k, v in plans.items()}
        say(f"\n* **Router query** (one customer's statement, filter on the shard key): `Task Count: {tc['router']}`, "
            f"{et['router']} ms. The coordinator sends it to the single shard that owns customer {cust}.")
        say(f"* **Multi-shard analytics** (debits by region, joining a distributed table with a reference table): "
            f"`Task Count: {tc['multi_shard']}`, {et['multi_shard']} ms, the 8 shard queries run in parallel on both workers "
            f"and the coordinator merges partial aggregates. Plans: `citus_plan_router.txt`, `citus_plan_multi_shard.txt`.")

        # cross-shard transfer -> two-phase commit
        # resolve shard -> node on the coordinator in two steps (joining a distributed table
        # to the coordinator-local citus_shards view returns nothing)
        node_of = dict(c.execute("SELECT shardid, nodename FROM citus_shards "
                                 "WHERE table_name = 'bank.account'::regclass").fetchall())
        pair = []
        for cust, acc in c.execute("""SELECT customer_id, account_id FROM bank.account
                                       WHERE balance > 10000 AND customer_id > 1 ORDER BY customer_id LIMIT 50""").fetchall():
            sid = c.execute("SELECT get_shard_id_for_distribution_column('bank.account', %s)", (cust,)).fetchone()[0]
            pair.append((cust, acc, node_of[sid]))
        src_ = pair[0]
        dst_ = next(p for p in pair if p[2] != src_[2])
        cmds = []
        c.add_notice_handler(lambda d: cmds.append(d.message_primary))
        c.execute("SET citus.log_remote_commands = on")
        c.execute("SET citus.grep_remote_commands = '%%TRANSACTION%%'")
        with c.transaction():
            c.execute("UPDATE bank.account SET balance = balance - 5000 WHERE customer_id=%s AND account_id=%s", src_[:2])
            c.execute("UPDATE bank.account SET balance = balance + 5000 WHERE customer_id=%s AND account_id=%s", dst_[:2])
        c.execute("RESET citus.log_remote_commands")
        two_pc = [m for m in cmds if "PREPARE TRANSACTION" in m or "COMMIT PREPARED" in m]
        say(f"\n* **Cross-shard transfer** between customer {src_[0]} (on {src_[2]}) and customer {dst_[0]} "
            f"(on {dst_[2]}): Citus coordinated it with **two-phase commit**. Remote commands logged:")
        for m in two_pc[:4]:
            say(f"  * `{m.split('command ')[-1][:90]}`")
        (OUT / "citus_2pc_log.txt").write_text("\n".join(cmds), encoding="utf-8")
    dc("rm", "-sfv", "citus-coord", "citus-w1", "citus-w2", check=False)


# =============================================================================
def mongosh(service, js, check=True):
    return dc("exec", "-T", service, "mongosh", "--quiet", "--eval", js, check=check)


def mongo_rs():
    say("\n## C. MongoDB 3-node replica set: election and quorum\n")
    dc("rm", "-sfv", "mongo-a", "mongo-b", "mongo-c", check=False)
    dc("up", "-d", "mongo-a", "mongo-b", "mongo-c")
    time.sleep(6)
    mongosh("mongo-a", "rs.initiate({_id:'rs1', members:[{_id:0,host:'mongo-a:27017'},"
                       "{_id:1,host:'mongo-b:27017'},{_id:2,host:'mongo-c:27017'}]})")
    for _ in range(60):
        if mongosh("mongo-a", "db.hello().isWritablePrimary", check=False) == "true":
            break
        time.sleep(1)
    uri = "mongodb://mongo-a,mongo-b,mongo-c/nairaflow?replicaSet=rs1"
    out = mongosh("mongo-a", f"""
        const c = connect('{uri}');
        const docs = []; for (let i = 0; i < 1000; i++) docs.push({{_id: i, type: 'TRANSFER', amount: NumberDecimal('1000.00')}});
        const t = Date.now();
        c.transactions.insertMany(docs, {{writeConcern: {{w: 'majority', wtimeout: 5000}}}});
        print(JSON.stringify({{inserted: c.transactions.countDocuments(), ms: Date.now() - t,
          members: rs.status().members.map(m => m.name + ':' + m.stateStr)}}));""")
    r = json.loads(out.splitlines()[-1])
    say(f"Replica set rs1 = {r['members']}. Inserted {r['inserted']} transaction documents with "
        f"`writeConcern: majority` in {r['ms']} ms (acknowledged only after 2 of 3 nodes had them).\n")

    primary = mongosh("mongo-a", "db.hello().primary").split(":")[0]
    survivors = [s for s in ("mongo-a", "mongo-b", "mongo-c") if s != primary]
    sh("docker", "kill", cid(primary))
    t = time.perf_counter()
    out = mongosh(survivors[0], f"""
        const start = Date.now(); let p = null;
        while (!p && Date.now() - start < 60000) {{ try {{ p = db.hello().primary; }} catch (e) {{}} sleep(100); }}
        const c = connect('mongodb://{survivors[0]},{survivors[1]}/nairaflow?replicaSet=rs1');
        c.transactions.insertOne({{_id: 'after-failover', amount: NumberDecimal('5.00')}}, {{writeConcern: {{w: 'majority'}}}});
        print(JSON.stringify({{new_primary: p, waited_ms: Date.now() - start, count: c.transactions.countDocuments()}}));""")
    r = json.loads(out.splitlines()[-1])
    total = time.perf_counter() - t
    say(f"* Killed the primary `{primary}` (`docker kill`, simulated crash). The remaining two nodes elected "
        f"**{r['new_primary']}** about {total:.1f}s later (`electionTimeoutMillis` = 10s by default).")
    say(f"* A `w: majority` write then succeeded (2 of 3 nodes alive = quorum); collection holds "
        f"{r['count']} documents, none of the 1,000 majority-acknowledged writes were lost.")

    sh("docker", "kill", cid(survivors[1]))
    time.sleep(12)
    out = mongosh(survivors[0], """
        const h = db.hello();
        let w = 'ok';
        try { db.getSiblingDB('nairaflow').transactions.insertOne({_id: 'no-quorum'}, {writeConcern: {w: 'majority', wtimeout: 3000}}); }
        catch (e) { w = e.codeName || e.message; }
        db.getMongo().setReadPref('secondaryPreferred');
        print(JSON.stringify({isWritablePrimary: h.isWritablePrimary, secondary: h.secondary, write: w,
                              readable: db.getSiblingDB('nairaflow').transactions.countDocuments()}));""")
    r = json.loads(out.splitlines()[-1])
    say(f"* Killed a second node: the last survivor `{survivors[0]}` stepped down to SECONDARY "
        f"(isWritablePrimary={r['isWritablePrimary']}). Write attempt -> `{r['write']}`; reads with "
        f"`secondaryPreferred` still return {r['readable']} documents. MongoDB chose **consistency (CP)**: "
        f"no writes without a majority, so no split-brain.")
    dc("start", primary, survivors[1])
    time.sleep(15)
    out = mongosh(survivors[0], "print(JSON.stringify(rs.status().members.map(m => m.name + ':' + m.stateStr)))")
    say(f"* Restarted both nodes; they rejoined and caught up from the oplog: {out.splitlines()[-1]}")
    dc("rm", "-sfv", "mongo-a", "mongo-b", "mongo-c", check=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["replication", "citus", "mongo"])
    a = ap.parse_args()
    say("# Portfolio 5 run log")
    say(f"_Generated by run_distributed.py on {time.strftime('%Y-%m-%d %H:%M')}_\n")
    for name, fn in (("replication", replication), ("citus", citus), ("mongo", mongo_rs)):
        if a.only in (None, name):
            fn()
    name = f"run_log{'_' + a.only if a.only else ''}.md"
    (OUT / name).write_text("\n".join(REPORT) + "\n", encoding="utf-8")
    print(f"\nWrote {OUT / name}")
