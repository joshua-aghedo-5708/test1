"""
Capture terminal-style evidence screenshots for every portfolio.

    python scripts/capture_evidence.py

Each image is produced by running a real command (a live psql query against the
nf-postgres container, or a command that prints a committed results file) and
rendering its exact output, with the command shown in the title bar.
Images are written to portfolio-*/screenshots/.
"""
import subprocess, textwrap
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
FONT = ImageFont.truetype("C:/Windows/Fonts/consola.ttf", 15)
BOLD = ImageFont.truetype("C:/Windows/Fonts/consolab.ttf", 15)
BG, BAR, FG, DIM, PROMPT = (24, 26, 33), (44, 47, 58), (220, 223, 228), (150, 156, 168), (126, 200, 120)
WRAP, MAXLINES = 118, 46
PSQL = 'docker exec nf-postgres psql -U nairaflow -d nairaflow -P pager=off'

SHOTS = [
    # ---------------- Portfolio 1 ----------------
    ("portfolio-1-database-design", "P1_TableList",
     PSQL + ' -c "\\dt+ bank.*"'),
    ("portfolio-1-database-design", "P1_RowCountsAndIntegrity",
     PSQL + ''' -c "SELECT relname AS table_name, n_live_tup AS row_count FROM pg_stat_user_tables WHERE schemaname='bank' ORDER BY 2 DESC" -c "SELECT count(*) AS unbalanced_txns FROM (SELECT txn_id FROM bank.ledger_entry GROUP BY txn_id HAVING sum(amount) FILTER (WHERE direction='D') <> sum(amount) FILTER (WHERE direction='C')) x" -c "SELECT count(*) AS accounts_with_balance_drift FROM bank.account a LEFT JOIN (SELECT account_id, sum(CASE direction WHEN 'C' THEN amount ELSE -amount END) net FROM bank.ledger_entry GROUP BY 1) l USING (account_id) WHERE a.balance <> COALESCE(l.net,0)"'''),
    ("portfolio-1-database-design", "P1_ConstraintViolations",
     PSQL + ''' -c "INSERT INTO bank.customer(bvn,first_name,last_name,email,phone,date_of_birth,state_code,home_branch_id) VALUES ('123','A','B','a@b.ng','+2348099999999','1990-01-01','LA',1)" -c "INSERT INTO bank.customer(bvn,first_name,last_name,email,phone,date_of_birth,state_code,home_branch_id) VALUES ('99999999999','A','B','kid@b.ng','+2348099999999',current_date - 3650,'LA',1)" -c "UPDATE bank.account SET overdraft_limit = 1000 WHERE account_id = (SELECT min(account_id) FROM bank.account WHERE account_type='WALLET')" -c "INSERT INTO bank.txn(txn_type,channel,amount) VALUES ('REVERSAL','WEB',50)" -c "UPDATE bank.ledger_entry SET amount = 1 WHERE entry_id = 1" -c "DELETE FROM bank.ledger_entry WHERE entry_id = 1"'''),
    ("portfolio-1-database-design", "P1_TransferProcedure",
     PSQL + ''' -c "\\sf bank.transfer_funds" '''),
    # ---------------- Portfolio 2 ----------------
    ("portfolio-2-query-optimisation", "P2_BeforeIndex",
     "sed -n '1,40p' portfolio-2-query-optimisation/results/q1_statement_before.txt"),
    ("portfolio-2-query-optimisation", "P2_AfterIndex",
     "sed -n '1,40p' portfolio-2-query-optimisation/results/q1_statement_after.txt"),
    ("portfolio-2-query-optimisation", "P2_LiveIndexedStatement",
     PSQL + ''' -c "EXPLAIN (ANALYZE, BUFFERS) SELECT l.created_at, l.direction, l.amount, l.balance_after FROM bank.ledger_entry l WHERE l.account_id = 4174 AND l.created_at >= '2026-04-01' AND l.created_at < '2026-10-01' ORDER BY l.created_at DESC LIMIT 50"'''),
    ("portfolio-2-query-optimisation", "P2_Summary",
     "cut -d, -f1-4 portfolio-2-query-optimisation/results/summary.csv | column -t -s,"),
    ("portfolio-2-query-optimisation", "P2_IndexSizes",
     PSQL + ''' -c "SELECT c.relname AS index_name, am.amname AS type, pg_size_pretty(pg_relation_size(c.oid)) AS size FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_am am ON am.oid=c.relam JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='bank' AND c.relname LIKE ANY (ARRAY['ix_%','brin_%']) ORDER BY pg_relation_size(c.oid) DESC"'''),
    # ---------------- Portfolio 3 ----------------
    ("portfolio-3-transactions-concurrency", "P3_CommitRollbackSavepoint",
     "sed -n '/^S1 /,/^S5 /p' portfolio-3-transactions-concurrency/results/transcript.txt"),
    ("portfolio-3-transactions-concurrency", "P3_LostUpdateAndFixes",
     "sed -n '/^S5 /,/^S9 /p' portfolio-3-transactions-concurrency/results/transcript.txt"),
    ("portfolio-3-transactions-concurrency", "P3_IsolationAndWriteSkew",
     "sed -n '/^S9 /,/^S11 /p' portfolio-3-transactions-concurrency/results/transcript.txt"),
    ("portfolio-3-transactions-concurrency", "P3_DeadlockAndIdempotency",
     "awk '/^S11 /{f=1} f' portfolio-3-transactions-concurrency/results/transcript.txt"),
    # ---------------- Portfolio 4 ----------------
    ("portfolio-4-nosql", "P4_ETLValidationIndexes",
     "sed -n '/## 1/,/## 4/p' portfolio-4-nosql/results/run_log.md"),
    ("portfolio-4-nosql", "P4_AggregationTransactionGraph",
     "sed -n '/## 4/,/## 7/p' portfolio-4-nosql/results/run_log.md"),
    ("portfolio-4-nosql", "P4_RedisAndStorage",
     "awk '/## 7/{f=1} f' portfolio-4-nosql/results/run_log.md"),
    ("portfolio-4-nosql", "P4_SampleCustomerDocument",
     "sed -n '1,46p' portfolio-4-nosql/results/sample_customer_document.json"),
    # ---------------- Portfolio 5 ----------------
    ("portfolio-5-distributed-cloud", "P5_ReplicationSetup",
     "sed -n '/## A/,/### A3/p' portfolio-5-distributed-cloud/results/run_log_replication.md"),
    ("portfolio-5-distributed-cloud", "P5_LagOutageFailover",
     "awk '/### A3/{f=1} f' portfolio-5-distributed-cloud/results/run_log_replication.md"),
    ("portfolio-5-distributed-cloud", "P5_CitusSharding",
     "awk '/## B/{f=1} f' portfolio-5-distributed-cloud/results/run_log_citus.md"),
    ("portfolio-5-distributed-cloud", "P5_MongoReplicaSetElection",
     "awk '/## C/{f=1} f' portfolio-5-distributed-cloud/results/run_log_mongo.md"),
]


def render(cmd, out, path):
    lines = []
    for raw in out.rstrip().splitlines() or ["(no output)"]:
        lines += textwrap.wrap(raw.expandtabs(4), WRAP, replace_whitespace=False, drop_whitespace=False) or [""]
    cut = len(lines) > MAXLINES
    lines = lines[:MAXLINES] + (["... (output truncated, full text in the repository)"] if cut else [])
    shown = cmd if len(cmd) <= WRAP - 2 else cmd[:WRAP - 5] + "..."
    w = 24 + int(FONT.getlength("M") * WRAP) + 24
    h = 40 + 30 + 20 * len(lines) + 24
    img = Image.new("RGB", (w, h), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, w, 36], fill=BAR)
    for i, c in enumerate([(237, 106, 94), (245, 191, 79), (98, 197, 84)]):
        d.ellipse([14 + i * 20, 12, 26 + i * 20, 24], fill=c)
    d.text((84, 10), "nairaflow-portfolio  |  terminal", font=FONT, fill=DIM)
    d.text((24, 48), "$ ", font=BOLD, fill=PROMPT)
    d.text((24 + FONT.getlength("$ "), 48), shown, font=BOLD, fill=FG)
    y = 48 + 30
    for ln in lines:
        col = (240, 128, 128) if ln.startswith(("ERROR", "psql: error")) else FG
        d.text((24, y), ln, font=FONT, fill=col)
        y += 20
    img.save(path, optimize=True)


def main():
    for folder, name, cmd in SHOTS:
        r = subprocess.run(["bash", "-c", cmd], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
        out = (r.stdout or "") + (r.stderr or "")
        d = ROOT / folder / "screenshots"
        d.mkdir(exist_ok=True)
        path = d / f"{name}.png"
        render(cmd.replace(PSQL, "psql nairaflow"), out, path)
        print(f"{path.relative_to(ROOT)}  ({len(out.splitlines())} lines)")


if __name__ == "__main__":
    main()
