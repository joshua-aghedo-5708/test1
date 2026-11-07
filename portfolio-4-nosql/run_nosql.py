"""
Portfolio 4: NairaFlow on MongoDB (document + graph traversal) and Redis (key-value).

    python portfolio-4-nosql/run_nosql.py

1. ETL: migrate customers/accounts and the last 3 months of transactions from PostgreSQL
2. Document model with $jsonSchema validation, embedded accounts, bucket pattern statements
3. Queries + explain("executionStats") before/after indexes (COLLSCAN vs IXSCAN)
4. Aggregation pipelines equivalent to Portfolio 2 SQL reports
5. Multi-document ACID transfer (commit + abort)
6. $graphLookup money-flow trace for fraud investigation (graph model)
7. Redis: cache-aside balances, idempotency keys, sliding-window velocity limit, leaderboard
Outputs go to results/.
"""
import datetime as dt, json, statistics, sys, time, uuid
from decimal import Decimal
from pathlib import Path

import psycopg
import redis
from bson.decimal128 import Decimal128
from pymongo import ASCENDING, DESCENDING, MongoClient, errors as merr

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from db import MONGO_URI, PG_DSN, REDIS_URL  # noqa: E402

HERE = Path(__file__).parent
OUT = HERE / "results"
OUT.mkdir(exist_ok=True)
REPORT = []
SINCE = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)


def say(s=""):
    print(s)
    REPORT.append(s)


def d128(x):
    return Decimal128(Decimal(x).quantize(Decimal("0.01")))


def dump(name, obj):
    (OUT / name).write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def ensure_replset(client):
    try:
        client.admin.command("replSetGetStatus")
    except merr.OperationFailure:
        client.admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": "localhost:27017"}]})
        for _ in range(60):
            time.sleep(1)
            if client.admin.command("hello").get("isWritablePrimary"):
                break


# ---------------------------------------------------------------------------
CUSTOMER_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "bvn", "name", "phone", "email", "kyc", "accounts", "status"],
        "properties": {
            "bvn": {"bsonType": "string", "pattern": "^[0-9]{11}$"},
            "phone": {"bsonType": "string", "pattern": "^\\+234[0-9]{10}$"},
            "email": {"bsonType": "string"},
            "status": {"enum": ["ACTIVE", "DORMANT", "BLOCKED", "CLOSED"]},
            "kyc": {"bsonType": "object", "required": ["tier", "daily_limit"],
                    "properties": {"tier": {"enum": [1, 2, 3]}}},
            "accounts": {
                "bsonType": "array", "minItems": 1, "maxItems": 5,
                "items": {"bsonType": "object",
                          "required": ["account_number", "type", "balance", "status"],
                          "properties": {
                              "account_number": {"bsonType": "string", "pattern": "^[0-9]{10}$"},
                              "type": {"enum": ["SAVINGS", "CURRENT", "WALLET"]},
                              "balance": {"bsonType": "decimal"},
                              "status": {"enum": ["ACTIVE", "FROZEN", "CLOSED"]}}}},
        },
    }
}

TXN_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "type", "amount", "status", "channel", "created_at"],
        "properties": {
            "type": {"enum": ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "REVERSAL", "FEE"]},
            "amount": {"bsonType": "decimal"},
            "status": {"enum": ["PENDING", "COMPLETED", "FAILED", "REVERSED"]},
            "legs": {"bsonType": "array", "maxItems": 2},
        },
    }
}


def etl(pg, db):
    say("## 1. ETL from PostgreSQL")
    t = time.time()
    for c in ("customers", "transactions", "statements"):
        db.drop_collection(c)
    db.create_collection("customers", validator=CUSTOMER_SCHEMA, validationLevel="strict")
    db.create_collection("transactions", validator=TXN_SCHEMA)
    db.create_collection("statements")

    cur = pg.cursor()
    cur.execute("""
        SELECT c.customer_id, c.bvn, c.first_name, c.last_name, c.email, c.phone, c.date_of_birth,
               s.state_name, r.region_name, b.branch_name, k.tier_id, k.daily_limit, c.status, c.created_at,
               json_agg(json_build_object('account_number', a.account_number, 'type', a.account_type,
                        'currency', a.currency, 'balance', a.balance::text, 'overdraft_limit', a.overdraft_limit::text,
                        'status', a.status, 'opened_at', extract(epoch FROM a.opened_at)) ORDER BY a.account_id) AS accounts,
               (SELECT json_agg(json_build_object('bank', bk.bank_name, 'account_number', be.account_number))
                  FROM bank.beneficiary be JOIN bank.bank bk USING (bank_code)
                 WHERE be.customer_id = c.customer_id) AS beneficiaries
          FROM bank.customer c
          JOIN bank.state s USING (state_code) JOIN bank.region r USING (region_code)
          JOIN bank.branch b ON b.branch_id = c.home_branch_id
          JOIN bank.kyc_tier k ON k.tier_id = c.kyc_tier_id
          JOIN bank.account a ON a.customer_id = c.customer_id
         WHERE c.customer_id > 1
         GROUP BY c.customer_id, s.state_name, r.region_name, b.branch_name, k.tier_id, k.daily_limit""")
    docs = []
    for r in cur:
        docs.append({
            "_id": r[0], "bvn": r[1], "name": {"first": r[2], "last": r[3]}, "email": r[4], "phone": r[5],
            "dob": dt.datetime.combine(r[6], dt.time()), "address": {"state": r[7], "region": r[8]},
            "home_branch": r[9], "kyc": {"tier": r[10], "daily_limit": d128(r[11])}, "status": r[12],
            "created_at": r[13],
            "accounts": [{**a, "balance": d128(a["balance"]), "overdraft_limit": d128(a["overdraft_limit"]),
                          "opened_at": dt.datetime.fromtimestamp(float(a["opened_at"]), dt.timezone.utc)} for a in r[14]],
            "beneficiaries": r[15] or [],
        })
    db.customers.insert_many(docs, ordered=False)

    cur.execute("""
        SELECT t.txn_id, t.reference, t.txn_type, t.channel, t.amount, t.status, t.narration,
               t.failure_reason, t.reverses_txn_id, t.created_at,
               json_agg(json_build_object('account_number', a.account_number, 'customer_id', a.customer_id,
                        'direction', l.direction, 'amount', l.amount::text, 'balance_after', l.balance_after::text))
                 FILTER (WHERE l.entry_id IS NOT NULL) AS legs
          FROM bank.txn t
          LEFT JOIN bank.ledger_entry l ON l.txn_id = t.txn_id
          LEFT JOIN bank.account a ON a.account_id = l.account_id
         WHERE t.created_at >= %s
         GROUP BY t.txn_id""", (SINCE,))
    txns, buckets = [], {}
    for r in cur:
        legs = [{**l, "amount": d128(l["amount"]), "balance_after": d128(l["balance_after"])} for l in (r[10] or [])]
        frm = next((l["account_number"] for l in legs if l["direction"] == "D"), None)
        to = next((l["account_number"] for l in legs if l["direction"] == "C"), None)
        txns.append({"_id": r[0], "reference": str(r[1]), "type": r[2], "channel": r[3], "amount": d128(r[4]),
                     "status": r[5], "narration": r[6], "failure_reason": r[7], "reverses": r[8],
                     "created_at": r[9], "from": frm, "to": to, "legs": legs})
        # bucket pattern: one statement document per account per month
        for l in legs:
            if l["customer_id"] == 1:
                continue
            key = (l["account_number"], r[9].strftime("%Y-%m"))
            b = buckets.setdefault(key, {"_id": f"{key[0]}:{key[1]}", "account_number": key[0], "month": key[1],
                                         "customer_id": l["customer_id"], "count": 0, "entries": []})
            b["entries"].append({"ts": r[9], "txn_id": r[0], "type": r[2], "dir": l["direction"],
                                 "amount": l["amount"], "balance_after": l["balance_after"]})
            b["count"] += 1
    for i in range(0, len(txns), 20000):
        db.transactions.insert_many(txns[i:i + 20000], ordered=False)
    for b in buckets.values():
        b["entries"].sort(key=lambda e: e["ts"])
        b["closing_balance"] = b["entries"][-1]["balance_after"]
    db.statements.insert_many(list(buckets.values()), ordered=False)
    say(f"Migrated {len(docs):,} customer documents (accounts embedded), {len(txns):,} transactions "
        f"(since {SINCE.date()}), {len(buckets):,} monthly statement buckets in {time.time() - t:.1f}s")
    sample = db.customers.find_one({"_id": 4242})
    dump("sample_customer_document.json", sample)
    dump("sample_transaction_document.json", db.transactions.find_one({"type": "TRANSFER", "status": "COMPLETED"}))
    sb = db.statements.find_one({"count": {"$gte": 5}})
    sb["entries"] = sb["entries"][:5] + [f"... {sb['count'] - 5} more"]
    dump("sample_statement_bucket.json", sb)
    say("Sample documents: `sample_customer_document.json`, `sample_transaction_document.json`, "
        "`sample_statement_bucket.json`")
    return sample


def validation_demo(db):
    say("\n## 2. Schema validation")
    bad = {"_id": 999999, "bvn": "123", "name": {"first": "X", "last": "Y"}, "email": "x@y.ng",
           "phone": "08012345678", "kyc": {"tier": 9, "daily_limit": d128(1)}, "accounts": [], "status": "ACTIVE"}
    try:
        db.customers.insert_one(bad)
        say("Invalid document was ACCEPTED (unexpected)")
    except merr.WriteError as e:
        rules = e.details.get("errInfo", {}).get("details", {}).get("schemaRulesNotSatisfied", [])
        props = [p.get("propertyName") for r in rules for p in r.get("propertiesNotSatisfied", [])]
        say(f"Invalid customer rejected by $jsonSchema (code {e.code}). Failing properties: {props}")


def explain_stats(db, coll, flt, sort=None, limit=None):
    cmd = {"find": coll, "filter": flt}
    if sort:
        cmd["sort"] = sort
    if limit:
        cmd["limit"] = limit
    ex = db.command("explain", cmd, verbosity="executionStats")
    s = ex["executionStats"]
    stage = ex["queryPlanner"]["winningPlan"]
    stages = []
    while stage:
        stages.append(stage.get("stage") + (f"({stage['indexName']})" if "indexName" in stage else ""))
        stage = stage.get("inputStage")
    return {"plan": " <- ".join(stages), "docsExamined": s["totalDocsExamined"],
            "keysExamined": s["totalKeysExamined"], "returned": s["nReturned"], "ms": s["executionTimeMillis"]}


def index_study(db, sample):
    say("\n## 3. Indexing study (explain executionStats)")
    busy = list(db.statements.aggregate([{"$group": {"_id": "$account_number", "n": {"$sum": "$count"}}},
                                          {"$sort": {"n": -1}}, {"$limit": 1}]))[0]
    acct = busy["_id"]
    say(f"Statement/bucket tests use the busiest customer account {acct} ({busy['n']} entries since Aug).\n")
    tests = [
        ("login by phone", "customers", {"phone": sample["phone"]}, None, None),
        ("find customer by account number (multikey)", "customers",
         {"accounts.account_number": sample["accounts"][0]["account_number"]}, None, None),
        ("account statement: last 20 txns for an account", "transactions", {"legs.account_number": acct},
         {"created_at": -1}, 20),
        ("failed transfers queue", "transactions", {"status": "FAILED", "type": "TRANSFER"}, {"created_at": -1}, 50),
    ]
    rows = []
    for label, coll, flt, sort, lim in tests:
        rows.append([label, explain_stats(db, coll, flt, sort, lim)])
    db.customers.create_index("phone", unique=True)
    db.customers.create_index("bvn", unique=True)
    db.customers.create_index("accounts.account_number", unique=True)       # multikey index on embedded array
    db.transactions.create_index([("legs.account_number", ASCENDING), ("created_at", DESCENDING)])
    db.transactions.create_index([("created_at", DESCENDING)], partialFilterExpression={"status": "FAILED"},
                                 name="failed_recent_partial")
    db.transactions.create_index([("status", ASCENDING), ("type", ASCENDING), ("created_at", DESCENDING)])
    db.transactions.create_index("reference", unique=True)
    db.transactions.create_index([("from", ASCENDING), ("created_at", ASCENDING)])
    db.statements.create_index([("account_number", ASCENDING), ("month", DESCENDING)])
    for i, (label, coll, flt, sort, lim) in enumerate(tests):
        rows[i].append(explain_stats(db, coll, flt, sort, lim))
    say("| Query | Before: plan | docs examined | ms | After: plan | keys / docs examined | ms |")
    say("|---|---|---:|---:|---|---:|---:|")
    for label, b, a in rows:
        say(f"| {label} | {b['plan']} | {b['docsExamined']:,} | {b['ms']} | {a['plan']} | "
            f"{a['keysExamined']:,} / {a['docsExamined']:,} | {a['ms']} |")
    dump("explain_index_study.json", rows)
    t = time.perf_counter()
    st = list(db.statements.find({"account_number": acct}).sort("month", -1).limit(3))
    say(f"\nBucket pattern: the last 3 monthly statements for {acct} = {len(st)} documents / "
        f"{sum(s['count'] for s in st)} entries in {1000 * (time.perf_counter() - t):.1f} ms (one indexed range read)")


def aggregations(db):
    say("\n## 4. Aggregation pipelines (equivalents of Portfolio 2 SQL reports)")
    pipe_daily = [
        {"$match": {"status": "COMPLETED", "created_at": {"$gte": dt.datetime(2026, 9, 1), "$lt": dt.datetime(2026, 10, 1)}}},
        {"$group": {"_id": {"day": {"$dateTrunc": {"date": "$created_at", "unit": "day"}}, "channel": "$channel"},
                    "n": {"$sum": 1}, "value": {"$sum": "$amount"}}},
        {"$sort": {"_id.day": 1, "_id.channel": 1}},
    ]
    t = time.perf_counter()
    daily = list(db.transactions.aggregate(pipe_daily))
    say(f"Daily volume by channel, Sept 2026: {len(daily)} groups in {1000 * (time.perf_counter() - t):.0f} ms")
    pipe_top = [
        {"$match": {"created_at": {"$gte": dt.datetime(2026, 8, 1), "$lt": dt.datetime(2026, 10, 1)},
                    "status": {"$in": ["COMPLETED", "REVERSED"]}}},
        {"$unwind": "$legs"},
        {"$match": {"legs.direction": "D", "legs.customer_id": {"$ne": 1}}},
        {"$group": {"_id": "$legs.customer_id", "outflow": {"$sum": "$legs.amount"}, "debits": {"$sum": 1}}},
        {"$sort": {"outflow": -1}}, {"$limit": 10},
        {"$lookup": {"from": "customers", "localField": "_id", "foreignField": "_id", "as": "c",
                     "pipeline": [{"$project": {"name": 1, "address.region": 1}}]}},
        {"$project": {"outflow": 1, "debits": 1, "name": {"$first": "$c.name"}, "region": {"$first": "$c.address.region"}}},
    ]
    t = time.perf_counter()
    top = list(db.transactions.aggregate(pipe_top))
    say(f"Top 10 spenders Aug-Sep 2026 ($unwind + $group + $lookup): {1000 * (time.perf_counter() - t):.0f} ms")
    say("| Customer | Region | Outflow (NGN) | Debits |")
    say("|---|---|---:|---:|")
    for r in top[:5]:
        say(f"| {r['name']['first']} {r['name']['last']} | {r['region']} | {r['outflow'].to_decimal():,.2f} | {r['debits']} |")
    dump("aggregation_daily_channel.json", daily[:12])
    dump("aggregation_top_spenders.json", top)
    (OUT / "pipelines.js").write_text(
        "// Aggregation pipelines run by run_nosql.py (mongosh syntax)\n"
        f"db.transactions.aggregate({json.dumps(pipe_daily, default=str, indent=2)})\n\n"
        f"db.transactions.aggregate({json.dumps(pipe_top, default=str, indent=2)})\n", encoding="utf-8")


def mongo_transfer(client, db, frm, to, amount, ref):
    """Multi-document ACID transfer: two customer docs + one transaction doc."""
    amt = Decimal(amount)

    def body(s):
        src = db.customers.find_one({"accounts.account_number": frm}, {"accounts.$": 1}, session=s)["accounts"][0]
        if src["status"] != "ACTIVE" or src["balance"].to_decimal() - amt < -src["overdraft_limit"].to_decimal():
            raise ValueError(f"insufficient funds: balance {src['balance']}, requested {amt}")
        db.customers.update_one({"accounts.account_number": frm},
                                {"$inc": {"accounts.$.balance": d128(-amt)}}, session=s)
        db.customers.update_one({"accounts.account_number": to},
                                {"$inc": {"accounts.$.balance": d128(amt)}}, session=s)
        db.transactions.insert_one({"_id": f"mongo-{ref}", "reference": ref, "type": "TRANSFER", "channel": "MOBILE",
                                    "amount": d128(amt), "status": "COMPLETED", "from": frm, "to": to,
                                    "created_at": dt.datetime.now(dt.timezone.utc)}, session=s)

    with client.start_session() as s:
        s.with_transaction(body)


def bal(db, nuban):
    return db.customers.find_one({"accounts.account_number": nuban}, {"accounts.$": 1})["accounts"][0]["balance"].to_decimal()


def transactions_demo(client, db):
    say("\n## 5. Multi-document ACID transaction (replica set rs0)")
    pair = list(db.customers.aggregate([{"$unwind": "$accounts"},
                                        {"$match": {"accounts.status": "ACTIVE", "accounts.balance": {"$gt": d128(60000)}}},
                                        {"$limit": 2}, {"$project": {"n": "$accounts.account_number"}}]))
    a, b = pair[0]["n"], pair[1]["n"]
    before = (bal(db, a), bal(db, b))
    mongo_transfer(client, db, a, b, "25000.00", str(uuid.uuid4()))
    after = (bal(db, a), bal(db, b))
    say(f"Commit: {a} {before[0]} -> {after[0]}, {b} {before[1]} -> {after[1]}")
    try:
        mongo_transfer(client, db, a, b, "999999999.00", str(uuid.uuid4()))
    except ValueError as e:
        say(f"Abort: {e}; balances unchanged: {(bal(db, a), bal(db, b)) == after}")
    ref = str(uuid.uuid4())
    mongo_transfer(client, db, a, b, "100.00", ref)
    try:
        mongo_transfer(client, db, a, b, "100.00", ref)
    except merr.DuplicateKeyError:
        say("Replay with the same reference -> DuplicateKeyError on unique index `reference`; "
            "the whole transaction (including the two $inc updates) was rolled back")
    say(f"Final: {a} = {bal(db, a)}, {b} = {bal(db, b)} (expected {after[0] - 100}, {after[1] + 100})")


def graph_demo(db):
    say("\n## 6. Graph traversal: $graphLookup money-flow trace (fraud investigation)")
    hub = list(db.transactions.aggregate([
        {"$match": {"type": "TRANSFER", "status": "COMPLETED", "created_at": {"$lt": dt.datetime(2026, 10, 20)}}},
        {"$group": {"_id": "$from", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}, {"$limit": 1}]))[0]["_id"]
    alert = db.transactions.find_one({"type": "TRANSFER", "status": "COMPLETED", "to": hub,
                                      "created_at": {"$lt": dt.datetime(2026, 10, 20)}},
                                     sort=[("amount", -1)])
    start = alert["to"]
    t0 = alert["created_at"]
    pipe = [
        {"$match": {"_id": alert["_id"]}},
        {"$graphLookup": {"from": "transactions", "startWith": "$to", "connectFromField": "to",
                          "connectToField": "from", "as": "hops", "maxDepth": 2, "depthField": "depth",
                          "restrictSearchWithMatch": {"type": "TRANSFER", "status": "COMPLETED",
                                                      "created_at": {"$gte": t0, "$lt": t0 + dt.timedelta(days=7)},
                                                      "amount": {"$gte": d128(10000)}}}},
        {"$project": {"amount": 1, "from": 1, "to": 1,
                      "hops": {"$map": {"input": "$hops", "as": "h",
                                        "in": {"depth": "$$h.depth", "from": "$$h.from", "to": "$$h.to",
                                               "amount": "$$h.amount", "at": "$$h.created_at"}}}}},
    ]
    t = time.perf_counter()
    res = list(db.transactions.aggregate(pipe))[0]
    ms = 1000 * (time.perf_counter() - t)
    by_depth = {}
    for h in res["hops"]:
        by_depth[h["depth"]] = by_depth.get(h["depth"], 0) + 1
    say(f"Flagged transfer {alert['_id']}: {alert['amount']} NGN {alert['from']} -> {start} at {t0:%Y-%m-%d %H:%M}")
    say(f"Onward transfers of >= 10,000 within 7 days, up to 3 hops: {dict(sorted(by_depth.items()))} "
        f"(depth 0 = direct onward transfers) in {ms:.0f} ms")
    dump("graph_trace.json", res)


def redis_demo(pg, db):
    say("\n## 7. Redis key-value patterns")
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.flushdb()
    cur = pg.cursor()
    cur.execute("SELECT account_number FROM bank.account WHERE account_id > 2 ORDER BY account_id LIMIT 500")
    nubans = [x[0] for x in cur.fetchall()]

    # (a) cache-aside balance lookups
    def get_balance(n):
        v = r.get(f"bal:{n}")
        if v is not None:
            return v, "hit"
        cur.execute("""SELECT a.balance, (SELECT count(*) FROM bank.ledger_entry l WHERE l.account_id=a.account_id)
                         FROM bank.account a WHERE a.account_number=%s""", (n,))
        v = str(cur.fetchone()[0])
        r.set(f"bal:{n}", v, ex=30)
        return v, "miss"

    miss, hit = [], []
    for n in nubans:
        t = time.perf_counter(); get_balance(n); miss.append(time.perf_counter() - t)
    for n in nubans:
        t = time.perf_counter(); get_balance(n); hit.append(time.perf_counter() - t)
    say(f"(a) Cache-aside balance (`bal:<nuban>`, TTL 30s): median miss (PostgreSQL) "
        f"{1000 * statistics.median(miss):.2f} ms vs hit (Redis) {1000 * statistics.median(hit):.2f} ms "
        f"over {len(nubans)} accounts. Invalidate with DEL on every posting.")

    # (b) idempotency keys
    ref = str(uuid.uuid4())
    first = r.set(f"idem:{ref}", "PROCESSING", nx=True, ex=86400)
    second = r.set(f"idem:{ref}", "PROCESSING", nx=True, ex=86400)
    say(f"(b) Idempotency `SET idem:<ref> NX EX 86400`: first request -> {first}, retried request -> {second}")

    # (c) sliding-window velocity rule: max 5 transfers per 60s per account
    def allow(n, now):
        key = f"vel:{n}"
        p = r.pipeline()
        p.zremrangebyscore(key, 0, now - 60)
        p.zadd(key, {str(uuid.uuid4()): now})
        p.zcard(key)
        p.expire(key, 120)
        return p.execute()[2] <= 5

    now = time.time()
    decisions = [allow(nubans[0], now + i) for i in range(8)]
    say(f"(c) Velocity limit (sorted-set sliding window, 5 per 60s): 8 rapid transfers -> {decisions}")

    # (d) real-time leaderboard of top receiving accounts (ZINCRBY)
    for t in db.transactions.find({"type": "TRANSFER", "status": "COMPLETED", "created_at": {"$gte": dt.datetime(2026, 10, 1)}},
                                  {"to": 1, "amount": 1}):
        r.zincrby("top_receivers:2026-10", float(t["amount"].to_decimal()), t["to"])
    top = r.zrevrange("top_receivers:2026-10", 0, 2, withscores=True)
    say(f"(d) Leaderboard `ZINCRBY top_receivers:2026-10`: top 3 = "
        + ", ".join(f"{k} ({v:,.0f} NGN)" for k, v in top))

    # (e) session store as a hash with TTL
    r.hset("session:abc123", mapping={"customer_id": 4242, "device": "android", "kyc_tier": 2})
    r.expire("session:abc123", 900)
    say(f"(e) Session hash `session:abc123`: {r.hgetall('session:abc123')}, TTL {r.ttl('session:abc123')}s")
    info = r.info("memory")
    say(f"Redis keys: {r.dbsize()}, memory used {info['used_memory_human']}")


def main():
    client = MongoClient(MONGO_URI)
    ensure_replset(client)
    db = client.nairaflow
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        say("# Portfolio 4 run log")
        say(f"_Generated by run_nosql.py on {dt.date.today()}_\n")
        sample = etl(pg, db)
        validation_demo(db)
        index_study(db, sample)
        aggregations(db)
        transactions_demo(client, db)
        graph_demo(db)
        redis_demo(pg, db)
        stats = {c: db.command("collstats", c) for c in ("customers", "transactions", "statements")}
        say("\n## Storage")
        say("| Collection | Documents | Avg doc size | Data size | Index size |")
        say("|---|---:|---:|---:|---:|")
        for c, s in stats.items():
            say(f"| {c} | {s['count']:,} | {s['avgObjSize']:,} B | {s['size'] / 1e6:.1f} MB | {s['totalIndexSize'] / 1e6:.1f} MB |")
    (OUT / "run_log.md").write_text("\n".join(REPORT) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
