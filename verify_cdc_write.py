import json
import random
import struct
from pathlib import Path
from statistics import median, quantiles
from time import perf_counter_ns

import psycopg

rng = random.Random(20261125)
db = psycopg.connect("host=127.0.0.1 dbname=postgres user=postgres", autocommit=True, prepare_threshold=0)
assert db.execute("SHOW wal_level").fetchone()[0] == "logical"
assert db.execute("SHOW synchronous_commit").fetchone()[0] == "on"
assert db.execute("SHOW fsync").fetchone()[0] == "on"

tables = {"WalFull": "orders_wal", "Trigger": "orders"}
for table in tables.values():
    db.execute(f"CREATE TABLE public.{table} (order_id bigint PRIMARY KEY, tenant_id integer NOT NULL, amount_cents bigint NOT NULL)")
    db.execute(f"INSERT INTO public.{table} SELECT i, i%32, i*17 FROM generate_series(1,8192) AS i")
    db.execute(f"ANALYZE public.{table}")

db.execute(Path("topics/202611/lakebase_data_api/order_history.sql").read_text(), prepare=False)
db.execute("ALTER TABLE public.orders_wal REPLICA IDENTITY FULL")
db.execute("CREATE PUBLICATION order_publication FOR TABLE public.orders_wal")
db.execute("SELECT pg_create_logical_replication_slot('write_check', 'pgoutput')")
assert dict(db.execute("SELECT relname, relreplident FROM pg_class WHERE relname IN ('orders','orders_wal')").fetchall()) == {"orders": "d", "orders_wal": "f"}

for table in tables.values():
    plan = db.execute(f"EXPLAIN (FORMAT JSON) UPDATE public.{table} SET amount_cents=700 WHERE order_id=2500").fetchone()[0][0]["Plan"]
    assert plan["Plans"][0]["Node Type"] == "Index Scan"
    print("PRIMARY-KEY PLAN:", table, plan["Plans"][0]["Index Name"])

state = {tag: {} for tag in tables}
expected_changes = []
samples = {tag: [] for tag in tables}
rounds = 3
pairs_per_round = 160
preheat = 30
for sequence in range(preheat + rounds * pairs_per_round):
    key = 48 + rng.randrange(8000)
    old = state["WalFull"].get(key, key*17)
    assert old == state["Trigger"].get(key, key*17)
    new = 500000 + sequence
    expected_changes.append((key, key%32, old, new))
    order = list(tables)
    rng.shuffle(order)
    for tag in order:
        table = tables[tag]
        warm = db.execute(f"SELECT order_id, amount_cents FROM public.{table} WHERE order_id BETWEEN 37 AND 41 ORDER BY order_id DESC LIMIT 2").fetchall()
        assert warm == [(41,697), (40,680)]
        assert db.execute(f"SELECT amount_cents FROM public.{table} WHERE order_id=%s", (key,)).fetchone() == (old,)
        started = perf_counter_ns()
        returned = db.execute(f"UPDATE public.{table} SET amount_cents=%s WHERE order_id=%s RETURNING order_id,amount_cents", (new,key)).fetchone()
        elapsed = (perf_counter_ns()-started)/1e6
        assert returned == (key, new)
        state[tag][key] = new
        if sequence >= preheat:
            samples[tag].append(elapsed)

trigger_rows = db.execute("SELECT order_id,old_amount_cents,new_amount_cents,operation FROM public.orders_cdc ORDER BY event_id").fetchall()
assert trigger_rows == [(k, old, new, "UPDATE") for k, tenant, old, new in expected_changes]
wal_rows = db.execute("SELECT data FROM pg_logical_slot_get_binary_changes('write_check',NULL,NULL,'proto_version','1','publication_names','order_publication')").fetchall()

def tuple_fields(payload, offset):
    count = struct.unpack_from("!H", payload, offset)[0]
    offset += 2
    values = []
    for _ in range(count):
        assert payload[offset:offset+1] == b"t"
        length = struct.unpack_from("!I", payload, offset+1)[0]
        offset += 5
        values.append(int(payload[offset:offset+length]))
        offset += length
    return tuple(values), offset

decoded = []
for raw, in wal_rows:
    payload = bytes(raw)
    if payload[:1] == b"U":
        assert payload[5:6] == b"O"
        before, offset = tuple_fields(payload, 6)
        assert payload[offset:offset+1] == b"N"
        after, offset = tuple_fields(payload, offset+1)
        assert before[:2] == after[:2] and offset == len(payload)
        decoded.append((*before, after[-1]))
assert decoded == expected_changes
print("Validated before/after for every write from the pgoutput slot and trigger table:", len(decoded))
print("PostgreSQL:", db.execute("SHOW server_version").fetchone()[0])
evidence = {"engine":db.execute("SELECT version()").fetchone()[0], "validated_updates_per_route":len(decoded), "warmups_per_route":preheat, "measured_updates_per_route":rounds*pairs_per_round, "raw_ms":samples}
for tag, times in samples.items():
    by_pass = [round(median(times[i:i+pairs_per_round]),6) for i in range(0,len(times),pairs_per_round)]
    p50 = round(median(times), 6)
    p95 = round(quantiles(times, n=100, method="inclusive")[94],6)
    evidence[tag] = {"median_ms":p50,"p95_ms":p95,"pass_medians_ms":by_pass}
    print("RESULT",tag,"n",len(times),"median_ms",p50,"p95_ms",p95,"pass_medians_ms",by_pass)
Path("write-timings.json").write_text(json.dumps(evidence,indent=2))
db.close()
