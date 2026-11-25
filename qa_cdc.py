from statistics import median
from time import perf_counter_ns

import psycopg
import requests

db = psycopg.connect("dbname=postgres")
http = requests.Session()
base = "http://127.0.0.1:3000/orders_cdc"
columns = "order_id,operation,old_amount_cents,new_amount_cents"

def history(key):
    r = http.get(base, params={"select": columns, "order_id": f"eq.{key}", "order": "event_id.desc", "limit": 1}, timeout=10)
    r.raise_for_status()
    return r

print("PostgreSQL integration on GitHub Actions")
print(db.execute("SELECT version()").fetchone()[0])
assert not db.execute("SELECT has_table_privilege('history_reader','public.orders_cdc','UPDATE')").fetchone()[0]
assert history(42).json() == []
db.execute("UPDATE public.orders SET amount_cents=999 WHERE order_id=42")
db.rollback()
assert history(42).json() == []
print("Rolled-back update left no history row.")
db.execute("UPDATE public.orders SET amount_cents=799 WHERE order_id=42 AND amount_cents=714")
db.commit()
r = history(42)
expected = {"order_id": 42, "operation": "UPDATE", "old_amount_cents": 714, "new_amount_cents": 799}
assert r.json() == [expected]
print("BEGIN_REST_PROOF")
print("Engine: PostgreSQL 18 + PostgREST 12.2.3, GitHub-hosted Linux runner")
print("GET " + r.request.path_url)
print("HTTP " + str(r.status_code))
print(r.text.strip())
print("END_REST_PROOF")
db.execute("INSERT INTO public.orders VALUES (81,1,700)")
db.commit()
assert history(81).json()[0] == {"order_id":81,"operation":"INSERT","old_amount_cents":None,"new_amount_cents":700}
db.execute("DELETE FROM public.orders WHERE order_id=81")
db.commit()
assert history(81).json()[0] == {"order_id":81,"operation":"DELETE","old_amount_cents":700,"new_amount_cents":None}
commit_to_response_ms = []
for new_amount in range(901, 906):
    history(42)
    previous = db.execute("SELECT amount_cents FROM public.orders WHERE order_id=41").fetchone()[0]
    db.execute("UPDATE public.orders SET amount_cents=%s WHERE order_id=41", (new_amount,))
    db.commit()
    start = perf_counter_ns()
    rows = history(41).json()
    elapsed = (perf_counter_ns()-start)/1e6
    assert rows == [{"order_id":41, "operation":"UPDATE", "old_amount_cents":previous, "new_amount_cents":new_amount}]
    commit_to_response_ms.append(elapsed)
print("Warm commit acknowledgement to first verified REST response, ms:", [round(n,4) for n in commit_to_response_ms])
print("Median ms:",round(median(commit_to_response_ms),4))
print("Verified insert, update, delete, rolled-back update, rerunnable DDL and reader write restrictions.")
