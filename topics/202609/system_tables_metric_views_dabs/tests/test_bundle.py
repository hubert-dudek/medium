"""Offline structural and reference-fixture tests, not a Databricks SQL/CLI integration test."""
import ast
import calendar
import copy
import json
import re
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

def metric(path):
    text = (SQL / path).read_text()
    body = re.search(r"AS \$\$\s*(.*?)\s*\$\$", text, re.S)
    if not body:
        raise AssertionError(f"No metric YAML in {path}")
    return yaml.safe_load(body.group(1))

def fiscal(d):
    year = d.year if d.month >= 3 else d.year - 1
    month = ((d.month - 3) % 12) + 1
    return year, month, (month - 1) // 3 + 1

def add_months(d, n):
    y, m = divmod(d.year * 12 + d.month - 1 + n, 12)
    return date(y, m + 1, min(d.day, calendar.monthrange(y, m + 1)[1]))

def price_for(event, prices):
    matches = [p for p in prices
               if all(event[k] == p[k] for k in ["account", "sku", "cloud", "unit"])
               and p["currency"] == "USD" and event["start"] >= p["start"]
               and (p["end"] is None or (event["start"] < p["end"] and event["end"] <= p["end"]))]
    if len(matches) > 1:
        raise ValueError("Overlapping price intervals")
    return matches[0]["price"] if matches else None

def total(events, prices):
    subtotal = Decimal(0)
    missing = 0
    for event in events:
        price = price_for(event, prices)
        if event["quantity"] == 0:
            continue
        if price is None:
            missing += 1
        else:
            subtotal += event["quantity"] * price
    return (subtotal if missing == 0 else None), subtotal, missing

class StructureTests(unittest.TestCase):
    def setUp(self):
        self.bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
        self.resources = {}
        for path in (ROOT / "resources").glob("*.yml"):
            for kind, values in yaml.safe_load(path.read_text())["resources"].items():
                self.resources.setdefault(kind, {}).update(values)

    def test_all_yaml_json_and_python_parse(self):
        for path in ROOT.rglob("*.yml"):
            with self.subTest(path=str(path)): yaml.safe_load(path.read_text())
        for path in ROOT.rglob("*.json"):
            with self.subTest(path=str(path)): json.loads(path.read_text())
        for path in ROOT.rglob("*.py"):
            with self.subTest(path=str(path)): ast.parse(path.read_text(), filename=str(path))

    def test_exact_native_schemas(self):
        schemas = self.resources["schemas"]
        self.assertEqual(set(schemas), {"system_tables_metrics", "system_tables_metrics_materialized"})
        for key, s in schemas.items():
            self.assertEqual(s["catalog_name"], "main")
            self.assertEqual(s["name"], key)
            self.assertTrue(s["lifecycle"]["prevent_destroy"])
        self.assertNotIn("mode", self.bundle["targets"]["main"])

    def test_warehouse_lookup_and_direct_deploy_run(self):
        self.assertEqual(self.bundle["variables"]["warehouse_id"]["lookup"]["warehouse"], "SQL warehouse")
        self.assertEqual(self.bundle["bundle"]["engine"], "direct")
        self.assertIn("1.13.0", self.bundle["bundle"]["databricks_cli_version"])
        run = self.resources["job_runs"]["deploy_metric_views_on_deploy"]
        self.assertEqual(run["job_id"], "${resources.jobs.deploy_metric_views.id}")
        self.assertEqual(run["lifecycle"]["triggers"], [{"on_bundle_deploy": True}])

    def test_no_invented_metric_resource_or_duplicate_pipeline(self):
        self.assertEqual(set(self.resources), {"schemas", "jobs", "job_runs", "dashboards"})

    def test_deployment_tasks_have_real_sql_only_notebooks(self):
        tasks = self.resources["jobs"]["deploy_metric_views"]["tasks"]
        self.assertEqual(len(tasks), 9)
        seen = set()
        for task in tasks:
            self.assertNotIn(task["task_key"], seen)
            for dep in task.get("depends_on", []):
                self.assertIn(dep["task_key"], seen)
            seen.add(task["task_key"])
            n = task["notebook_task"]
            path = (ROOT / "resources" / n["notebook_path"]).resolve()
            self.assertTrue(path.is_file())
            text = path.read_text()
            self.assertTrue(text.startswith("-- Databricks notebook source"))
            self.assertNotIn("-- MAGIC %python", text)
            self.assertNotIn("-- MAGIC %md", text)
            self.assertEqual(n["warehouse_id"], "${var.warehouse_id}")
            self.assertRegex(n["base_parameters"]["schema"], r"\$\{resources\.schemas\.[a-z_]+\.name\}")
            self.assertNotIn("new_cluster", task)
            self.assertNotIn("job_cluster_key", task)

    def test_six_persistent_metric_definitions(self):
        files = [p for p in SQL.glob("*.sql") if p.name not in {"00_preflight.sql", "01_warehouse_latest.sql", "08_smoke_tests.sql"}]
        self.assertEqual(len(files), 6)
        for p in files:
            m = metric(p.name)
            self.assertEqual(str(m["version"]), "1.1")
            dims = [d["name"] for d in m["dimensions"]]
            measures = [d["name"] for d in m["measures"]]
            self.assertEqual(len(dims), len(set(dims)))
            self.assertEqual(len(measures), len(set(measures)))
            self.assertFalse(set(dims) & set(measures))
            known = set()
            for measure in m["measures"]:
                refs = re.findall(r"MEASURE\((\w+)\)", measure["expr"])
                self.assertTrue(set(refs).issubset(known), (p.name, measure["name"], refs))
                known.add(measure["name"])
            self.assertNotIn("parameters", m)

    def test_source_columns_and_full_interval_price_keys(self):
        m = metric("02_billing.sql")
        self.assertEqual(m["source"], "system.billing.usage")
        price = m["joins"][0]
        for key in ["account_id", "sku_name", "cloud", "usage_unit"]:
            self.assertIn(f"source.{key} = prices.{key}", price["on"])
        self.assertIn("currency_code = 'USD'", price["source"])
        self.assertIn("pricing.effective_list.default", price["source"])
        self.assertIn("source.usage_start_time >= prices.price_start_time", price["on"])
        self.assertIn("source.usage_start_time < prices.price_end_time", price["on"])
        self.assertIn("source.usage_end_time <= prices.price_end_time", price["on"])
        self.assertNotIn("filter", m)  # Do not remove retractions or pre-filter fiscal history.

    def test_cost_guard_and_single_unit_guard(self):
        measures = {m["name"]: m for m in metric("02_billing.sql")["measures"]}
        self.assertIn("CAST(source.usage_quantity AS DECIMAL(28,9)) * prices.unit_price_usd", measures["priced_cost_usd"]["expr"])
        self.assertIn("MEASURE(unpriced_record_count) = 0", measures["spend_usd"]["expr"])
        self.assertIn("source.usage_unit = 'DBU'", measures["dbu_usage"]["expr"])
        self.assertIn("COUNT(DISTINCT source.usage_unit) = 1", measures["usage_quantity_single_unit"]["expr"])

    def test_materialized_billing_reuses_canonical_semantics(self):
        base, mirror = metric("02_billing.sql"), metric("06_billing_materialized.sql")
        self.assertEqual(mirror["source"], "main.system_tables_metrics.billing")
        self.assertEqual([x["name"] for x in base["dimensions"]], [x["name"] for x in mirror["dimensions"]])
        for m in mirror["measures"]:
            self.assertEqual(m["expr"], f'MEASURE(source.{m["name"]})')
        self.assertEqual([x["name"] for x in base["measures"]], [x["name"] for x in mirror["measures"]])
        self.assertNotIn("pricing.effective_list.default", str(mirror))

    def test_monthly_semantics_identical_between_schemas(self):
        live, materialized = metric("03_billing_monthly.sql"), metric("07_billing_monthly_materialized.sql")
        materialized.pop("materialization")
        live.pop("comment"); materialized.pop("comment")
        self.assertEqual(live, materialized)
        self.assertIn("main.system_tables_metrics.billing", live["source"])
        self.assertNotIn("system_tables_metrics_materialized", live["source"])

    def test_materialization_names_grains_and_measures_valid(self):
        for file in ["06_billing_materialized.sql", "07_billing_monthly_materialized.sql"]:
            m = metric(file)
            self.assertEqual(m["materialization"]["mode"], "relaxed")
            self.assertEqual(m["materialization"]["schedule"], "every 6 hours")
            dims = {d["name"] for d in m["dimensions"]}
            measures = {d["name"] for d in m["measures"]}
            for mat in m["materialization"]["materialized_views"]:
                self.assertEqual(mat["type"], "aggregated")
                self.assertTrue(set(mat["dimensions"]) <= dims)
                self.assertTrue(set(mat["measures"]) <= measures)
                self.assertIn("unpriced_record_count", mat["measures"])
                self.assertNotIn("fiscal_ytd_spend_usd", mat["measures"])

    def test_offset_and_fiscal_window_structure(self):
        m = metric("03_billing_monthly.sql")
        measures = {x["name"]: x for x in m["measures"]}
        self.assertEqual(measures["previous_month_priced_cost_usd"]["window"][0]["offset"], "-1 month")
        self.assertEqual(measures["previous_year_priced_cost_usd"]["window"][0]["offset"], "-12 month")
        self.assertEqual(measures["fiscal_ytd_priced_cost_usd"]["window"][1]["order"], "fiscal_year_start")
        dims = {x["name"]: x["expr"] for x in m["dimensions"]}
        self.assertIn("ADD_MONTHS(usage_month, -2)", dims["fiscal_year_start"])
        self.assertNotIn("filter", m)

    def test_metric_join_is_preaggregated_and_preserves_keys(self):
        source = metric("05_warehouse_efficiency.sql")["source"]
        self.assertEqual(source.count("GROUP BY account_id, workspace_id, warehouse_id"), 2)
        self.assertIn("main.system_tables_metrics.billing", source)
        self.assertIn("main.system_tables_metrics.warehouse_queries", source)
        self.assertIn("FULL OUTER JOIN queries", source)
        self.assertNotIn("JOIN system.query.history", source)
        for key in ["account_id", "workspace_id", "warehouse_id", "day"]:
            self.assertIn(f"c.{key} = q.{key}", source)

    def test_no_query_history_materialization_or_sensitive_fields(self):
        for file in ["04_warehouse_queries.sql", "05_warehouse_efficiency.sql"]:
            m = metric(file)
            self.assertNotIn("materialization", m)
            self.assertNotIn("statement_text", m["source"])
            self.assertNotIn("executed_by", m["source"])
        self.assertNotIn("tags", (SQL / "01_warehouse_latest.sql").read_text().split("AS\nSELECT", 1)[1])

    def test_single_dashboard_real_dataset(self):
        res = self.resources["dashboards"]["billing_fiscal_ytd"]
        path = (ROOT / "resources" / res["file_path"]).resolve()
        dashboard = json.loads(path.read_text())
        self.assertFalse(res["embed_credentials"])
        self.assertEqual(len(dashboard["datasets"]), 1)
        self.assertEqual(len(dashboard["pages"]), 1)
        page = dashboard["pages"][0]
        self.assertEqual(len(page["layout"]), 1)
        widget = page["layout"][0]["widget"]
        self.assertEqual(widget["spec"]["widgetType"], "bar")
        self.assertEqual(widget["queries"][0]["query"]["datasetName"], dashboard["datasets"][0]["name"])
        sql = dashboard["datasets"][0]["query"]
        self.assertIn("main.system_tables_metrics_materialized.billing", sql)
        self.assertIn("MEASURE(priced_cost_usd)", sql)
        self.assertIn("MEASURE(unpriced_record_count)", sql)
        self.assertIn("MAKE_DATE", sql)
        self.assertIn("'UTC'", sql)

    def test_parameters_are_opt_in_native_and_not_materialized(self):
        code = (ROOT / "notebooks/90_native_parameters_lab.py").read_text()
        self.assertIn('"run_parameter_lab", "false"', code)
        self.assertIn("CREATE OR REPLACE TEMPORARY VIEW", code)
        self.assertIn("parameters:", code)
        self.assertIn("discount_rate => :discount", code)
        self.assertIn("offset: comparison_months month", code)
        self.assertNotIn("materialization:", code)
        self.assertNotIn("except Exception", code)
        native_yaml = re.search(r"AS \$\$\s*(.*?)\s*\$\$", code, re.S)
        lab = yaml.safe_load(native_yaml.group(1))
        self.assertEqual([x["name"] for x in lab["parameters"]], ["discount_rate", "comparison_months"])
        self.assertEqual(lab["parameters"][1]["default"], -12)

    def test_engineering_identifiers_are_safe_and_schema_explicit(self):
        code = (ROOT / "notebooks/10_engineering_examples.py").read_text()
        self.assertIn('dbutils.widgets.text("schema",', code)
        self.assertIn("FROM IDENTIFIER(:billing_view)", code)
        self.assertIn("args=args", code)
        self.assertIn("GROUP BY usage_month\n)\nSELECT * FROM history\nWHERE usage_month", code)
        self.assertNotIn('spark.table(', code)

    def test_manual_refresh_is_opt_in(self):
        code = (ROOT / "notebooks/20_materialization.py").read_text()
        self.assertIn('"refresh_now", "false"', code)
        self.assertIn("EXPLAIN EXTENDED", code)
        self.assertIn('if dbutils.widgets.get("refresh_now") == "true":', code)

class FiscalFixtureTests(unittest.TestCase):
    def test_boundaries(self):
        cases = {"2024-02-29": (2023,12,4), "2024-03-01": (2024,1,1),
                 "2026-05-31": (2026,3,1), "2026-06-01": (2026,4,2),
                 "2026-08-31": (2026,6,2), "2026-09-01": (2026,7,3),
                 "2026-11-30": (2026,9,3), "2026-12-01": (2026,10,4),
                 "2027-02-28": (2026,12,4), "2027-03-01": (2027,1,1)}
        for d, expected in cases.items():
            with self.subTest(date=d): self.assertEqual(fiscal(date.fromisoformat(d)), expected)

    def test_ytd_on_march_first_is_empty(self):
        cutoff = date(2026,3,1)
        fy_start = date(fiscal(cutoff)[0],3,1)
        self.assertEqual(fy_start, cutoff)
        self.assertFalse(fy_start <= date(2026,2,28) < cutoff)

    def test_month_offsets_and_leap_alignment(self):
        self.assertEqual(add_months(date(2026,3,1),-1),date(2026,2,1))
        self.assertEqual(add_months(date(2026,3,1),-12),date(2025,3,1))
        self.assertEqual(add_months(date(2024,2,29),-12),date(2023,2,28))

    def test_fixture_expected_offset_and_fiscal_ytd(self):
        values = {date(2025,3,1):100, date(2026,2,1):120, date(2026,3,1):150}
        anchor = date(2026,3,1)
        self.assertEqual(values.get(add_months(anchor,-12)), 100)
        self.assertEqual(values.get(add_months(anchor,-1)), 120)
        self.assertIsNone(values.get(date(2025,2,1)))
        self.assertEqual(sum(v for d,v in values.items() if d <= anchor and fiscal(d)[0] == fiscal(anchor)[0]),150)

class BillingFixtureTests(unittest.TestCase):
    def setUp(self):
        self.price = dict(account="a", sku="sku", cloud="AWS", unit="DBU", currency="USD",
                          start=datetime(2026,1,1), end=None, price=Decimal("2.00"))
        self.event = dict(account="a", sku="sku", cloud="AWS", unit="DBU", quantity=Decimal(10),
                          start=datetime(2026,3,1,10), end=datetime(2026,3,1,11))

    def test_signed_corrections(self):
        events = [{**self.event,"quantity":q,"record_type":r} for q,r in
                  [(Decimal(10),"ORIGINAL"),(Decimal(-10),"RETRACTION"),(Decimal(8),"RESTATEMENT")]]
        self.assertEqual(total(events,[self.price]),(Decimal(16),Decimal(16),0))

    def test_full_interval_and_price_boundary(self):
        boundary = datetime(2026,3,1,11)
        prices = [{**self.price,"end":boundary}, {**self.price,"start":boundary,"price":Decimal(3)}]
        self.assertEqual(price_for(self.event,prices),Decimal(2))
        after = {**self.event,"start":boundary,"end":datetime(2026,3,1,12)}
        self.assertEqual(price_for(after,prices),Decimal(3))
        crossing = {**self.event,"end":datetime(2026,3,1,12)}
        self.assertIsNone(price_for(crossing,prices))

    def test_missing_price_is_not_zero(self):
        unpriced = {**self.event,"sku":"missing"}
        self.assertEqual(total([self.event,unpriced],[self.price]),(None,Decimal(20),1))

    def test_zero_quantity_does_not_create_missing_spend(self):
        zero = {**self.event,"sku":"missing","quantity":Decimal(0)}
        self.assertEqual(total([zero],[self.price]),(Decimal(0),Decimal(0),0))

    def test_currency_unit_cloud_and_account_are_not_interchangeable(self):
        for field,value in [("currency","EUR"),("unit","TOKEN"),("cloud","GCP"),("account","other")]:
            with self.subTest(field=field):
                self.assertIsNone(price_for(self.event,[{**self.price,field:value}]))

    def test_overlaps_rejected(self):
        with self.assertRaises(ValueError):
            price_for(self.event,[self.price,copy.deepcopy(self.price)])

    def test_fact_to_fact_join_needs_preaggregation(self):
        billing_amounts=[Decimal(3),Decimal(7)]
        statements=["q1","q2","q3"]
        wrong=sum(amount for amount in billing_amounts for _ in statements)
        correct=sum(billing_amounts)
        self.assertEqual(wrong,Decimal(30))
        self.assertEqual(correct,Decimal(10))
        self.assertEqual(len(statements),3)

if __name__ == "__main__":
    unittest.main()
