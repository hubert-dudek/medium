# Databricks notebook source
import json

from pyspark.sql import functions as F

from cases import CASES

inputs = spark.createDataFrame(CASES, "case_name STRING, category STRING, sql_text STRING")
parsed = inputs.withColumn("parsed_json", F.expr("parse_sql(sql_text)"))
rows = parsed.collect()
results = {row.case_name: json.loads(row.parsed_json) if row.parsed_json is not None else None for row in rows}
display(parsed.orderBy("category", "case_name"))

# COMMAND ----------

summary = []
for row in rows:
    statements = results[row.case_name]
    if not statements:
        summary.append((row.case_name, row.category, None, "NULL" if statements is None else "EMPTY", None, None))
    for index, statement in enumerate(statements or [], start=1):
        summary.append((row.case_name, row.category, index, "PARSED" if statement["parse_success"] else "PARSE_ERROR", statement.get("statement_identifier"), statement.get("error", {}).get("errorClass")))

display(spark.createDataFrame(summary, "case_name STRING, category STRING, statement_number INT, result STRING, statement_type STRING, error_class STRING").orderBy("category", "case_name", "statement_number"))

# COMMAND ----------

assert results["null_input"] is None
assert results["empty_input"] == []
assert results["comments_only"] == []
assert results["malformed_select"][0]["parse_success"] is False
assert results["unresolved_table"][0]["parse_success"] is True
assert results["unresolved_function"][0]["parse_success"] is True
assert len(results["multiple_valid_statements"]) == 3
assert [s["parse_success"] for s in results["valid_invalid_valid"]] == [True, False, True]
assert len(results["comments_and_string_semicolons"]) == 1
assert results["nested_ctes"][0].get("source_table_references", []) == []
assert results["quoted_identifier_with_dot"][0]["source_table_references"] == [["demo_catalog", "sandbox", "orders.archive"]]
assert results["positional_parameters"][0]["parameter_markers"]["unnamed_count"] == 2
assert set(results["named_parameter"][0]["parameter_markers"]["named"]) == {"country", "minimum_amount"}

expected_valid = {name for name, _, _ in CASES} - {
    "null_input", "empty_input", "comments_only", "malformed_select", "valid_invalid_valid"
}
unexpected_errors = {
    name: results[name]
    for name in sorted(expected_valid)
    if not results[name] or not all(statement["parse_success"] for statement in results[name])
}
assert not unexpected_errors, json.dumps(unexpected_errors, indent=2)

# COMMAND ----------

unicode_sql = next(sql_text for case_name, _, sql_text in CASES if case_name == "unicode_offsets")
utf16 = unicode_sql.encode("utf-16-le")
fragments = []
for statement in results["unicode_offsets"]:
    start = (statement["start"] - 1) * 2
    end = start + statement["length"] * 2
    fragments.append(utf16[start:end].decode("utf-16-le"))
print(json.dumps(fragments, ensure_ascii=False, indent=2))
assert fragments == ["SELECT '😀' AS emoji", "SELECT 2 AS next_value"]

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "PASS", "input_count": len(CASES), "statement_count": sum(len(value or []) for value in results.values()), "results": results}))
