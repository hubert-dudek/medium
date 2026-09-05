# Databricks notebook source
# MAGIC %md
# MAGIC # Tag Automations experiment — setup
# MAGIC
# MAGIC This notebook creates one schema, five small Delta tables, two governed tags, and a few seeded PII column tags.
# MAGIC
# MAGIC The schema is intentionally small so every dry-run result is easy to explain and screenshot.

# COMMAND ----------

from __future__ import annotations

import re
from collections.abc import Iterable

from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Existing catalog")
dbutils.widgets.text("schema_name", "tags_experiment", "Experiment schema")
dbutils.widgets.dropdown("reset_schema", "true", ["true", "false"], "Reset schema")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
reset_schema = dbutils.widgets.get("reset_schema").lower() == "true"

if not catalog_name:
    raise ValueError("catalog_name cannot be empty")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", schema_name):
    raise ValueError(
        "schema_name must start with a letter or underscore and contain only letters, digits, and underscores"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def qi(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def qname(*parts: str) -> str:
    return ".".join(qi(part) for part in parts)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_statements(statements: Iterable[str]) -> None:
    for statement in statements:
        spark.sql(statement)


def is_already_exists(exc: Exception) -> bool:
    text = str(exc).lower()
    return "already_exists" in text or ("already" in text and "exist" in text)


def ensure_governed_tag(
    tag_key: str,
    description: str,
    allowed_values: list[str] | None = None,
) -> None:
    values_clause = ""
    if allowed_values:
        values_clause = " VALUES (" + ", ".join(sql_string(v) for v in allowed_values) + ")"

    statement = (
        f"CREATE GOVERNED TAG {qi(tag_key)} "
        f"DESCRIPTION {sql_string(description)}{values_clause}"
    )

    try:
        spark.sql(statement)
        print(f"Created governed tag: {tag_key}")
    except Exception as exc:
        if not is_already_exists(exc):
            raise RuntimeError(
                f"Could not create governed tag {tag_key}. "
                "Run on Databricks Runtime 18.1+ and check account-level CREATE permission."
            ) from exc
        print(
            f"Governed tag already exists: {tag_key}. "
            "The setup leaves its existing definition unchanged."
        )


def set_key_only_column_tag(table_name: str, column_name: str, tag_key: str) -> None:
    target = qname(catalog_name, schema_name, table_name, column_name)
    try:
        spark.sql(f"SET TAG ON COLUMN {target} {qi(tag_key)}")
    except Exception as exc:
        raise RuntimeError(
            f"Could not assign {tag_key} to {table_name}.{column_name}. "
            "Check ASSIGN on the governed tag and APPLY TAG on the catalog objects."
        ) from exc


schema_qn = qname(catalog_name, schema_name)

print(f"Catalog: {catalog_name}")
print(f"Schema:  {schema_name}")
print(f"Reset:   {reset_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the governed tags
# MAGIC
# MAGIC - `ta_demo_pii` is deliberately **key-only**.
# MAGIC - `ta_demo_documentation` has one allowed value: `missing`.
# MAGIC
# MAGIC The deprecation rule uses Databricks' built-in `system.certification_status=deprecated`, so no third custom tag is needed.

# COMMAND ----------

spark.sql(f"DESCRIBE CATALOG {qi(catalog_name)}").collect()

ensure_governed_tag(
    "ta_demo_pii",
    "Demo key-only marker for assets that contain personally identifiable information.",
)
ensure_governed_tag(
    "ta_demo_documentation",
    "Demo documentation-state tag used by Tag Automations.",
    ["missing"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Recreate `tags_experiment`

# COMMAND ----------

if reset_schema:
    spark.sql(f"DROP SCHEMA IF EXISTS {schema_qn} CASCADE")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {schema_qn} "
    f"COMMENT {sql_string('Small deterministic dataset for governed Tag Automations testing.')}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create five small tables
# MAGIC
# MAGIC Two tables intentionally have no table description. One table intentionally contains `_legacy` in its name.

# COMMAND ----------

run_statements(
    [
        f"""
        CREATE OR REPLACE TABLE {qname(catalog_name, schema_name, 'customers')}
        USING DELTA
        AS
        SELECT * FROM VALUES
          (1L, 'Alice Meyer', 'alice.meyer@example.test', '+420 555 0101', 'CZ', DATE'2026-09-01'),
          (2L, 'Bruno Novak', 'bruno.novak@example.test', '+420 555 0102', 'CZ', DATE'2026-09-02'),
          (3L, 'Carla Rossi', 'carla.rossi@example.test', '+39 555 0103',  'IT', DATE'2026-09-03')
        AS t(customer_id, full_name, email_address, phone_number, country_code, created_date)
        """,
        f"COMMENT ON TABLE {qname(catalog_name, schema_name, 'customers')} "
        f"IS {sql_string('Customer contact master used for the PII roll-up example.')}",
        f"""
        CREATE OR REPLACE TABLE {qname(catalog_name, schema_name, 'orders')}
        USING DELTA
        AS
        SELECT * FROM VALUES
          (1001L, 1L, CAST(125.40 AS DECIMAL(12,2)), 'CZK', DATE'2026-09-01'),
          (1002L, 2L, CAST(89.10  AS DECIMAL(12,2)), 'CZK', DATE'2026-09-02'),
          (1003L, 3L, CAST(210.00 AS DECIMAL(12,2)), 'EUR', DATE'2026-09-03')
        AS t(order_id, customer_id, order_amount, currency, order_date)
        """,
        f"COMMENT ON TABLE {qname(catalog_name, schema_name, 'orders')} "
        f"IS {sql_string('Simple order facts without seeded PII column tags.')}",
        f"""
        CREATE OR REPLACE TABLE {qname(catalog_name, schema_name, 'products')}
        USING DELTA
        AS
        SELECT * FROM VALUES
          (101L, 'Reusable Bottle', 'Accessories', CAST(125.40 AS DECIMAL(12,2))),
          (202L, 'Notebook',        'Stationery',  CAST(89.10  AS DECIMAL(12,2))),
          (303L, 'Coffee Mug',      'Accessories', CAST(59.00  AS DECIMAL(12,2)))
        AS t(product_id, product_name, category_name, list_price)
        """,
        f"COMMENT ON TABLE {qname(catalog_name, schema_name, 'products')} "
        f"IS {sql_string('Product reference data used as a non-PII control table.')}",
        f"""
        CREATE OR REPLACE TABLE {qname(catalog_name, schema_name, 'support_cases')}
        USING DELTA
        AS
        SELECT * FROM VALUES
          ('CASE-001', 'alice.meyer@example.test', 'Delivery status',  'open',   TIMESTAMP'2026-09-03 08:00:00'),
          ('CASE-002', 'bruno.novak@example.test', 'Payment question', 'closed', TIMESTAMP'2026-09-03 08:15:00')
        AS t(case_id, requester_email, subject, status, created_at)
        """,
        f"""
        CREATE OR REPLACE TABLE {qname(catalog_name, schema_name, 'customer_export_legacy')}
        USING DELTA
        AS
        SELECT * FROM VALUES
          (9001L, 'Legacy Alice', 'legacy.alice@example.test', DATE'2019-12-31'),
          (9002L, 'Legacy Bruno', 'legacy.bruno@example.test', DATE'2019-12-31')
        AS t(export_id, full_name, email_address, export_date)
        """,
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Seed PII column tags
# MAGIC
# MAGIC Tag Automations evaluate metadata. They do not inspect the data values to discover PII. This setup therefore pre-tags a few columns, similar to metadata that could come from Data Classification or a manual classification process.

# COMMAND ----------

pii_columns = [
    ("customers", "full_name"),
    ("customers", "email_address"),
    ("customers", "phone_number"),
    ("support_cases", "requester_email"),
    ("customer_export_legacy", "full_name"),
    ("customer_export_legacy", "email_address"),
]

for table_name, column_name in pii_columns:
    set_key_only_column_tag(table_name, column_name, "ta_demo_pii")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dataset summary

# COMMAND ----------

expected_assets = spark.createDataFrame(
    [
        Row(
            table_name="customers",
            description_exists=True,
            seeded_pii_columns="full_name, email_address, phone_number",
            expected_pii_table_tag=True,
            expected_documentation_missing=False,
            expected_deprecated=False,
        ),
        Row(
            table_name="orders",
            description_exists=True,
            seeded_pii_columns="",
            expected_pii_table_tag=False,
            expected_documentation_missing=False,
            expected_deprecated=False,
        ),
        Row(
            table_name="products",
            description_exists=True,
            seeded_pii_columns="",
            expected_pii_table_tag=False,
            expected_documentation_missing=False,
            expected_deprecated=False,
        ),
        Row(
            table_name="support_cases",
            description_exists=False,
            seeded_pii_columns="requester_email",
            expected_pii_table_tag=True,
            expected_documentation_missing=True,
            expected_deprecated=False,
        ),
        Row(
            table_name="customer_export_legacy",
            description_exists=False,
            seeded_pii_columns="full_name, email_address",
            expected_pii_table_tag=True,
            expected_documentation_missing=True,
            expected_deprecated=True,
        ),
    ]
)
display(expected_assets)

seeded_tags = spark.sql(
    f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {qname(catalog_name, 'information_schema', 'column_tags')}
    WHERE schema_name = {sql_string(schema_name)}
      AND tag_name = 'ta_demo_pii'
    ORDER BY table_name, column_name
    """
)
display(seeded_tags)

rules = spark.createDataFrame(
    [
        Row(
            rule_order=1,
            automation="PII roll-up",
            condition="Any column has ta_demo_pii",
            action="Add key-only ta_demo_pii to the table",
            deterministic_matches=3,
        ),
        Row(
            rule_order=2,
            automation="Missing documentation",
            condition="Description does not exist",
            action="Add ta_demo_documentation=missing",
            deterministic_matches=2,
        ),
        Row(
            rule_order=3,
            automation="Deprecate stale assets",
            condition="Name contains _legacy OR last queried > 90 days",
            action="Add system.certification_status=deprecated",
            deterministic_matches=1,
        ),
    ]
)
display(rules.orderBy("rule_order"))

print("Setup complete. Open Catalog Explorer > Govern > Governed Tags > Automations.")
