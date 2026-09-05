# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect and verify the Tag Automations experiment
# MAGIC
# MAGIC Run this notebook once immediately after setup for the **before** screenshots and again after enabling the three automations for the **after** screenshots.

# COMMAND ----------

from __future__ import annotations

import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Existing catalog")
dbutils.widgets.text("schema_name", "tags_experiment", "Experiment schema")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()

if not catalog_name:
    raise ValueError("catalog_name cannot be empty")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", schema_name):
    raise ValueError("Invalid schema_name")


def qi(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def qname(*parts: str) -> str:
    return ".".join(qi(part) for part in parts)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table inventory and descriptions

# COMMAND ----------

inventory = spark.sql(
    f"""
    SELECT
      table_name,
      table_type,
      comment,
      CASE WHEN table_name IN ('customers', 'support_cases', 'customer_export_legacy')
           THEN true ELSE false END AS expect_pii_table_tag,
      CASE WHEN table_name IN ('support_cases', 'customer_export_legacy')
           THEN true ELSE false END AS expect_documentation_missing,
      CASE WHEN table_name = 'customer_export_legacy'
           THEN true ELSE false END AS expect_deprecated_from_name
    FROM {qname(catalog_name, 'information_schema', 'tables')}
    WHERE table_schema = {sql_string(schema_name)}
      AND table_name IN (
        'customers',
        'orders',
        'products',
        'support_cases',
        'customer_export_legacy'
      )
    ORDER BY table_name
    """
)
display(inventory)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seeded column tags

# COMMAND ----------

column_tags = spark.sql(
    f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {qname(catalog_name, 'information_schema', 'column_tags')}
    WHERE schema_name = {sql_string(schema_name)}
      AND tag_name = 'ta_demo_pii'
    ORDER BY table_name, column_name
    """
)
display(column_tags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Current table tags

# COMMAND ----------

table_tags = spark.sql(
    f"""
    SELECT table_name, tag_name, tag_value
    FROM {qname(catalog_name, 'information_schema', 'table_tags')}
    WHERE schema_name = {sql_string(schema_name)}
      AND tag_name IN (
        'ta_demo_pii',
        'ta_demo_documentation',
        'system.certification_status'
      )
    ORDER BY table_name, tag_name
    """
)
display(table_tags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Expected versus actual state

# COMMAND ----------

verification = spark.sql(
    f"""
    WITH expected(table_name, expect_pii, expect_missing_docs, expect_deprecated) AS (
      SELECT * FROM VALUES
        ('customers',              true,  false, false),
        ('orders',                 false, false, false),
        ('products',               false, false, false),
        ('support_cases',          true,  true,  false),
        ('customer_export_legacy', true,  true,  true)
    ),
    actual AS (
      SELECT
        table_name,
        MAX(CASE WHEN tag_name = 'ta_demo_pii' THEN 1 ELSE 0 END) AS has_pii,
        MAX(CASE WHEN tag_name = 'ta_demo_documentation' AND tag_value = 'missing' THEN 1 ELSE 0 END) AS has_missing_docs,
        MAX(CASE WHEN tag_name = 'system.certification_status' AND tag_value = 'deprecated' THEN 1 ELSE 0 END) AS is_deprecated
      FROM {qname(catalog_name, 'information_schema', 'table_tags')}
      WHERE schema_name = {sql_string(schema_name)}
      GROUP BY table_name
    )
    SELECT
      e.table_name,
      e.expect_pii,
      COALESCE(a.has_pii, 0) = 1 AS actual_pii,
      CASE
        WHEN (e.expect_pii AND COALESCE(a.has_pii, 0) = 1)
          OR (NOT e.expect_pii AND COALESCE(a.has_pii, 0) = 0) THEN 'PASS'
        WHEN e.expect_pii THEN 'PENDING'
        ELSE 'UNEXPECTED'
      END AS pii_check,
      e.expect_missing_docs,
      COALESCE(a.has_missing_docs, 0) = 1 AS actual_missing_docs,
      CASE
        WHEN (e.expect_missing_docs AND COALESCE(a.has_missing_docs, 0) = 1)
          OR (NOT e.expect_missing_docs AND COALESCE(a.has_missing_docs, 0) = 0) THEN 'PASS'
        WHEN e.expect_missing_docs THEN 'PENDING'
        ELSE 'UNEXPECTED'
      END AS documentation_check,
      e.expect_deprecated,
      COALESCE(a.is_deprecated, 0) = 1 AS actual_deprecated,
      CASE
        WHEN (e.expect_deprecated AND COALESCE(a.is_deprecated, 0) = 1)
          OR (NOT e.expect_deprecated AND COALESCE(a.is_deprecated, 0) = 0) THEN 'PASS'
        WHEN e.expect_deprecated THEN 'PENDING'
        ELSE 'UNEXPECTED'
      END AS deprecation_check
    FROM expected e
    LEFT JOIN actual a USING (table_name)
    ORDER BY e.table_name
    """
)
verification.createOrReplaceTempView("tag_automation_verification")
display(verification)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compact article-ready summary

# COMMAND ----------

summary = spark.sql(
    """
    SELECT
      'PII roll-up' AS automation,
      SUM(CASE WHEN expect_pii THEN 1 ELSE 0 END) AS expected_matches,
      SUM(CASE WHEN actual_pii THEN 1 ELSE 0 END) AS actual_matches,
      CASE WHEN SUM(CASE WHEN expect_pii THEN 1 ELSE 0 END)
              = SUM(CASE WHEN actual_pii THEN 1 ELSE 0 END)
           THEN 'PASS' ELSE 'PENDING' END AS status
    FROM tag_automation_verification

    UNION ALL

    SELECT
      'Missing documentation',
      SUM(CASE WHEN expect_missing_docs THEN 1 ELSE 0 END),
      SUM(CASE WHEN actual_missing_docs THEN 1 ELSE 0 END),
      CASE WHEN SUM(CASE WHEN expect_missing_docs THEN 1 ELSE 0 END)
              = SUM(CASE WHEN actual_missing_docs THEN 1 ELSE 0 END)
           THEN 'PASS' ELSE 'PENDING' END
    FROM tag_automation_verification

    UNION ALL

    SELECT
      'Deprecated by _legacy name',
      SUM(CASE WHEN expect_deprecated THEN 1 ELSE 0 END),
      SUM(CASE WHEN actual_deprecated THEN 1 ELSE 0 END),
      CASE WHEN SUM(CASE WHEN expect_deprecated THEN 1 ELSE 0 END)
              = SUM(CASE WHEN actual_deprecated THEN 1 ELSE 0 END)
           THEN 'PASS' ELSE 'PENDING' END
    FROM tag_automation_verification
    """
)
display(summary)

print(
    "Expected finished result: PII 3/3, missing documentation 2/2, and deprecated-by-name 1/1. "
    "A last-queried condition can add extra matches only when the schema contains genuinely old usage history."
)
